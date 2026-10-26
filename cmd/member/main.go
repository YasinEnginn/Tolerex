package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"tolerex/internal/logger"
	"tolerex/internal/metrics"
	"tolerex/internal/middleware"
	"tolerex/internal/security"
	"tolerex/internal/server/member"
	"tolerex/internal/storage"
	proto "tolerex/proto/gen"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func main() {
	port := flag.String("port", getenv("MEMBER_GRPC_PORT", "5556"), "gRPC port")
	ioMode := flag.String("io", getenv("MEMBER_IO_MODE", storage.ModeBuffered), "disk IO mode: buffered | unbuffered")
	metricsPort := flag.String("metrics", getenv("MEMBER_METRICS_PORT", "9092"), "Prometheus metrics port")
	flag.Parse()

	logger.Init("member-" + *port)
	memberLog := logger.Member
	if !storage.IsValidMode(*ioMode) {
		logger.Fatal(memberLog, "Invalid disk I/O mode %q", *ioMode)
	}

	leaderAddr := getenv("LEADER_ADDR", "localhost:5555")
	memberAddr := getenv("MEMBER_ADDR", "localhost:"+*port)
	grpcAddr := net.JoinHostPort(getenv("MEMBER_GRPC_BIND", ""), *port)
	metricsAddr := net.JoinHostPort(getenv("MEMBER_METRICS_BIND", ""), *metricsPort)
	dataDir := filepath.Join("internal", "data", "member-"+*port)
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		logger.Fatal(memberLog, "Data directory could not be created: %v", err)
	}

	serverCreds, err := security.NewMTLSServerCreds(
		getenv("MEMBER_CERT_FILE", "config/tls/member.crt"),
		getenv("MEMBER_KEY_FILE", "config/tls/member.key"),
		getenv("CA_CERT_FILE", "config/tls/ca.crt"),
	)
	if err != nil {
		logger.Fatal(memberLog, "Failed to load member server credentials: %v", err)
	}

	listener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Fatal(memberLog, "Failed to listen on address %s: %v", grpcAddr, err)
	}
	grpcServer := grpc.NewServer(
		grpc.Creds(serverCreds),
		grpc.ChainUnaryInterceptor(
			middleware.RequestIDInterceptor(),
			middleware.LoggingInterceptor("member"),
			middleware.MetricsInterceptor(),
			middleware.RecoveryInterceptor("member"),
		),
	)
	proto.RegisterStorageServiceServer(grpcServer, &member.MemberServer{DataDir: dataDir, IOMode: *ioMode})

	metrics.Init()
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	metricsServer := &http.Server{
		Addr:              metricsAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	signalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	ctx, cancel := context.WithCancel(signalCtx)
	defer cancel()
	serverErr := make(chan error, 2)

	go func() {
		logger.Info(memberLog, "Member gRPC listening on %s (data=%s, io=%s)", grpcAddr, dataDir, *ioMode)
		if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			serverErr <- err
			cancel()
		}
	}()
	go func() {
		logger.Info(memberLog, "Metrics listening on %s", metricsAddr)
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
			cancel()
		}
	}()

	if err := registerToLeader(ctx, memberLog, leaderAddr, memberAddr); err != nil {
		if ctx.Err() == nil {
			logger.Error(memberLog, "Leader registration stopped: %v", err)
		}
	} else {
		heartbeatDone, err := startHeartbeat(ctx, memberLog, leaderAddr, memberAddr)
		if err != nil {
			logger.Error(memberLog, "Heartbeat startup failed: %v", err)
			cancel()
		} else {
			defer func() { <-heartbeatDone }()
		}
	}

	<-ctx.Done()
	select {
	case err := <-serverErr:
		logger.Error(memberLog, "Member server stopped: %v", err)
	default:
		logger.Info(memberLog, "Member shutting down")
	}

	grpcDone := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(grpcDone)
	}()
	select {
	case <-grpcDone:
	case <-time.After(5 * time.Second):
		grpcServer.Stop()
		<-grpcDone
	}

	httpCtx, httpCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = metricsServer.Shutdown(httpCtx)
	httpCancel()
}

func registerToLeader(ctx context.Context, memberLog *log.Logger, leaderAddr, memberAddr string) error {
	creds, err := security.NewMTLSClientCreds(
		getenv("MEMBER_CERT_FILE", "config/tls/member.crt"),
		getenv("MEMBER_KEY_FILE", "config/tls/member.key"),
		getenv("CA_CERT_FILE", "config/tls/ca.crt"),
		"leader",
	)
	if err != nil {
		return err
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		conn, err := grpc.NewClient(leaderAddr, grpc.WithTransportCredentials(creds))
		if err == nil {
			callCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
			reply, callErr := proto.NewStorageServiceClient(conn).RegisterMember(callCtx, &proto.MemberInfo{Address: memberAddr})
			cancel()
			_ = conn.Close()
			if callErr == nil && reply != nil && reply.Ok {
				logger.Info(memberLog, "Registered with leader as %s", memberAddr)
				return nil
			}
			err = callErr
		}
		logger.Warn(memberLog, "Leader registration failed; retrying in 2s: %v", err)
		if !sleepOrStop(ctx.Done(), 2*time.Second) {
			return ctx.Err()
		}
	}
}

type heartbeatSession struct {
	client proto.StorageServiceClient
	close  func() error
}

type heartbeatConnectFn func() (*heartbeatSession, error)

func sleepOrStop(stop <-chan struct{}, duration time.Duration) bool {
	if stop == nil {
		time.Sleep(duration)
		return true
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-stop:
		return false
	}
}

func buildHeartbeatConnectFn(leaderAddr string, creds credentials.TransportCredentials) heartbeatConnectFn {
	return func() (*heartbeatSession, error) {
		conn, err := grpc.NewClient(leaderAddr, grpc.WithTransportCredentials(creds))
		if err != nil {
			return nil, err
		}
		return &heartbeatSession{client: proto.NewStorageServiceClient(conn), close: conn.Close}, nil
	}
}

func runHeartbeatLoop(memberLog *log.Logger, leaderAddr, memberAddr string, interval time.Duration, connect heartbeatConnectFn, stop <-chan struct{}) {
	var session *heartbeatSession
	defer func() {
		if session != nil && session.close != nil {
			_ = session.close()
		}
	}()

	for {
		select {
		case <-stop:
			return
		default:
		}

		if session == nil {
			connected, err := connect()
			if err != nil {
				logger.Warn(memberLog, "Leader unreachable: %v", err)
				if !sleepOrStop(stop, interval) {
					return
				}
				continue
			}
			session = connected
			logger.Debug(memberLog, "Heartbeat channel established to %s", leaderAddr)
		}

		heartbeatCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := session.client.Heartbeat(heartbeatCtx, &proto.HeartbeatRequest{Address: memberAddr})
		cancel()
		if err != nil {
			if status.Code(err) == codes.NotFound {
				registerCtx, registerCancel := context.WithTimeout(context.Background(), 2*time.Second)
				reply, registerErr := session.client.RegisterMember(registerCtx, &proto.MemberInfo{Address: memberAddr})
				registerCancel()
				if registerErr == nil && reply != nil && reply.Ok {
					logger.Info(memberLog, "Member re-registered after leader state loss: %s", memberAddr)
					if !sleepOrStop(stop, interval) {
						return
					}
					continue
				}
			}
			logger.Warn(memberLog, "Heartbeat failed: %v", err)
			if session.close != nil {
				_ = session.close()
			}
			session = nil
		}

		if !sleepOrStop(stop, interval) {
			return
		}
	}
}

func startHeartbeat(ctx context.Context, memberLog *log.Logger, leaderAddr, memberAddr string) (<-chan struct{}, error) {
	creds, err := security.NewMTLSClientCreds(
		getenv("MEMBER_CERT_FILE", "config/tls/member.crt"),
		getenv("MEMBER_KEY_FILE", "config/tls/member.key"),
		getenv("CA_CERT_FILE", "config/tls/ca.crt"),
		"leader",
	)
	if err != nil {
		return nil, err
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		runHeartbeatLoop(memberLog, leaderAddr, memberAddr, 5*time.Second, buildHeartbeatConnectFn(leaderAddr, creds), ctx.Done())
	}()
	return done, nil
}
