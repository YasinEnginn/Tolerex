package main

import (
	"context"
	"errors"
	"fmt"
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
	"tolerex/internal/server/leader"
	proto "tolerex/proto/gen"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
)

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func main() {
	logger.Init("leader")
	log := logger.Leader

	grpcPort := getenv("LEADER_GRPC_PORT", "5555")
	tcpAddr := net.JoinHostPort(getenv("LEADER_TCP_BIND", "127.0.0.1"), getenv("LEADER_TCP_PORT", "6666"))
	metricsAddr := ":" + getenv("LEADER_METRICS_PORT", "9090")

	memberCreds, err := security.NewMTLSClientCreds(
		getenv("LEADER_CERT_FILE", "config/tls/leader.crt"),
		getenv("LEADER_KEY_FILE", "config/tls/leader.key"),
		getenv("CA_CERT_FILE", "config/tls/ca.crt"),
		"member",
	)
	if err != nil {
		logger.Fatal(log, "Failed to load member client credentials: %v", err)
	}

	leaderSrv, err := leader.NewLeaderServer(
		nil,
		getenv("TOLERANCE_CONF", filepath.Join("config", "tolerance.conf")),
		leader.WithMemberDialOption(grpc.WithTransportCredentials(memberCreds)),
	)
	if err != nil {
		logger.Fatal(log, "Leader initialization failed: %v", err)
	}

	serverCreds, err := security.NewMTLSServerCreds(
		getenv("LEADER_CERT_FILE", "config/tls/leader.crt"),
		getenv("LEADER_KEY_FILE", "config/tls/leader.key"),
		getenv("CA_CERT_FILE", "config/tls/ca.crt"),
	)
	if err != nil {
		logger.Fatal(log, "Failed to load leader server credentials: %v", err)
	}

	grpcListener, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		logger.Fatal(log, "Failed to listen on gRPC port %s: %v", grpcPort, err)
	}
	tcpListener, err := net.Listen("tcp", tcpAddr)
	if err != nil {
		_ = grpcListener.Close()
		logger.Fatal(log, "Failed to listen on TCP address %s: %v", tcpAddr, err)
	}

	grpcServer := grpc.NewServer(
		grpc.Creds(serverCreds),
		grpc.ChainUnaryInterceptor(
			middleware.RequestIDInterceptor(),
			middleware.LoggingInterceptor("leader"),
			middleware.MetricsInterceptor(),
			middleware.RecoveryInterceptor("leader"),
		),
	)
	proto.RegisterStorageServiceServer(grpcServer, leaderSrv)

	metrics.Init()
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	metricsServer := &http.Server{
		Addr:              metricsAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	leaderSrv.GrpcListener = grpcListener
	leaderSrv.TcpListener = tcpListener
	leaderSrv.StartStatePersister(0)
	leaderSrv.StartHeartbeatWatcher()

	serverErr := make(chan error, 3)
	go func() {
		logger.Info(log, "Leader gRPC listening on :%s", grpcPort)
		if err := grpcServer.Serve(grpcListener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			serverErr <- fmt.Errorf("gRPC server stopped: %w", err)
		}
	}()
	go func() {
		logger.Info(log, "Leader TCP listening on %s", tcpAddr)
		for {
			conn, err := tcpListener.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				serverErr <- fmt.Errorf("TCP accept failed: %w", err)
				return
			}
			leaderSrv.ServeClient(conn)
		}
	}()
	go func() {
		logger.Info(log, "Metrics listening on %s", metricsAddr)
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- fmt.Errorf("metrics server stopped: %w", err)
		}
	}()

	var statsTicker *time.Ticker
	var statsTick <-chan time.Time
	if raw := os.Getenv("LEADER_STATS_INTERVAL"); raw != "" && raw != "0" {
		interval, err := time.ParseDuration(raw)
		if err != nil || interval <= 0 {
			logger.Fatal(log, "Invalid LEADER_STATS_INTERVAL %q; use 0 or a duration such as 5s", raw)
		}
		statsTicker = time.NewTicker(interval)
		statsTick = statsTicker.C
		defer statsTicker.Stop()
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(shutdown)

	running := true
	for running {
		select {
		case <-statsTick:
			leaderSrv.PrintMemberStats()
		case sig := <-shutdown:
			logger.Info(log, "Shutdown signal received: %s", sig)
			running = false
		case err := <-serverErr:
			logger.Error(log, "%v", err)
			running = false
		}
	}

	_ = tcpListener.Close()
	leaderSrv.CloseClients()
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

	httpCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = metricsServer.Shutdown(httpCtx)
	cancel()

	leaderSrv.StopHeartbeatWatcher()
	leaderSrv.CloseAllConns()
	if err := leaderSrv.StopStatePersister(); err != nil {
		logger.Error(log, "Final leader state flush failed: %v", err)
	}
}
