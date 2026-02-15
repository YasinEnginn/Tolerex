package main

import (
	"context"
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
)

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// ===================================================================================
// TOLEREX – MEMBER NODE BOOTSTRAP
// ===================================================================================
//
// This file is the **infrastructure entry point** for a Member node in the
// Tolerex distributed, fault-tolerant storage system.
//
// From an architectural perspective, a Member node:
//
//   - Acts as a stateful storage replica
//   - Accepts Store / Retrieve RPCs exclusively from the Leader
//   - Persists data locally to disk
//   - Does NOT perform coordination, replication strategy, or consensus
//
// This file intentionally contains **no business logic**.
// Its sole responsibility is wiring together runtime components:
//
//   - Logging initialization
//   - Command-line configuration
//   - Secure gRPC server bootstrap (mTLS)
//   - Leader registration
//   - Periodic heartbeat emission
//   - Graceful shutdown handling
//
// This file serves as the **composition root** of the Member process.
//
// ===================================================================================

func main() {

	// -------------------------------------------------------------------------------
	// LOGGING INITIALIZATION
	// -------------------------------------------------------------------------------
	//
	// Initializes the global logging subsystem for the Member process.
	// Logging includes structured output, rotation, and runtime-adjustable levels.
	// All logs are prefixed with the "Member" role identifier.
	// This must be done before any other operations to ensure
	// that all components have access to logging facilities.

	logger.Init()
	logger.SetLevel(logger.INFO)

	memberLog := logger.Member
	logger.Info(memberLog, "Member server starting")

	// -------------------------------------------------------------------------------
	// COMMAND-LINE FLAGS
	// -------------------------------------------------------------------------------
	//
	// Each Member instance is configured via CLI flags:
	//
	//   - port : gRPC listening port (unique per Member)
	//   - io   : disk I/O mode (buffered | unbuffered)
	// These flags allow flexible deployment and testing.
	// Defaults are provided for convenience.
	// The port flag is critical to ensure no conflicts between
	// multiple Member instances on the same host.
	// The I/O mode flag allows experimentation with different
	// disk persistence strategies.

	port := flag.String(
		"port",
		getenv("MEMBER_GRPC_PORT", "5556"),
		"gRPC port",
	)

	ioMode := flag.String(
		"io",
		getenv("MEMBER_IO_MODE", storage.ModeBuffered),
		"disk IO mode: buffered | unbuffered",
	)

	metricsPort := flag.String(
		"metrics",
		getenv("MEMBER_METRICS_PORT", "9092"),
		"Prometheus metrics port",
	)

	flag.Parse()

	leaderAddr := getenv("LEADER_ADDR", "localhost:5555")
	myAddr := os.Getenv("MEMBER_ADDR")
	if myAddr == "" {
		myAddr = "localhost:" + *port
	}

	// -------------------------------------------------------------------------------
	// PROMETHEUS METRICS INITIALIZATION
	// -------------------------------------------------------------------------------
	//
	// Registers all Prometheus metric collectors and exposes
	// an HTTP /metrics endpoint for scraping.
	//
	// This HTTP server is intentionally decoupled from gRPC
	// and runs on a dedicated port.

	metrics.Init()

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())

		addr := ":" + *metricsPort
		logger.Info(memberLog, "Metrics endpoint listening on %s", addr)

		if err := http.ListenAndServe(addr, mux); err != nil {
			logger.Warn(memberLog, "Metrics server stopped: %v", err)
		}
	}()

	// -------------------------------------------------------------------------------
	// DATA DIRECTORY INITIALIZATION
	// -------------------------------------------------------------------------------
	//
	// Each Member maintains an isolated data directory based on its port.
	// This ensures:
	//   - No cross-member disk interference
	//   - Deterministic storage layout
	//   - Easy debugging and cleanup
	// The directory is created if it does not exist.
	// Any failure to create the directory results in process termination.
	// This is a critical step to ensure that the Member can persist data reliably.
	// The directory follows the pattern:
	//   internal/data/member-<port>/
	// This convention simplifies management of multiple Member instances.

	dataDir := filepath.Join("internal", "data", "member-"+*port)
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		logger.Fatal(memberLog, "Data directory could not be created: %v", err)
	}

	// -------------------------------------------------------------------------------
	// GRACEFUL SHUTDOWN HANDLING
	// -------------------------------------------------------------------------------
	//
	// Listens for OS termination signals (SIGINT, SIGTERM).
	// On shutdown:
	//   - Logs the event
	//   - Allows controlled process termination
	//
	// Note:
	// This is intentionally minimal. Any cleanup policy (ephemeral storage, etc.)
	// is handled explicitly by design decisions elsewhere.
	// The focus here is on logging and orderly shutdown.

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sig
		logger.Warn(memberLog, "Shutdown signal received, terminating member process")
		os.Exit(0)
	}()

	// -------------------------------------------------------------------------------
	// START MEMBER gRPC SERVER
	// -------------------------------------------------------------------------------
	//
	// The gRPC server:
	//   - Accepts Store / Retrieve RPCs
	//   - Is secured with mutual TLS (mTLS)
	//   - Is instrumented with logging, recovery, request IDs, and metrics
	//
	// It is launched in a separate goroutine so that registration and heartbeat
	// logic can proceed concurrently.
	// This ensures the Member is operational as soon as possible.
	// Any fatal errors during server startup result in process termination.
	// This is critical to ensure availability and reliability.
	// The server binds to the specified port and uses the designated data directory
	// and I/O mode for disk operations.
	// This setup is essential for the Member's role as a storage worker in the Tolerex system.

	go startMemberGRPC(memberLog, *port, dataDir, *ioMode)

	// -------------------------------------------------------------------------------
	// LEADER REGISTRATION
	// -------------------------------------------------------------------------------
	//
	// After startup, the Member explicitly registers itself with the Leader.
	// A short delay ensures the gRPC server is fully listening before registration.
	// Registration involves:
	//   - Establishing a secure mTLS connection to the Leader
	//   - Sending its network address
	// Any failure to register results in process termination.
	// This is critical to ensure the Member is recognized and can participate
	// in the distributed storage system.
	// Successful registration is logged for auditing and debugging purposes.
	// This step is essential for the Member to become an active participant
	// in the Tolerex distributed architecture.
	// A brief sleep is used to allow the server to start before registration.

	time.Sleep(500 * time.Millisecond)
	registerToLeader(memberLog, leaderAddr, myAddr)

	// -------------------------------------------------------------------------------
	// HEARTBEAT LOOP
	// -------------------------------------------------------------------------------
	//
	// Starts a background heartbeat routine that:
	//   - Periodically contacts the Leader
	//   - Updates liveness state
	//   - Allows the Leader to detect failures via timeout

	startHeartbeat(memberLog, leaderAddr, myAddr)

	// -------------------------------------------------------------------------------
	// BLOCK FOREVER
	// -------------------------------------------------------------------------------
	//
	// The Member process is designed to run indefinitely until terminated.

	select {}
}

// ===================================================================================
// MEMBER gRPC SERVER BOOTSTRAP
// ===================================================================================

func startMemberGRPC(memberLog *log.Logger, port, dataDir, ioMode string) {

	// Bind TCP listener
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		logger.Fatal(memberLog, "Failed to listen on port %s: %v", port, err)
	}

	logger.Info(memberLog, "Starting gRPC server on port %s", port)

	// Load mTLS server credentials
	creds, err := security.NewMTLSServerCreds(
		"config/tls/member.crt",
		"config/tls/member.key",
		"config/tls/ca.crt",
	)
	if err != nil {
		logger.Fatal(memberLog, "Failed to load mTLS credentials: %v", err)
	}

	// Construct gRPC server with middleware chain
	grpcServer := grpc.NewServer(
		grpc.Creds(creds),
		grpc.ChainUnaryInterceptor(
			middleware.RequestIDInterceptor(),
			middleware.LoggingInterceptor("member"),
			middleware.MetricsInterceptor(),
			middleware.RecoveryInterceptor("member"),
		),
	)

	// Create MemberServer instance (storage worker)
	srv := &member.MemberServer{
		DataDir: dataDir,
		IOMode:  ioMode,
	}

	// Register gRPC service
	proto.RegisterStorageServiceServer(grpcServer, srv)

	logger.Info(
		memberLog,
		"Member running on port %s (dataDir=%s, io=%s)",
		port,
		dataDir,
		ioMode,
	)

	// Start serving requests
	if err := grpcServer.Serve(listener); err != nil {
		logger.Fatal(memberLog, "gRPC server terminated: %v", err)
	}
}

// ===================================================================================
// LEADER REGISTRATION ROUTINE
// ===================================================================================

func registerToLeader(memberLog *log.Logger, leaderAddr, myAddr string) {

	logger.Info(memberLog, "Registering to leader at %s as %s", leaderAddr, myAddr)

	// Load mTLS client credentials
	creds, err := security.NewMTLSClientCreds(
		"config/tls/member.crt",
		"config/tls/member.key",
		"config/tls/ca.crt",
		"leader",
	)
	if err != nil {
		logger.Fatal(memberLog, "mTLS client credentials error: %v", err)
	}

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		conn, err := grpc.DialContext(
			ctx,
			leaderAddr,
			grpc.WithTransportCredentials(creds),
			grpc.WithBlock(),
		)
		if err != nil {
			cancel()
			logger.Warn(memberLog, "Leader not ready yet, retrying registration: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		client := proto.NewStorageServiceClient(conn)
		_, err = client.RegisterMember(ctx, &proto.MemberInfo{Address: myAddr})
		conn.Close()
		cancel()
		if err != nil {
			logger.Warn(memberLog, "Leader registration failed, retrying: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		logger.Info(memberLog, "Successfully registered to leader: %s", myAddr)
		return
	}
}

// ===================================================================================
// HEARTBEAT LOOP
// ===================================================================================

func startHeartbeat(memberLog *log.Logger, leaderAddr, myAddr string) {

	logger.Info(memberLog, "Heartbeat loop started (interval=5s, leader=%s)", leaderAddr)

	// Prepare mTLS client credentials once
	creds, err := security.NewMTLSClientCreds(
		"config/tls/member.crt",
		"config/tls/member.key",
		"config/tls/ca.crt",
		"leader",
	)
	if err != nil {
		logger.Fatal(memberLog, "mTLS client credentials error: %v", err)
	}

	go func() {
		for {
			dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
			conn, err := grpc.DialContext(
				dialCtx,
				leaderAddr,
				grpc.WithTransportCredentials(creds),
				grpc.WithBlock(),
			)
			dialCancel()

			if err != nil {
				logger.Warn(memberLog, "Leader unreachable: %v", err)
				time.Sleep(5 * time.Second)
				continue
			}

			client := proto.NewStorageServiceClient(conn)

			hbCtx, hbCancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err = client.Heartbeat(hbCtx, &proto.HeartbeatRequest{Address: myAddr})
			hbCancel()
			conn.Close()

			if err != nil {
				logger.Warn(memberLog, "Heartbeat failed: %v", err)
			}

			time.Sleep(5 * time.Second)
		}
	}()
}

/*
USAGE EXAMPLE:

  go run cmd/member/main.go -port=5556 -io=buffered

*/
