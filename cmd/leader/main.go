// ===================================================================================
// TOLEREX – LEADER NODE BOOTSTRAP
// ===================================================================================
//
// This file is the **infrastructure bootstrap** of the Leader process in the
// Tolerex distributed, fault-tolerant storage system.
//
// The Leader node is the *single coordination authority* in the cluster.
// This entry point is intentionally free of business logic and focuses purely on
// wiring together runtime components.
//
// Responsibilities of this file:
//
//   - Initialize global logging (rotation, levels, formatting)
//   - Load system-level configuration (replication tolerance)
//   - Construct the LeaderServer (cluster coordination core)
//   - Start a secure gRPC server protected by mutual TLS (mTLS)
//   - Expose a localhost-only TCP control interface for operators
//   - Publish Prometheus-compatible metrics for observability
//   - Launch background goroutines for heartbeat supervision
//   - Periodically render live cluster state for operational insight
//
// This file acts as the **composition root** of the Leader process.
// All domain behavior is delegated to internal packages:
//
//   - server     → cluster coordination & state
//   - security   → TLS / mTLS primitives
//   - middleware → cross-cutting gRPC concerns
//   - logger     → structured logging & lifecycle tracing
//
// ===================================================================================

package main

import (
	"net"
	"net/http"
	"os"
	"path/filepath"
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

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// ===================================================================================
// NETWORK CONFIGURATION
// ===================================================================================
//
// Ports are intentionally fixed for local development clarity.
// In production, these would typically be configurable via environment variables
// or a configuration system.

// ===================================================================================
// MAIN
// ===================================================================================

func main() {

	// -------------------------------------------------------------------------------
	// LOGGING INITIALIZATION
	// -------------------------------------------------------------------------------
	//
	// Initializes the global logging subsystem:
	//   - Log rotation via lumberjack
	//   - Structured logging format
	//   - Runtime-adjustable log levels
	//
	// The Leader logger is used throughout this process lifecycle.

	logger.Init()
	logger.SetLevel(logger.INFO)

	log := logger.Leader
	logger.Info(log, "Leader process starting")

	grpcPort := getenv("LEADER_GRPC_PORT", "5555")
	tcpPort := getenv("LEADER_TCP_PORT", "6666")
	tcpBind := getenv("LEADER_TCP_BIND", "127.0.0.1")
	metricsPort := getenv("LEADER_METRICS_PORT", "9090")

	grpcAddr := ":" + grpcPort
	tcpAddr := net.JoinHostPort(tcpBind, tcpPort)
	metricsAddr := ":" + metricsPort

	// -------------------------------------------------------------------------------
	// CONFIGURATION LOADING
	// -------------------------------------------------------------------------------
	//
	// tolerance.conf defines the replication tolerance factor (N-fault tolerance).
	// The initial member list is intentionally empty; members self-register dynamically.

	confPath := getenv("TOLERANCE_CONF", filepath.Join("config", "tolerance.conf"))
	initialMembers := []string{}

	// -------------------------------------------------------------------------------
	// LEADER SERVER CONSTRUCTION
	// -------------------------------------------------------------------------------
	//
	// The LeaderServer is the core coordination component responsible for:
	//   - Member registration & liveness tracking
	//   - Replica placement decisions
	//   - Store / Retrieve orchestration
	//   - Cluster metadata persistence

	leaderSrv, err := leader.NewLeaderServer(initialMembers, confPath)
	if err != nil {
		logger.Fatal(log, "Leader initialization failed: %v", err)
	}

	// -------------------------------------------------------------------------------
	// HEARTBEAT SUPERVISION
	// -------------------------------------------------------------------------------
	//
	// Starts a background watcher that:
	//   - Monitors periodic heartbeats from members
	//   - Detects failures via timeout
	//   - Marks unreachable members as DOWN
	//
	// This runs independently of the gRPC server.

	leaderSrv.StartHeartbeatWatcher()

	// -------------------------------------------------------------------------------
	// gRPC SERVER (DATA PLANE – mTLS PROTECTED)
	// -------------------------------------------------------------------------------
	//
	// This server handles all cluster-level RPCs:
	//   - RegisterMember
	//   - Heartbeat
	//   - Store / Retrieve
	//
	// It is secured using mutual TLS (mTLS) and instrumented with
	// logging, recovery, request IDs, and metrics.

	go func() {
		lis, err := net.Listen("tcp", grpcAddr)
		if err != nil {
			logger.Fatal(log, "Failed to listen on gRPC port %s: %v", grpcPort, err)
		}

		// Load mTLS server credentials:
		//   - Leader certificate
		//   - Leader private key
		//   - Trusted CA certificate

		creds, err := security.NewMTLSServerCreds(
			"config/tls/leader.crt",
			"config/tls/leader.key",
			"config/tls/ca.crt",
		)
		if err != nil {
			logger.Fatal(log, "Failed to load mTLS credentials: %v", err)
		}

		// Construct gRPC server with cross-cutting middleware.
		grpcServer := grpc.NewServer(
			grpc.Creds(creds),
			grpc.ChainUnaryInterceptor(
				middleware.RequestIDInterceptor(),
				middleware.LoggingInterceptor("leader"),
				middleware.MetricsInterceptor(),
				middleware.RecoveryInterceptor("leader"),
			),
		)

		// Bind LeaderServer implementation to the gRPC service definition.
		proto.RegisterStorageServiceServer(grpcServer, leaderSrv)

		logger.Info(log, "Leader gRPC server listening on %s", grpcPort)

		if err := grpcServer.Serve(lis); err != nil {
			logger.Fatal(log, "gRPC server terminated: %v", err)
		}
	}()

	// -------------------------------------------------------------------------------
	// TCP CONTROL PLANE (OPERATOR INTERFACE)
	// -------------------------------------------------------------------------------
	//
	// This is a localhost-only TCP interface intended for:
	//   - Manual inspection
	//   - Debugging
	//   - Interactive SET / GET commands
	//
	// It is intentionally isolated from external networks.

	go func() {
		listener, err := net.Listen("tcp", tcpAddr)
		if err != nil {
			logger.Fatal(log, "Failed to listen on TCP port %s: %v", tcpPort, err)
		}
		defer listener.Close()

		logger.Info(log, "Leader TCP control plane listening on %s", tcpAddr)

		for {
			conn, err := listener.Accept()
			if err != nil {
				logger.Warn(log, "TCP accept error: %v", err)
				continue
			}

			go leaderSrv.HandleClient(conn)
		}
	}()

	// -------------------------------------------------------------------------------
	// PROMETHEUS METRICS ENDPOINT
	// -------------------------------------------------------------------------------
	//
	// Exposes application and runtime metrics at:
	//   http://localhost:9090/metrics
	//
	// This endpoint is intentionally lightweight and read-only.

	metrics.Init()
	go func() {
		logger.Info(log, "Metrics endpoint listening on %s", metricsPort)

		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())

		if err := http.ListenAndServe(metricsAddr, mux); err != nil {
			logger.Warn(log, "Metrics server stopped: %v", err)
		}
	}()

	// -------------------------------------------------------------------------------
	// LIVE CLUSTER STATE RENDERING
	// -------------------------------------------------------------------------------
	//
	// Periodically prints a live view of cluster state to stdout:
	//   - Member addresses
	//   - Liveness status
	//   - Last heartbeat timestamp
	//   - Stored message counts
	//
	// This is intended for local observability and demos.

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		leaderSrv.PrintMemberStats()
	}
}
