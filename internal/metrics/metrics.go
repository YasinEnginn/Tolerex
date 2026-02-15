// ===================================================================================
// TOLEREX – PROMETHEUS METRICS SUBSYSTEM
// ===================================================================================
//
// This package defines the core Prometheus metric collectors used to observe
// gRPC behavior inside the Tolerex distributed system.
//
// From an observability and systems-monitoring perspective, this subsystem:
//
//   - Exposes high-level gRPC traffic metrics
//   - Provides request-level visibility (volume + latency)
//   - Integrates naturally with Prometheus’ pull-based scraping model
//   - Is designed to be consumed by gRPC middleware / interceptors
//
// Key design constraints:
//
//   - Metrics are process-wide and singleton in nature
//   - Collectors must be registered exactly once at startup
//   - No business logic is embedded in this package
//
// This package DOES NOT expose an HTTP endpoint.
// It only defines and registers metric collectors.
// Endpoint exposure (e.g. /metrics) is handled by the application bootstrap.
//
// ===================================================================================

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// ===================================================================================
// METRIC COLLECTORS
// ===================================================================================

var (

	// -------------------------------------------------------------------------------
	// TOTAL gRPC REQUEST COUNTER
	// -------------------------------------------------------------------------------
	//
	// GrpcRequestsTotal counts the total number of gRPC requests processed
	// by the server process.
	//
	// This metric provides a high-level view of traffic volume and error rates.
	//
	// Labels:
	//   - method : Fully-qualified gRPC method name
	//              (e.g. StorageService/Store)
	//   - status : Logical request outcome
	//              (e.g. success, error)
	//
	// Typical use cases:
	//   - Traffic volume monitoring
	//   - Error-rate alerting
	//   - Capacity planning

	GrpcRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "grpc_requests_total",
			Help: "Total number of gRPC requests processed",
		},
		[]string{"method", "status"},
	)

	// -------------------------------------------------------------------------------
	// gRPC REQUEST LATENCY HISTOGRAM
	// -------------------------------------------------------------------------------
	//
	// GrpcRequestDuration measures the end-to-end latency of gRPC requests.
	//
	// Buckets:
	//   Uses Prometheus default buckets, which are well-suited for
	//   general-purpose RPC latency analysis.
	//
	// Labels:
	//   - method : Fully-qualified gRPC method name
	//
	// Typical use cases:
	//   - Latency SLO/SLA tracking
	//   - Tail latency analysis (p95 / p99)
	//   - Performance regression detection

	GrpcRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "grpc_request_duration_seconds",
			Help:    "Latency of gRPC requests in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method"},
	)
)

// ===================================================================================
// METRICS REGISTRATION
// ===================================================================================
//
// Init registers all metric collectors with Prometheus’ default registry.
//
// IMPORTANT:
//   - This function must be called exactly once per process.
//   - Multiple registrations will cause a panic (MustRegister).
//
// Registration is intentionally explicit to ensure
// predictable startup behavior and fail-fast semantics.

func Init() {
	prometheus.MustRegister(
		GrpcRequestsTotal,
		GrpcRequestDuration,
	)
}
