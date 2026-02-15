// ===================================================================================
// TOLEREX – gRPC METRICS INTERCEPTOR
// ===================================================================================
//
// This file implements a gRPC unary server interceptor responsible for
// collecting Prometheus-compatible metrics for RPC traffic.
//
// From an observability and telemetry perspective, this interceptor:
//
//   - Counts total gRPC requests by method and outcome
//   - Measures end-to-end request latency
//   - Produces metrics suitable for SLI / SLO analysis
//   - Integrates transparently via gRPC middleware
//
// The interceptor wraps the actual gRPC handler and records metrics
// *after* request completion to ensure accurate latency measurement.
//
// Metrics collected:
//
//   - grpc_requests_total (Counter)
//   - grpc_request_duration_seconds (Histogram)
//
// This interceptor is intentionally passive:
// it does not affect request flow, error handling, or control logic.
//
// ===================================================================================

package middleware

import (
	"context"
	"time"

	"tolerex/internal/metrics"

	"google.golang.org/grpc"
)

// ===================================================================================
// METRICS INTERCEPTOR
// ===================================================================================
//
// MetricsInterceptor returns a gRPC UnaryServerInterceptor that records
// request volume and latency metrics for every incoming RPC.
//
// Behavior:
//
//   - Captures request start time
//   - Delegates execution to the next interceptor or handler
//   - Classifies request outcome (success / error)
//   - Updates Prometheus counters and histograms
//
// This interceptor should wrap recovery logic in the chain (be declared
// before RecoveryInterceptor) so panics converted into gRPC errors are
// still counted as failed requests.

func MetricsInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {

		// -----------------------------------------------------------------------
		// REQUEST START TIMESTAMP
		// -----------------------------------------------------------------------
		//
		// Capture the start time before invoking the handler to measure
		// total end-to-end request processing latency.

		start := time.Now()

		// -----------------------------------------------------------------------
		// DELEGATE TO NEXT HANDLER
		// -----------------------------------------------------------------------
		//
		// Execute the actual gRPC method implementation or the next
		// interceptor in the chain.

		resp, err := handler(ctx, req)

		// -----------------------------------------------------------------------
		// DURATION CALCULATION
		// -----------------------------------------------------------------------
		//
		// Compute request duration in seconds, which is the canonical
		// unit for Prometheus latency metrics.

		duration := time.Since(start).Seconds()

		// -----------------------------------------------------------------------
		// OUTCOME CLASSIFICATION
		// -----------------------------------------------------------------------
		//
		// Requests are classified into a simple success / error model.
		// This keeps label cardinality low while remaining useful for
		// high-level error-rate analysis.

		status := "success"
		if err != nil {
			status = "error"
		}

		// -----------------------------------------------------------------------
		// REQUEST COUNTER UPDATE
		// -----------------------------------------------------------------------
		//
		// Increment the total request counter labeled by:
		//   - gRPC method name
		//   - request outcome

		metrics.GrpcRequestsTotal.
			WithLabelValues(info.FullMethod, status).
			Inc()

		// -----------------------------------------------------------------------
		// LATENCY HISTOGRAM UPDATE
		// -----------------------------------------------------------------------
		//
		// Observe the request latency for the given gRPC method.
		// This enables percentile-based latency analysis (p50, p95, p99).

		metrics.GrpcRequestDuration.
			WithLabelValues(info.FullMethod).
			Observe(duration)

		return resp, err
	}
}
