// ===================================================================================
// TOLEREX – gRPC LOGGING INTERCEPTOR
// ===================================================================================
//
// This file implements a gRPC unary server interceptor responsible for
// structured request lifecycle logging.
//
// From an observability and systems-monitoring perspective, this interceptor:
//
//   - Logs the start and completion of every gRPC request
//   - Measures and reports end-to-end request execution latency
//   - Correlates logs using context-propagated Request IDs
//   - Differentiates log output based on node role (Leader / Member)
//   - Provides consistent, centralized visibility into RPC execution
//
// This interceptor wraps the actual gRPC handler and therefore represents
// the most reliable point to observe request behavior without modifying
// application logic.
//
// Typical use cases:
//
//   - Latency analysis and performance tuning
//   - Error diagnosis and incident investigation
//   - Request tracing across distributed components
//   - Production debugging and auditing
//
// ===================================================================================

package middleware

import (
	"context"
	"time"

	"tolerex/internal/logger"

	"google.golang.org/grpc"
)

// ===================================================================================
// LOGGING INTERCEPTOR
// ===================================================================================
//
// LoggingInterceptor returns a gRPC UnaryServerInterceptor that logs the full
// lifecycle of each incoming gRPC request.
//
// Parameters:
//
//   baseLoggerName:
//     Identifies the node role ("leader" or "member") so that
//     log output is routed to the appropriate role-based logger.
//
// Behavior:
//
//   - Logs request start
//   - Delegates execution to the next interceptor or handler
//   - Logs request completion (success or failure)
//   - Includes execution duration in all completion logs
//
// The interceptor relies on RequestIDInterceptor (if present earlier in the
// chain) to enrich logs with request-scoped identifiers.

func LoggingInterceptor(baseLoggerName string) grpc.UnaryServerInterceptor {



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
		// Capture the start time before delegating to the handler.
		// This allows precise measurement of total request execution duration.

		start := time.Now()

		// -----------------------------------------------------------------------
		// BASE LOGGER SELECTION
		// -----------------------------------------------------------------------
		//
		// Select the appropriate role-based logger.
		// This ensures logs are clearly attributed to Leader or Member nodes.

		var base = logger.Leader
		if baseLoggerName == "member" {
			base = logger.Member
		}

		// -----------------------------------------------------------------------
		// CONTEXT-AWARE LOGGER
		// -----------------------------------------------------------------------
		//
		// Enrich the base logger with request-scoped metadata
		// such as Request ID (if present in the context).

		log := logger.WithContext(ctx, base)

		// -----------------------------------------------------------------------
		// REQUEST START LOG
		// -----------------------------------------------------------------------
		//
		// Log the beginning of request processing.
		// At this point, no assumptions are made about outcome.

		logger.Info(log, "gRPC started: %s", info.FullMethod)

		// -----------------------------------------------------------------------
		// DELEGATE TO NEXT HANDLER
		// -----------------------------------------------------------------------
		//
		// Control is passed to the next interceptor in the chain,
		// or to the actual gRPC method implementation.

		resp, err := handler(ctx, req)

		// -----------------------------------------------------------------------
		// DURATION CALCULATION
		// -----------------------------------------------------------------------
		//
		// Measure total request execution time.

		dur := time.Since(start)

		// -----------------------------------------------------------------------
		// REQUEST COMPLETION LOG
		// -----------------------------------------------------------------------
		//
		// Log the outcome of the request along with its duration.
		// Errors are explicitly logged to aid diagnosis.

		if err != nil {
			logger.Error(
				log,
				"gRPC failed: %s | duration=%v | err=%v",
				info.FullMethod,
				dur,
				err,
			)
		} else {
			logger.Info(
				log,
				"gRPC finished: %s | duration=%v",
				info.FullMethod,
				dur,
			)
		}

		return resp, err
	}
}
