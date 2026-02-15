// ===================================================================================
// TOLEREX – gRPC PANIC RECOVERY INTERCEPTOR
// ===================================================================================
//
// This file implements a gRPC unary server interceptor responsible for
// isolating and recovering from panics occurring during request handling.
//
// From a systems reliability and fault-tolerance perspective, this interceptor:
//
//   - Prevents process-wide crashes caused by unexpected panics
//   - Contains failures strictly at the request boundary
//   - Logs detailed panic diagnostics, including stack traces
//   - Translates unrecoverable panics into standardized gRPC errors
//   - Preserves overall service availability under runtime failures
//
// This interceptor represents the **last-resort safety net** in the gRPC
// interceptor chain and should be placed as the innermost interceptor
// (i.e., last in the chain), so upstream logging/metrics interceptors
// can observe the recovered error result.
//
// ===================================================================================

package middleware

import (
	"context"
	"runtime/debug"

	"tolerex/internal/logger"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ===================================================================================
// PANIC RECOVERY INTERCEPTOR
// ===================================================================================
//
// RecoveryInterceptor returns a gRPC UnaryServerInterceptor that:
//
//   - Wraps request execution in a deferred recover() block
//   - Captures panics raised by downstream handlers or interceptors
//   - Logs panic details with full stack trace
//   - Converts the panic into a gRPC INTERNAL error
//
// The interceptor does NOT attempt to resume execution.
// Instead, it fails the request safely while keeping the process alive.
//
// Parameter:
//
//   baseLoggerName:
//     Identifies the node role ("leader" or "member") so that panic logs
//     are attributed to the correct component.

func RecoveryInterceptor(baseLoggerName string) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp interface{}, err error) {

		// -----------------------------------------------------------------------
		// PANIC RECOVERY DEFER
		// -----------------------------------------------------------------------
		//
		// The deferred function ensures that any panic occurring during
		// request processing is recovered and handled gracefully.
		//
		// Without this interceptor, a single panic could terminate the
		// entire gRPC server process.

		defer func() {
			if r := recover(); r != nil {

				// ---------------------------------------------------------------
				// BASE LOGGER SELECTION
				// ---------------------------------------------------------------
				//
				// Select the appropriate role-based logger so that
				// panic diagnostics are clearly attributed.

				var base = logger.Leader
				if baseLoggerName == "member" {
					base = logger.Member
				}

				// ---------------------------------------------------------------
				// CONTEXT-AWARE LOGGER
				// ---------------------------------------------------------------
				//
				// Enrich panic logs with request-scoped metadata
				// such as Request ID (if present in context).

				log := logger.WithContext(ctx, base)

				// ---------------------------------------------------------------
				// PANIC DIAGNOSTIC LOGGING
				// ---------------------------------------------------------------
				//
				// Log:
				//   - Panic value
				//   - gRPC method name
				//   - Full stack trace
				//
				// Stack traces are critical for post-mortem analysis
				// and root-cause investigation.

				logger.Error(
					log,
					"PANIC recovered: %v | method=%s\n%s",
					r,
					info.FullMethod,
					string(debug.Stack()),
				)

				// ---------------------------------------------------------------
				// ERROR TRANSLATION
				// ---------------------------------------------------------------
				//
				// Convert the panic into a standardized gRPC INTERNAL error.
				// This prevents leaking internal implementation details
				// to the client while clearly signaling a server-side failure.

				err = status.Errorf(codes.Internal, "internal server error")
			}
		}()

		// -----------------------------------------------------------------------
		// DELEGATION
		// -----------------------------------------------------------------------
		//
		// Execute the next interceptor in the chain or the actual
		// gRPC method implementation.

		return handler(ctx, req)
	}
}
