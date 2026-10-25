package middleware

import (
	"context"
	"strconv"
	"sync/atomic"

	"tolerex/internal/logger"

	"google.golang.org/grpc"
)

var requestSequence atomic.Uint64

// RequestIDInterceptor adds a cheap process-local request identifier to logs.
func RequestIDInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		requestID := strconv.FormatUint(requestSequence.Add(1), 36)
		return handler(context.WithValue(ctx, logger.RequestIDKey, requestID), req)
	}
}
