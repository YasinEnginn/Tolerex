package middleware

import (
	"context"
	"time"

	"tolerex/internal/logger"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// LoggingInterceptor logs failures at error level and successful RPCs only at
// debug level, keeping the normal data path quiet.
func LoggingInterceptor(role string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		response, err := handler(ctx, req)

		base := logger.Leader
		if role == "member" {
			base = logger.Member
		}
		if err != nil {
			code := status.Code(err)
			if code == codes.Internal || code == codes.Unavailable || code == codes.DataLoss || code == codes.Unknown {
				logger.Error(logger.WithContext(ctx, base), "gRPC failed: %s duration=%v err=%v", info.FullMethod, time.Since(start), err)
			} else if logger.Enabled(logger.DEBUG) {
				logger.Debug(logger.WithContext(ctx, base), "gRPC completed: %s duration=%v code=%s", info.FullMethod, time.Since(start), code)
			}
		} else if logger.Enabled(logger.DEBUG) {
			logger.Debug(logger.WithContext(ctx, base), "gRPC finished: %s duration=%v", info.FullMethod, time.Since(start))
		}
		return response, err
	}
}
