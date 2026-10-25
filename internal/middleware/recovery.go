package middleware

import (
	"context"
	"runtime/debug"

	"tolerex/internal/logger"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func RecoveryInterceptor(role string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response any, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				base := logger.Leader
				if role == "member" {
					base = logger.Member
				}
				logger.Error(logger.WithContext(ctx, base), "panic recovered: %v method=%s\n%s", recovered, info.FullMethod, debug.Stack())
				err = status.Error(codes.Internal, "internal server error")
			}
		}()
		return handler(ctx, req)
	}
}
