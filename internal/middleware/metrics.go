package middleware

import (
	"context"
	"time"

	"tolerex/internal/metrics"

	"google.golang.org/grpc"
)

func MetricsInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		response, err := handler(ctx, req)
		result := "success"
		if err != nil {
			result = "error"
		}
		metrics.GrpcRequestsTotal.WithLabelValues(info.FullMethod, result).Inc()
		metrics.GrpcRequestDuration.WithLabelValues(info.FullMethod).Observe(time.Since(start).Seconds())
		return response, err
	}
}
