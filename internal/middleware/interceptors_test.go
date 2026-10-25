package middleware

import (
	"context"
	"errors"
	"io"
	"log"
	"sync"
	"testing"

	"tolerex/internal/logger"
	"tolerex/internal/metrics"

	dto "github.com/prometheus/client_model/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRequestIDInterceptorConcurrentUniqueness(t *testing.T) {
	interceptor := RequestIDInterceptor()
	const calls = 256
	ids := make(chan string, calls)
	var wg sync.WaitGroup
	for range calls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
				id, _ := ctx.Value(logger.RequestIDKey).(string)
				ids <- id
				return nil, nil
			})
			if err != nil {
				t.Errorf("interceptor failed: %v", err)
			}
		}()
	}
	wg.Wait()
	close(ids)
	seen := make(map[string]struct{}, calls)
	for id := range ids {
		if id == "" {
			t.Fatal("empty request ID")
		}
		if _, duplicate := seen[id]; duplicate {
			t.Fatalf("duplicate request ID %q", id)
		}
		seen[id] = struct{}{}
	}
}

func TestLoggingInterceptorRoleAndStatusMatrix(t *testing.T) {
	logger.Leader = log.New(io.Discard, "", 0)
	logger.Member = log.New(io.Discard, "", 0)
	oldLevel := logger.INFO
	logger.SetLevel(logger.DEBUG)
	defer logger.SetLevel(oldLevel)

	for _, role := range []string{"leader", "member", "unknown"} {
		for _, code := range []codes.Code{codes.OK, codes.NotFound, codes.InvalidArgument, codes.Internal, codes.Unavailable, codes.DataLoss, codes.Unknown} {
			wantResponse := &struct{ value string }{value: role}
			var wantErr error
			if code != codes.OK {
				wantErr = status.Error(code, "matrix")
			}
			gotResponse, gotErr := LoggingInterceptor(role)(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/matrix/test"}, func(context.Context, any) (any, error) {
				return wantResponse, wantErr
			})
			if gotResponse != wantResponse || status.Code(gotErr) != code {
				t.Fatalf("role=%s code=%s response=%v err=%v", role, code, gotResponse, gotErr)
			}
		}
	}
}

func TestMetricsInterceptorResultMatrix(t *testing.T) {
	interceptor := MetricsInterceptor()
	for index, handlerErr := range []error{nil, status.Error(codes.NotFound, "missing"), errors.New("failure")} {
		method := "/matrix/metrics/" + string(rune('a'+index))
		result := "success"
		if handlerErr != nil {
			result = "error"
		}
		counter := metrics.GrpcRequestsTotal.WithLabelValues(method, result)
		before := counterValue(t, counter)
		response, err := interceptor(context.Background(), "request", &grpc.UnaryServerInfo{FullMethod: method}, func(context.Context, any) (any, error) {
			return "response", handlerErr
		})
		if response != "response" || !errors.Is(err, handlerErr) {
			t.Fatalf("case=%d response=%v err=%v", index, response, err)
		}
		if after := counterValue(t, counter); after-before != 1 {
			t.Fatalf("case=%d counter delta=%v", index, after-before)
		}
	}
}

func counterValue(t *testing.T, counter interface{ Write(*dto.Metric) error }) float64 {
	t.Helper()
	metric := &dto.Metric{}
	if err := counter.Write(metric); err != nil {
		t.Fatal(err)
	}
	return metric.GetCounter().GetValue()
}

func TestRecoveryInterceptorRoleMatrix(t *testing.T) {
	logger.Leader = log.New(io.Discard, "", 0)
	logger.Member = log.New(io.Discard, "", 0)
	for _, role := range []string{"leader", "member", "unknown"} {
		for _, panicValue := range []any{"panic", errors.New("panic"), 42} {
			response, err := RecoveryInterceptor(role)(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/matrix/panic"}, func(context.Context, any) (any, error) {
				panic(panicValue)
			})
			if response != nil || status.Code(err) != codes.Internal {
				t.Fatalf("role=%s panic=%v response=%v err=%v", role, panicValue, response, err)
			}
		}
	}
}
