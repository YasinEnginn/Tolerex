package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	registerOnce sync.Once

	GrpcRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "grpc_requests_total",
			Help: "Total number of gRPC requests processed",
		},
		[]string{"method", "status"},
	)

	GrpcRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "grpc_request_duration_seconds",
			Help:    "gRPC request latency in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method"},
	)

	MemberStoredMessagesTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "member_stored_messages_total",
			Help: "Messages stored successfully by this member",
		},
	)
)

func Init() {
	registerOnce.Do(func() {
		prometheus.MustRegister(GrpcRequestsTotal, GrpcRequestDuration, MemberStoredMessagesTotal)
	})
}
