package member

import (
	"context"
	"errors"
	"os"
	"time"

	"tolerex/internal/logger"
	"tolerex/internal/metrics"
	"tolerex/internal/storage"
	pb "tolerex/proto/gen"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	defaultExpectedLeaderCN = "leader"
	maxMessageBytes         = 4 << 20
)

func expectedLeaderCN() string {
	if commonName := os.Getenv("MEMBER_EXPECTED_LEADER_CN"); commonName != "" {
		return commonName
	}
	return defaultExpectedLeaderCN
}

func authorizeLeader(ctx context.Context) error {
	peerInfo, ok := peer.FromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing peer info")
	}
	tlsInfo, ok := peerInfo.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.VerifiedChains) == 0 || len(tlsInfo.State.VerifiedChains[0]) == 0 {
		return status.Error(codes.Unauthenticated, "unverified peer certificate")
	}
	if commonName := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName; commonName != expectedLeaderCN() {
		logger.Warn(logger.Member, "Unauthorized caller CN=%s", commonName)
		return status.Error(codes.PermissionDenied, "unauthorized caller")
	}
	return nil
}

func contextError(ctx context.Context) error {
	switch ctx.Err() {
	case nil:
		return nil
	case context.DeadlineExceeded:
		return status.Error(codes.DeadlineExceeded, "request deadline exceeded")
	default:
		return status.Error(codes.Canceled, "request cancelled")
	}
}

type MemberServer struct {
	pb.UnimplementedStorageServiceServer
	DataDir string
	IOMode  string
}

func (s *MemberServer) Store(ctx context.Context, message *pb.StoredMessage) (*pb.StoreResult, error) {
	if err := authorizeLeader(ctx); err != nil {
		return nil, err
	}
	if message == nil {
		return nil, status.Error(codes.InvalidArgument, "message is required")
	}
	if message.Id < 0 {
		return nil, status.Error(codes.InvalidArgument, "message id must be non-negative")
	}
	if len(message.Text) > maxMessageBytes {
		return nil, status.Error(codes.ResourceExhausted, "message exceeds 4 MiB limit")
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}

	start := time.Now()
	if err := storage.WriteMessage(s.DataDir, int(message.Id), message.Text, s.IOMode); err != nil {
		logger.Error(logger.WithContext(ctx, logger.Member), "Store failed: msg_id=%d err=%v", message.Id, err)
		return nil, status.Error(codes.Internal, "message could not be stored")
	}
	logger.Debug(logger.WithContext(ctx, logger.Member), "Store completed: msg_id=%d duration=%s", message.Id, time.Since(start))
	metrics.MemberStoredMessagesTotal.Inc()
	return &pb.StoreResult{Ok: true}, nil
}

func (s *MemberServer) Retrieve(ctx context.Context, request *pb.MessageID) (*pb.StoredMessage, error) {
	if err := authorizeLeader(ctx); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "message id is required")
	}
	if request.Id < 0 {
		return nil, status.Error(codes.InvalidArgument, "message id must be non-negative")
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}

	text, err := storage.ReadMessage(s.DataDir, int(request.Id))
	if errors.Is(err, storage.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "message not found")
	}
	if err != nil {
		logger.Error(logger.WithContext(ctx, logger.Member), "Retrieve failed: msg_id=%d err=%v", request.Id, err)
		return nil, status.Error(codes.Internal, "message could not be read")
	}
	return &pb.StoredMessage{Id: request.Id, Text: text}, nil
}
