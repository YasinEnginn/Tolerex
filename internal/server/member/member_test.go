package member

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"log"
	"testing"

	"tolerex/internal/logger"
	"tolerex/internal/storage"
	pb "tolerex/proto/gen"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestStoreAndRetrieveRequireLeaderIdentity(t *testing.T) {
	logger.Member = log.New(io.Discard, "", 0)
	server := &MemberServer{DataDir: t.TempDir(), IOMode: storage.ModeBuffered}

	unauthorized := tlsContext("member")
	if _, err := server.Store(unauthorized, &pb.StoredMessage{Id: 1, Text: "value"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}

	authorized := tlsContext("leader")
	result, err := server.Store(authorized, &pb.StoredMessage{Id: 1, Text: "value"})
	if err != nil || result == nil || !result.Ok {
		t.Fatalf("store failed: result=%+v err=%v", result, err)
	}
	message, err := server.Retrieve(authorized, &pb.MessageID{Id: 1})
	if err != nil || message.Text != "value" {
		t.Fatalf("retrieve failed: message=%+v err=%v", message, err)
	}
}

func TestMemberValidatesRequests(t *testing.T) {
	logger.Member = log.New(io.Discard, "", 0)
	server := &MemberServer{DataDir: t.TempDir(), IOMode: storage.ModeBuffered}
	ctx := tlsContext("leader")

	if _, err := server.Store(ctx, nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for nil store, got %v", err)
	}
	if _, err := server.Store(ctx, &pb.StoredMessage{Id: -1}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for negative id, got %v", err)
	}
	if _, err := server.Retrieve(ctx, &pb.MessageID{Id: 99}); status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

func tlsContext(commonName string) context.Context {
	cert := &x509.Certificate{Subject: pkix.Name{CommonName: commonName}}
	tlsInfo := credentials.TLSInfo{State: tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{cert},
		VerifiedChains:   [][]*x509.Certificate{{cert}},
	}}
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: tlsInfo})
}
