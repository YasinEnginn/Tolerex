package member

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"tolerex/internal/storage"
	pb "tolerex/proto/gen"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMemberModeIDPayloadBoundaryMatrix(t *testing.T) {
	payloads := []struct {
		name string
		text string
	}{
		{name: "empty", text: ""},
		{name: "ascii", text: "value"},
		{name: "unicode", text: "Türkçe 世界 🚀 tolerex> ERROR:"},
		{name: "binary-like", text: "zero\x00byte\nline"},
		{name: "max-minus-one", text: strings.Repeat("x", maxMessageBytes-1)},
		{name: "max", text: strings.Repeat("y", maxMessageBytes)},
	}
	ids := []int32{0, math.MaxInt32}
	modes := []string{storage.ModeBuffered, storage.ModeUnbuffered}
	ctx := tlsContext("leader")

	for _, mode := range modes {
		server := &MemberServer{DataDir: t.TempDir(), IOMode: mode}
		for _, id := range ids {
			for _, payload := range payloads {
				result, err := server.Store(ctx, &pb.StoredMessage{Id: id, Text: payload.text})
				if err != nil || result == nil || !result.Ok {
					t.Fatalf("mode=%s id=%d payload=%s: result=%+v err=%v", mode, id, payload.name, result, err)
				}
				got, err := server.Retrieve(ctx, &pb.MessageID{Id: id})
				if err != nil || got.Text != payload.text {
					t.Fatalf("mode=%s id=%d payload=%s: got bytes=%d err=%v", mode, id, payload.name, len(got.GetText()), err)
				}
			}
		}

		if _, err := server.Store(ctx, &pb.StoredMessage{Id: 1, Text: strings.Repeat("z", maxMessageBytes+1)}); status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("mode=%s oversized payload: %v", mode, err)
		}
		if _, err := server.Store(ctx, &pb.StoredMessage{Id: -1, Text: "value"}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("mode=%s negative id: %v", mode, err)
		}

		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := server.Store(cancelled, &pb.StoredMessage{Id: 2, Text: "value"}); status.Code(err) != codes.Canceled {
			t.Fatalf("mode=%s cancelled context: %v", mode, err)
		}
		expired, stop := context.WithDeadline(ctx, time.Now().Add(-time.Second))
		defer stop()
		if _, err := server.Retrieve(expired, &pb.MessageID{Id: 0}); status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("mode=%s expired context: %v", mode, err)
		}
	}
}

func TestMemberRejectsEveryInvalidIOMode(t *testing.T) {
	for _, mode := range []string{"", "BUFFERED", "sync", "unknown"} {
		server := &MemberServer{DataDir: t.TempDir(), IOMode: mode}
		if _, err := server.Store(tlsContext("leader"), &pb.StoredMessage{Id: 1, Text: "value"}); status.Code(err) != codes.Internal {
			t.Fatalf("mode=%q: expected Internal, got %v", mode, err)
		}
	}
}
