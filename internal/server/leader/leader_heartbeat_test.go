package leader

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"testing"
	"time"

	"tolerex/internal/server/shared"
	pb "tolerex/proto/gen"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func memberTLSContext(commonName string) context.Context {
	cert := &x509.Certificate{Subject: pkix.Name{CommonName: commonName}}
	tlsInfo := credentials.TLSInfo{State: tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{cert},
		VerifiedChains:   [][]*x509.Certificate{{cert}},
	}}
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: tlsInfo})
}

func waitUntil(t *testing.T, timeout time.Duration, check func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

func TestHeartbeatWatcher_MarksMemberDownAfterTimeout(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TOLEREX_TEST_MODE", "1")
	t.Setenv("TOLEREX_TEST_DIR", tmp)

	s := &LeaderServer{
		MemberLog: map[string]*shared.MemberInfo{
			"m1": {
				Address:  "m1",
				Alive:    true,
				AddedAt:  time.Now().Add(-time.Second),
				LastSeen: time.Now().Add(-time.Second),
			},
		},
		heartbeatTimeout:  60 * time.Millisecond,
		heartbeatInterval: 15 * time.Millisecond,
		MsgMap:            make(map[int][]string),
	}

	s.StartHeartbeatWatcher()
	defer s.StopHeartbeatWatcher()

	waitUntil(t, 600*time.Millisecond, func() bool {
		s.stateMu.Lock()
		defer s.stateMu.Unlock()
		return !s.MemberLog["m1"].Alive
	})
}

func TestHeartbeatWatcher_KeepsMemberAliveWithFrequentHeartbeats(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TOLEREX_TEST_MODE", "1")
	t.Setenv("TOLEREX_TEST_DIR", tmp)

	s := &LeaderServer{
		MemberLog: map[string]*shared.MemberInfo{
			"m1": {
				Address:  "m1",
				Alive:    true,
				AddedAt:  time.Now(),
				LastSeen: time.Now(),
			},
		},
		heartbeatTimeout:  120 * time.Millisecond,
		heartbeatInterval: 20 * time.Millisecond,
		MsgMap:            make(map[int][]string),
	}

	s.StartHeartbeatWatcher()
	defer s.StopHeartbeatWatcher()

	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_, _ = s.Heartbeat(memberTLSContext("member"), &pb.HeartbeatRequest{Address: "m1"})
			case <-stop:
				return
			}
		}
	}()

	time.Sleep(220 * time.Millisecond)
	close(stop)

	s.stateMu.Lock()
	alive := s.MemberLog["m1"].Alive
	s.stateMu.Unlock()

	if !alive {
		t.Fatal("member should stay alive when heartbeats are frequent")
	}
}

func TestMemberControlRPCsRequireMemberIdentity(t *testing.T) {
	s := &LeaderServer{MemberLog: map[string]*shared.MemberInfo{}, MsgMap: map[int][]string{}}
	if _, err := s.Heartbeat(context.Background(), &pb.HeartbeatRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", err)
	}
	wrongRole := memberTLSContext("leader")
	if _, err := s.RegisterMember(wrongRole, &pb.MemberInfo{Address: "localhost:5556"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}
}
