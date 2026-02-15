package leader

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"tolerex/internal/logger"
	"tolerex/internal/server/shared"
	pb "tolerex/proto/gen"
)

func init() {
	logger.Leader = log.New(io.Discard, "", 0)
	logger.Member = log.New(io.Discard, "", 0)
}

func TestNewLeaderServer_InitState(t *testing.T) {
	// --- TEST ISOLATION ---
	tmp := t.TempDir()
	t.Setenv("TOLEREX_TEST_MODE", "1")
	t.Setenv("TOLEREX_TEST_DIR", tmp)

	// --- CREATE TEMP TOLERANCE FILE ---
	toleranceFile := filepath.Join(tmp, "tolerance.conf")
	if err := os.WriteFile(
		toleranceFile,
		[]byte("TOLERANCE=4\n"),
		0644,
	); err != nil {
		t.Fatalf("failed to write tolerance file: %v", err)
	}

	// --- INPUT MEMBERS ---
	members := []string{
		"127.0.0.1:7001",
		"127.0.0.1:7002",
	}

	// --- CREATE LEADER ---
	leader, err := NewLeaderServer(members, toleranceFile)
	if err != nil {
		t.Fatalf("NewLeaderServer failed: %v", err)
	}

	// --- BASIC ASSERTIONS ---
	if leader == nil {
		t.Fatal("leader is nil")
	}

	if leader.Tolerance != 4 {
		t.Errorf("expected tolerance=4, got %d", leader.Tolerance)
	}

	// --- MEMBERS ---
	if len(leader.Members) != 2 {
		t.Errorf("expected 2 members, got %d", len(leader.Members))
	}

	// --- MEMBER LOG ---
	if len(leader.MemberLog) != 2 {
		t.Errorf("expected 2 memberLog entries, got %d", len(leader.MemberLog))
	}

	for _, addr := range members {
		info, ok := leader.MemberLog[addr]
		if !ok {
			t.Fatalf("member %s missing from MemberLog", addr)
		}
		if !info.Alive {
			t.Errorf("member %s should be alive", addr)
		}
	}

	// --- MESSAGE MAP ---
	if leader.MsgMap == nil {
		t.Fatal("MsgMap is nil")
	}
	if len(leader.MsgMap) != 0 {
		t.Errorf("expected empty MsgMap, got %d entries", len(leader.MsgMap))
	}

	if leader.memberDialOpt == nil {
		t.Fatal("memberDialOpt should be initialized even in test mode")
	}
}

func TestStore_NotEnoughAliveMembers(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TOLEREX_TEST_MODE", "1")
	t.Setenv("TOLEREX_TEST_DIR", tmp)

	s := &LeaderServer{
		Members:   []string{"m1", "m2"},
		Tolerance: 2,
		MsgMap:    make(map[int][]string),
		MemberLog: map[string]*shared.MemberInfo{
			"m1": {Address: "m1", Alive: true},
			"m2": {Address: "m2", Alive: false},
		},
		replicateFn: func(ctx context.Context, addr string, msg *pb.StoredMessage) bool {
			return true
		},
	}

	res, err := s.Store(context.Background(), &pb.StoredMessage{Id: 1, Text: "x"})
	if err != nil {
		t.Fatalf("Store returned error: %v", err)
	}
	if res == nil || res.Ok {
		t.Fatalf("expected store failure, got res=%+v", res)
	}
	if !strings.Contains(res.Err, "Not enough alive members") {
		t.Fatalf("expected not-enough-alive-members err, got: %q", res.Err)
	}
}

func TestStore_ReplicationIncomplete(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TOLEREX_TEST_MODE", "1")
	t.Setenv("TOLEREX_TEST_DIR", tmp)

	s := &LeaderServer{
		Members:   []string{"m1", "m2", "m3"},
		Tolerance: 2,
		MsgMap:    make(map[int][]string),
		MemberLog: map[string]*shared.MemberInfo{
			"m1": {Address: "m1", Alive: true},
			"m2": {Address: "m2", Alive: true},
			"m3": {Address: "m3", Alive: true},
		},
		replicateFn: func(ctx context.Context, addr string, msg *pb.StoredMessage) bool {
			return addr == "m1"
		},
	}

	res, err := s.Store(context.Background(), &pb.StoredMessage{Id: 2, Text: "x"})
	if err != nil {
		t.Fatalf("Store returned error: %v", err)
	}
	if res == nil || res.Ok {
		t.Fatalf("expected incomplete replication failure, got res=%+v", res)
	}
	if !strings.Contains(res.Err, "Replication incomplete") {
		t.Fatalf("expected replication-incomplete err, got: %q", res.Err)
	}
}

func TestStore_SuccessUpdatesMsgMapAndCounts(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TOLEREX_TEST_MODE", "1")
	t.Setenv("TOLEREX_TEST_DIR", tmp)

	s := &LeaderServer{
		Members:   []string{"m1", "m2", "m3"},
		Tolerance: 2,
		MsgMap:    make(map[int][]string),
		MemberLog: map[string]*shared.MemberInfo{
			"m1": {Address: "m1", Alive: true},
			"m2": {Address: "m2", Alive: true},
			"m3": {Address: "m3", Alive: true},
		},
		replicateFn: func(ctx context.Context, addr string, msg *pb.StoredMessage) bool {
			return true
		},
	}

	res, err := s.Store(context.Background(), &pb.StoredMessage{Id: 3, Text: "x"})
	if err != nil {
		t.Fatalf("Store returned error: %v", err)
	}
	if res == nil || !res.Ok {
		t.Fatalf("expected store success, got res=%+v", res)
	}

	replicas, ok := s.MsgMap[3]
	if !ok {
		t.Fatal("expected msg id=3 to be in MsgMap")
	}
	if len(replicas) != 2 {
		t.Fatalf("expected 2 replicas, got %d (%v)", len(replicas), replicas)
	}

	totalReplicas := 0
	for _, info := range s.MemberLog {
		totalReplicas += info.MessageCnt
	}
	if totalReplicas != 2 {
		t.Fatalf("expected total message count increase=2, got %d", totalReplicas)
	}
}
