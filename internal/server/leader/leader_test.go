package leader

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"tolerex/internal/logger"
	"tolerex/internal/server/shared"
	pb "tolerex/proto/gen"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func init() {
	logger.Leader = log.New(io.Discard, "", 0)
	logger.Member = log.New(io.Discard, "", 0)
}

func TestStoreRetriesAlternateMembers(t *testing.T) {
	t.Setenv("TOLEREX_TEST_MODE", "1")
	t.Setenv("TOLEREX_TEST_DIR", t.TempDir())

	var mu sync.Mutex
	var attempts []string
	s := &LeaderServer{
		Members:   []string{"m1", "m2", "m3"},
		Tolerance: 2,
		MsgMap:    make(map[int][]string),
		MemberLog: map[string]*shared.MemberInfo{
			"m1": {Address: "m1", Alive: true},
			"m2": {Address: "m2", Alive: true},
			"m3": {Address: "m3", Alive: true},
		},
		replicateFn: func(_ context.Context, addr string, _ *pb.StoredMessage) bool {
			mu.Lock()
			attempts = append(attempts, addr)
			mu.Unlock()
			return addr != "m1"
		},
	}

	result, err := s.Store(context.Background(), &pb.StoredMessage{Id: 10, Text: "value"})
	if err != nil || result == nil || !result.Ok {
		t.Fatalf("expected fallback replication success, result=%+v err=%v", result, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(attempts) != 3 {
		t.Fatalf("expected all three members to be attempted, got %v", attempts)
	}
	if s.MemberLog["m1"].Alive {
		t.Fatal("failed member should be marked unavailable")
	}
}

func TestStoreOverwriteDoesNotInflateCounts(t *testing.T) {
	t.Setenv("TOLEREX_TEST_MODE", "1")
	t.Setenv("TOLEREX_TEST_DIR", t.TempDir())

	s := &LeaderServer{
		Members:   []string{"m1", "m2"},
		Tolerance: 2,
		MsgMap:    make(map[int][]string),
		MemberLog: map[string]*shared.MemberInfo{
			"m1": {Address: "m1", Alive: true},
			"m2": {Address: "m2", Alive: true},
		},
		replicateFn: func(context.Context, string, *pb.StoredMessage) bool { return true },
	}

	for _, text := range []string{"first", "second"} {
		result, err := s.Store(context.Background(), &pb.StoredMessage{Id: 11, Text: text})
		if err != nil || result == nil || !result.Ok {
			t.Fatalf("store failed: result=%+v err=%v", result, err)
		}
	}

	total := s.MemberLog["m1"].MessageCnt + s.MemberLog["m2"].MessageCnt
	if total != 2 {
		t.Fatalf("expected two committed replicas after overwrite, got %d", total)
	}
}

func TestStatePersisterFlushesLatestSnapshot(t *testing.T) {
	t.Setenv("TOLEREX_TEST_MODE", "1")
	t.Setenv("TOLEREX_TEST_DIR", t.TempDir())

	s := &LeaderServer{
		Members:   []string{"m1"},
		Tolerance: 1,
		MsgMap:    make(map[int][]string),
		MemberLog: map[string]*shared.MemberInfo{
			"m1": {Address: "m1", Alive: true},
		},
		replicateFn: func(context.Context, string, *pb.StoredMessage) bool { return true },
	}
	s.StartStatePersister(time.Hour)

	result, err := s.Store(context.Background(), &pb.StoredMessage{Id: 23, Text: "pending"})
	if err != nil || result == nil || !result.Ok {
		t.Fatalf("store failed: result=%+v err=%v", result, err)
	}
	if err := s.StopStatePersister(); err != nil {
		t.Fatalf("state flush failed: %v", err)
	}

	data, err := os.ReadFile(stateFilePath())
	if err != nil {
		t.Fatal(err)
	}
	var state LeaderState
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatal(err)
	}
	if replicas := state.MsgMap[23]; len(replicas) != 1 || replicas[0] != "m1" {
		t.Fatalf("unexpected persisted replicas: %v", replicas)
	}
}

func TestValidateMemberAddress(t *testing.T) {
	for _, address := range []string{"localhost:5556", "member-1:5556", "127.0.0.1:5556"} {
		if err := validateMemberAddress(address); err != nil {
			t.Fatalf("valid address %q rejected: %v", address, err)
		}
	}
	for _, address := range []string{"", "member", "0.0.0.0:5556", "member:70000"} {
		if err := validateMemberAddress(address); err == nil {
			t.Fatalf("invalid address %q accepted", address)
		}
	}
}

func TestParseSetCommandPreservesPayloadSpacing(t *testing.T) {
	id, message, ok := parseSetCommand("SET\t42\tmerhaba   dünya tolerex> payload")
	if !ok || id != "42" || message != "merhaba   dünya tolerex> payload" {
		t.Fatalf("unexpected parse result: id=%q message=%q ok=%v", id, message, ok)
	}
	if _, _, ok := parseSetCommand("SET 42"); ok {
		t.Fatal("missing payload should be rejected")
	}
}

func TestHandleClientUsesOneLineResponses(t *testing.T) {
	t.Setenv("TOLEREX_TEST_MODE", "1")
	t.Setenv("TOLEREX_TEST_DIR", t.TempDir())
	s := &LeaderServer{
		Members:   []string{"m1"},
		Tolerance: 1,
		MsgMap:    make(map[int][]string),
		MemberLog: map[string]*shared.MemberInfo{"m1": {Address: "m1", Alive: true}},
		replicateFn: func(context.Context, string, *pb.StoredMessage) bool {
			return true
		},
	}

	client, server := net.Pipe()
	defer client.Close()
	go s.HandleClient(server)
	reader := bufio.NewReader(client)
	writer := bufio.NewWriter(client)

	if greeting, err := reader.ReadString('\n'); err != nil || strings.TrimSpace(greeting) != "Tolerex ready" {
		t.Fatalf("unexpected greeting %q, err=%v", greeting, err)
	}
	_, _ = writer.WriteString("SET 9 prompt tolerex> remains data\r\n")
	_ = writer.Flush()
	if response, err := reader.ReadString('\n'); err != nil || strings.TrimSpace(response) != "OK" {
		t.Fatalf("unexpected SET response %q, err=%v", response, err)
	}
	_, _ = writer.WriteString("STATUS\r\nHELP\r\nQUIT\r\n")
	_ = writer.Flush()
	if response, _ := reader.ReadString('\n'); strings.TrimSpace(response) != "STATUS members=1 alive=1 tolerance=1 messages=1" {
		t.Fatalf("unexpected STATUS response %q", response)
	}
	if response, _ := reader.ReadString('\n'); !strings.HasPrefix(response, "HELP ") {
		t.Fatalf("unexpected HELP response %q", response)
	}
	if response, _ := reader.ReadString('\n'); strings.TrimSpace(response) != "Bye" {
		t.Fatalf("unexpected QUIT response %q", response)
	}
}

func TestTCPCommandCombinationMatrix(t *testing.T) {
	s := &LeaderServer{
		Members:       []string{"member"},
		Tolerance:     1,
		MsgMap:        make(map[int][]string),
		MemberLog:     map[string]*shared.MemberInfo{"member": {Address: "member", Alive: true}},
		persistSignal: make(chan struct{}, 1),
		replicateFn:   func(context.Context, string, *pb.StoredMessage) bool { return true },
		retrieveFn: func(context.Context, string, *pb.MessageID) (*pb.StoredMessage, error) {
			return nil, status.Error(codes.NotFound, "missing")
		},
	}
	client, server := net.Pipe()
	defer client.Close()
	go s.HandleClient(server)
	reader := bufio.NewReader(client)
	writer := bufio.NewWriter(client)
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		command string
		want    string
	}{
		{command: "", want: "ERROR: empty command"},
		{command: "SET", want: "ERROR: SET <id> <message>"},
		{command: "SET 1", want: "ERROR: SET <id> <message>"},
		{command: "SET -1 value", want: "ERROR: id must be a non-negative 32-bit integer"},
		{command: "SET 2147483648 value", want: "ERROR: id must be a non-negative 32-bit integer"},
		{command: "SET text value", want: "ERROR: id must be a non-negative 32-bit integer"},
		{command: "sEt 0 Türkçe  世界 🚀 tolerex> ERROR:", want: "OK"},
		{command: "SET 2147483647 maximum-id", want: "OK"},
		{command: "GET", want: "ERROR: GET <id>"},
		{command: "GET 1 extra", want: "ERROR: GET <id>"},
		{command: "GET -1", want: "ERROR: id must be a non-negative 32-bit integer"},
		{command: "GET text", want: "ERROR: id must be a non-negative 32-bit integer"},
		{command: "GET 999", want: "NOT_FOUND"},
		{command: "status argument", want: "ERROR: STATUS takes no arguments"},
		{command: "StAtUs", want: "STATUS members=1 alive=1 tolerance=1 messages=2"},
		{command: "help", want: "HELP SET <id> <message> | GET <id> | STATUS | HELP | QUIT"},
		{command: "unknown", want: "ERROR: unknown command"},
	}
	for _, test := range tests {
		if _, err := writer.WriteString(test.command + "\r\n"); err != nil {
			t.Fatal(err)
		}
		if err := writer.Flush(); err != nil {
			t.Fatal(err)
		}
		response, err := reader.ReadString('\n')
		if err != nil || strings.TrimSpace(response) != test.want {
			t.Fatalf("command=%q response=%q want=%q err=%v", test.command, response, test.want, err)
		}
	}
	_, _ = writer.WriteString("QUIT\r\n")
	_ = writer.Flush()
	if response, err := reader.ReadString('\n'); err != nil || strings.TrimSpace(response) != "Bye" {
		t.Fatalf("QUIT response=%q err=%v", response, err)
	}
}

func TestTCPCommandLimitAndResynchronization(t *testing.T) {
	s := &LeaderServer{Tolerance: 1, MemberLog: map[string]*shared.MemberInfo{}, MsgMap: map[int][]string{}}
	client, server := net.Pipe()
	defer client.Close()
	go s.HandleClient(server)
	reader := bufio.NewReader(client)
	writer := bufio.NewWriter(client)
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatal(err)
	}

	if _, err := writer.WriteString(strings.Repeat("x", maxCommandBytes+1) + "\r\nSTATUS\r\n"); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	if response, err := reader.ReadString('\n'); err != nil || strings.TrimSpace(response) != "ERROR: command exceeds 4 MiB limit" {
		t.Fatalf("oversized response=%q err=%v", response, err)
	}
	if response, err := reader.ReadString('\n'); err != nil || strings.TrimSpace(response) != "STATUS members=0 alive=0 tolerance=1 messages=0" {
		t.Fatalf("resynchronized response=%q err=%v", response, err)
	}
}

func TestCloseClientsStopsActiveHandlersAndRejectsNewSessions(t *testing.T) {
	s := &LeaderServer{}
	client, server := net.Pipe()
	s.ServeClient(server)
	reader := bufio.NewReader(client)
	if greeting, err := reader.ReadString('\n'); err != nil || strings.TrimSpace(greeting) != "Tolerex ready" {
		t.Fatalf("unexpected greeting %q, err=%v", greeting, err)
	}

	done := make(chan struct{})
	go func() {
		s.CloseClients()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("CloseClients did not wait for and stop the active handler")
	}
	_ = client.Close()

	newClient, newServer := net.Pipe()
	s.ServeClient(newServer)
	_ = newClient.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := bufio.NewReader(newClient).ReadString('\n'); err == nil {
		t.Fatal("new session was accepted after CloseClients")
	}
	_ = newClient.Close()
}

func TestNewLeaderServer_InitState(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TOLEREX_TEST_MODE", "1")
	t.Setenv("TOLEREX_TEST_DIR", tmp)

	toleranceFile := filepath.Join(tmp, "tolerance.conf")
	if err := os.WriteFile(
		toleranceFile,
		[]byte("TOLERANCE=4\n"),
		0644,
	); err != nil {
		t.Fatalf("failed to write tolerance file: %v", err)
	}

	members := []string{
		"127.0.0.1:7001",
		"127.0.0.1:7002",
	}

	leader, err := NewLeaderServer(members, toleranceFile)
	if err != nil {
		t.Fatalf("NewLeaderServer failed: %v", err)
	}

	if leader == nil {
		t.Fatal("leader is nil")
	}

	if leader.Tolerance != 4 {
		t.Errorf("expected tolerance=4, got %d", leader.Tolerance)
	}

	if len(leader.Members) != 2 {
		t.Errorf("expected 2 members, got %d", len(leader.Members))
	}

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

	if leader.MsgMap == nil {
		t.Fatal("MsgMap is nil")
	}
	if len(leader.MsgMap) != 0 {
		t.Errorf("expected empty MsgMap, got %d entries", len(leader.MsgMap))
	}

	if leader.memberDialOpt != nil {
		t.Fatal("memberDialOpt should only be configured by the application bootstrap")
	}
}

func TestNewLeaderServerRejectsCorruptState(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TOLEREX_TEST_MODE", "1")
	t.Setenv("TOLEREX_TEST_DIR", tmp)
	if err := os.WriteFile(filepath.Join(tmp, "leader_state.json"), []byte("not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	toleranceFile := filepath.Join(tmp, "tolerance.conf")
	if err := os.WriteFile(toleranceFile, []byte("TOLERANCE=1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewLeaderServer(nil, toleranceFile); err == nil {
		t.Fatal("expected corrupt state to stop leader initialization")
	}
}

func TestNewLeaderServerBalancerStrategyMatrix(t *testing.T) {
	toleranceFile := filepath.Join(t.TempDir(), "tolerance.conf")
	if err := os.WriteFile(toleranceFile, []byte("TOLERANCE=1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		value    string
		wantType string
	}{
		{value: "", wantType: "*balancer.LeastLoadedBalancer"},
		{value: shared.StrategyLeastLoaded, wantType: "*balancer.LeastLoadedBalancer"},
		{value: shared.StrategyP2C, wantType: "*balancer.PowerOfTwoChoicesBalancer"},
		{value: "unknown", wantType: "*balancer.LeastLoadedBalancer"},
	} {
		t.Setenv("BALANCER_STRATEGY", test.value)
		t.Setenv("TOLEREX_TEST_MODE", "1")
		t.Setenv("TOLEREX_TEST_DIR", t.TempDir())
		server, err := NewLeaderServer(nil, toleranceFile)
		if err != nil {
			t.Fatal(err)
		}
		if got := fmt.Sprintf("%T", server.Balancer); got != test.wantType {
			t.Fatalf("strategy=%q: got %s want %s", test.value, got, test.wantType)
		}
	}
}

func TestRegisterMemberFailsWhenStateCannotBePersisted(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TOLEREX_TEST_MODE", "1")
	t.Setenv("TOLEREX_TEST_DIR", blocker)
	s := &LeaderServer{MemberLog: map[string]*shared.MemberInfo{}, MsgMap: map[int][]string{}}
	if _, err := s.RegisterMember(memberTLSContext("member"), &pb.MemberInfo{Address: "localhost:5556"}); status.Code(err) != codes.Internal {
		t.Fatalf("expected Internal, got %v", err)
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
