package leader

import (
	"context"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"tolerex/internal/server/shared"
	pb "tolerex/proto/gen"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type retrieveStubServer struct {
	pb.UnimplementedStorageServiceServer
	text string
	err  error
}

func (s *retrieveStubServer) Retrieve(ctx context.Context, req *pb.MessageID) (*pb.StoredMessage, error) {
	if s.err != nil {
		return nil, s.err
	}
	return &pb.StoredMessage{
		Id:   req.Id,
		Text: s.text,
	}, nil
}

func startRetrieveGRPCServer(t *testing.T, srv pb.StorageServiceServer) (addr string, stop func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterStorageServiceServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	stop = func() {
		grpcServer.Stop()
		_ = lis.Close()
	}
	return lis.Addr().String(), stop
}

func dialTestGRPC(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatalf("dial failed for %s: %v", addr, err)
	}
	return conn
}

func TestRetrieve_FallbackToAllMembers_WhenMsgMapMissing(t *testing.T) {
	t.Setenv("TOLEREX_TEST_MODE", "0")

	addr, stop := startRetrieveGRPCServer(t, &retrieveStubServer{text: "fallback-ok"})
	defer stop()

	conn := dialTestGRPC(t, addr)
	defer conn.Close()

	var mu sync.Mutex
	var dialed []string

	s := &LeaderServer{
		Members: []string{"m1", "m2"},
		MsgMap:  map[int][]string{},
		MemberLog: map[string]*shared.MemberInfo{
			"m1": {Address: "m1", Alive: false},
			"m2": {Address: "m2", Alive: true},
		},
		dialFn: func(addr string) (*grpc.ClientConn, error) {
			mu.Lock()
			dialed = append(dialed, addr)
			mu.Unlock()

			if addr == "m2" {
				return conn, nil
			}
			return nil, status.Error(codes.Unavailable, "unreachable")
		},
	}

	res, err := s.Retrieve(context.Background(), &pb.MessageID{Id: 42})
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	}
	if res == nil || res.Text != "fallback-ok" {
		t.Fatalf("unexpected retrieve result: %+v", res)
	}

	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(dialed, []string{"m2"}) {
		t.Fatalf("unexpected dial order/calls: got=%v want=%v", dialed, []string{"m2"})
	}
}

func TestRetrieve_SkipsDownReplica_UsesAliveReplica(t *testing.T) {
	t.Setenv("TOLEREX_TEST_MODE", "0")

	addr, stop := startRetrieveGRPCServer(t, &retrieveStubServer{text: "replica-ok"})
	defer stop()

	conn := dialTestGRPC(t, addr)
	defer conn.Close()

	var mu sync.Mutex
	var dialed []string

	s := &LeaderServer{
		Members: []string{"m1", "m2"},
		MsgMap: map[int][]string{
			7: {"m1", "m2"},
		},
		MemberLog: map[string]*shared.MemberInfo{
			"m1": {Address: "m1", Alive: false},
			"m2": {Address: "m2", Alive: true},
		},
		dialFn: func(addr string) (*grpc.ClientConn, error) {
			mu.Lock()
			dialed = append(dialed, addr)
			mu.Unlock()
			return conn, nil
		},
	}

	res, err := s.Retrieve(context.Background(), &pb.MessageID{Id: 7})
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	}
	if res == nil || res.Text != "replica-ok" {
		t.Fatalf("unexpected retrieve result: %+v", res)
	}

	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(dialed, []string{"m2"}) {
		t.Fatalf("down replica should be skipped, got dialed=%v", dialed)
	}
}

func TestRetrieve_NotFound_WhenAllTargetsFail(t *testing.T) {
	t.Setenv("TOLEREX_TEST_MODE", "0")

	var mu sync.Mutex
	var dialed []string

	s := &LeaderServer{
		Members: []string{"m1", "m2"},
		MsgMap: map[int][]string{
			99: {"m1", "m2"},
		},
		MemberLog: map[string]*shared.MemberInfo{
			"m1": {Address: "m1", Alive: true},
			"m2": {Address: "m2", Alive: true},
		},
		dialFn: func(addr string) (*grpc.ClientConn, error) {
			mu.Lock()
			dialed = append(dialed, addr)
			mu.Unlock()
			return nil, status.Error(codes.Unavailable, "dial failed")
		},
	}

	res, err := s.Retrieve(context.Background(), &pb.MessageID{Id: 99})
	if err == nil {
		t.Fatalf("expected not found error, got res=%+v", res)
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.NotFound {
		t.Fatalf("expected NotFound, got err=%v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(dialed, []string{"m1", "m2"}) {
		t.Fatalf("unexpected dialed targets: got=%v want=%v", dialed, []string{"m1", "m2"})
	}
}

func TestRetrieve_TestModeStillUsesReplicaPath(t *testing.T) {
	t.Setenv("TOLEREX_TEST_MODE", "1")

	addr, stop := startRetrieveGRPCServer(t, &retrieveStubServer{text: "from-replica"})
	defer stop()

	conn := dialTestGRPC(t, addr)
	defer conn.Close()

	var mu sync.Mutex
	var dialed []string

	s := &LeaderServer{
		Members: []string{"m1"},
		MsgMap: map[int][]string{
			77: {"m1"},
		},
		MemberLog: map[string]*shared.MemberInfo{
			"m1": {Address: "m1", Alive: true},
		},
		dialFn: func(addr string) (*grpc.ClientConn, error) {
			mu.Lock()
			dialed = append(dialed, addr)
			mu.Unlock()
			return conn, nil
		},
	}

	res, err := s.Retrieve(context.Background(), &pb.MessageID{Id: 77})
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	}
	if res == nil || res.Text != "from-replica" {
		t.Fatalf("unexpected retrieve result: %+v", res)
	}

	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(dialed, []string{"m1"}) {
		t.Fatalf("expected replica path dial, got=%v", dialed)
	}
}
