package main

import (
	"context"
	"errors"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	proto "tolerex/proto/gen"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type fakeStorageClient struct {
	mu         sync.Mutex
	hbCalls    int
	failOnCall map[int]error
}

func (f *fakeStorageClient) Store(ctx context.Context, in *proto.StoredMessage, opts ...grpc.CallOption) (*proto.StoreResult, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeStorageClient) Retrieve(ctx context.Context, in *proto.MessageID, opts ...grpc.CallOption) (*proto.StoredMessage, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeStorageClient) RegisterMember(ctx context.Context, in *proto.MemberInfo, opts ...grpc.CallOption) (*proto.RegisterReply, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeStorageClient) Heartbeat(ctx context.Context, in *proto.HeartbeatRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.hbCalls++
	if err, ok := f.failOnCall[f.hbCalls]; ok {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (f *fakeStorageClient) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hbCalls
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

func waitDone(t *testing.T, done <-chan struct{}, timeout time.Duration) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatalf("goroutine did not stop within %s", timeout)
	}
}

func TestRunHeartbeatLoop_ReusesConnectionWhileHealthy(t *testing.T) {
	memberLog := log.New(io.Discard, "", 0)
	stop := make(chan struct{})
	done := make(chan struct{})

	client := &fakeStorageClient{failOnCall: map[int]error{}}
	var connectCalls int32
	var closeCalls int32

	connect := func() (*heartbeatSession, error) {
		atomic.AddInt32(&connectCalls, 1)
		return &heartbeatSession{
			client: client,
			close: func() error {
				atomic.AddInt32(&closeCalls, 1)
				return nil
			},
		}, nil
	}

	go func() {
		runHeartbeatLoop(memberLog, "leader:5555", "member-1", 10*time.Millisecond, connect, stop)
		close(done)
	}()

	waitFor(t, 500*time.Millisecond, func() bool {
		return client.Calls() >= 3
	})

	close(stop)
	waitDone(t, done, 500*time.Millisecond)

	if got := atomic.LoadInt32(&connectCalls); got != 1 {
		t.Fatalf("expected one dial for healthy loop, got %d", got)
	}
	if got := atomic.LoadInt32(&closeCalls); got != 1 {
		t.Fatalf("expected one close on shutdown, got %d", got)
	}
}

func TestRunHeartbeatLoop_ReconnectsAfterHeartbeatFailure(t *testing.T) {
	memberLog := log.New(io.Discard, "", 0)
	stop := make(chan struct{})
	done := make(chan struct{})

	first := &fakeStorageClient{
		failOnCall: map[int]error{
			1: errors.New("transient heartbeat failure"),
		},
	}
	second := &fakeStorageClient{failOnCall: map[int]error{}}

	var connectCalls int32
	var closeCalls int32

	connect := func() (*heartbeatSession, error) {
		call := atomic.AddInt32(&connectCalls, 1)
		client := second
		if call == 1 {
			client = first
		}
		return &heartbeatSession{
			client: client,
			close: func() error {
				atomic.AddInt32(&closeCalls, 1)
				return nil
			},
		}, nil
	}

	go func() {
		runHeartbeatLoop(memberLog, "leader:5555", "member-1", 10*time.Millisecond, connect, stop)
		close(done)
	}()

	waitFor(t, 700*time.Millisecond, func() bool {
		return atomic.LoadInt32(&connectCalls) >= 2 && second.Calls() >= 2
	})

	close(stop)
	waitDone(t, done, 500*time.Millisecond)

	if got := atomic.LoadInt32(&connectCalls); got != 2 {
		t.Fatalf("expected reconnect to dial exactly twice, got %d", got)
	}
	if got := first.Calls(); got != 1 {
		t.Fatalf("expected first client to fail on first heartbeat, got calls=%d", got)
	}
	if got := second.Calls(); got < 1 {
		t.Fatalf("expected second client to receive heartbeat(s), got calls=%d", got)
	}
	if got := atomic.LoadInt32(&closeCalls); got != 2 {
		t.Fatalf("expected close for failed and final sessions, got %d", got)
	}
}
