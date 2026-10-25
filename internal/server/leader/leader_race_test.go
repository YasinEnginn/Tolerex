package leader

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"testing"
	"time"

	"tolerex/internal/logger"
	"tolerex/internal/server/shared"
	pb "tolerex/proto/gen"
)

func TestLeaderServer_ConcurrentStore(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TOLEREX_TEST_MODE", "1")
	t.Setenv("TOLEREX_TEST_DIR", tmp)
	logger.Leader = log.New(io.Discard, "", 0)
	logger.Member = log.New(io.Discard, "", 0)

	s := &LeaderServer{
		Members:   []string{"m1", "m2", "m3"},
		Tolerance: 2,
		MsgMap:    make(map[int][]string),
		MemberLog: map[string]*shared.MemberInfo{
			"m1": {Address: "m1", Alive: true},
			"m2": {Address: "m2", Alive: true},
			"m3": {Address: "m3", Alive: true},
		},

		//NETWORK + gRPC COMPLETELY DISABLED
		replicateFn: func(ctx context.Context, addr string, msg *pb.StoredMessage) bool {
			return true // pretend replication succeeded
		},
	}
	s.StartStatePersister(10 * time.Millisecond)
	defer func() {
		if err := s.StopStatePersister(); err != nil {
			t.Errorf("state flush failed: %v", err)
		}
	}()

	ctx := context.Background()

	const goroutines = 10
	const writesPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	errs := make(chan error, goroutines*writesPerGoroutine)

	for g := 0; g < goroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < writesPerGoroutine; i++ {
				id := int32(gid*1000 + i)
				res, err := s.Store(ctx, &pb.StoredMessage{
					Id:   id,
					Text: "HELLO",
				})
				if err != nil {
					errs <- err
					continue
				}
				if res == nil || !res.Ok {
					errs <- fmt.Errorf("store not ok: id=%d res=%+v", id, res)
				}
			}
		}(g)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent store failed: %v", err)
		}
	}

	expectedWrites := goroutines * writesPerGoroutine

	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	if len(s.MsgMap) != expectedWrites {
		t.Fatalf("MsgMap entries mismatch: got=%d want=%d", len(s.MsgMap), expectedWrites)
	}

	totalReplicas := 0
	for _, info := range s.MemberLog {
		totalReplicas += info.MessageCnt
	}
	expectedReplicaWrites := expectedWrites * s.Tolerance
	if totalReplicas != expectedReplicaWrites {
		t.Fatalf("member message counts mismatch: got=%d want=%d", totalReplicas, expectedReplicaWrites)
	}
}
