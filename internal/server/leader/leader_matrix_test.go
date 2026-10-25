package leader

import (
	"context"
	"fmt"
	"math/bits"
	"strings"
	"sync"
	"testing"

	"tolerex/internal/server/balancer"
	"tolerex/internal/server/shared"
	pb "tolerex/proto/gen"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestStoreReplicationCombinationMatrix(t *testing.T) {
	strategies := []struct {
		name string
		new  func() balancer.LoadBalancer
	}{
		{name: "least-loaded", new: func() balancer.LoadBalancer { return &balancer.LeastLoadedBalancer{} }},
		{name: "p2c", new: func() balancer.LoadBalancer { return &balancer.PowerOfTwoChoicesBalancer{} }},
	}

	for _, strategy := range strategies {
		for memberCount := 0; memberCount <= 5; memberCount++ {
			maskLimit := 1 << memberCount
			for tolerance := -1; tolerance <= memberCount+2; tolerance++ {
				for aliveMask := 0; aliveMask < maskLimit; aliveMask++ {
					for successMask := 0; successMask < maskLimit; successMask++ {
						members := make([]string, memberCount)
						memberLog := make(map[string]*shared.MemberInfo, memberCount)
						indexes := make(map[string]int, memberCount)
						for index := range memberCount {
							address := fmt.Sprintf("member-%d", index)
							members[index] = address
							indexes[address] = index
							memberLog[address] = &shared.MemberInfo{
								Address:    address,
								Alive:      aliveMask&(1<<index) != 0,
								MessageCnt: (index * 7) % 4,
							}
						}

						var attemptsMu sync.Mutex
						attempts := make(map[string]int)
						server := &LeaderServer{
							Members:       members,
							Tolerance:     tolerance,
							MsgMap:        make(map[int][]string),
							MemberLog:     memberLog,
							Balancer:      strategy.new(),
							persistSignal: make(chan struct{}, 1),
						}
						server.replicateFn = func(_ context.Context, address string, _ *pb.StoredMessage) bool {
							attemptsMu.Lock()
							attempts[address]++
							attemptsMu.Unlock()
							return successMask&(1<<indexes[address]) != 0
						}

						result, err := server.Store(context.Background(), &pb.StoredMessage{Id: 1, Text: "matrix"})
						label := fmt.Sprintf("strategy=%s members=%d tolerance=%d alive=%05b success=%05b", strategy.name, memberCount, tolerance, aliveMask, successMask)
						if tolerance <= 0 {
							if status.Code(err) != codes.FailedPrecondition || result != nil {
								t.Fatalf("%s: invalid tolerance result=%+v err=%v", label, result, err)
							}
							continue
						}
						if err != nil {
							t.Fatalf("%s: unexpected error: %v", label, err)
						}

						aliveSuccesses := bits.OnesCount(uint(aliveMask & successMask))
						aliveCount := bits.OnesCount(uint(aliveMask))
						wantSuccess := aliveCount >= tolerance && aliveSuccesses >= tolerance
						if result == nil || result.Ok != wantSuccess {
							t.Fatalf("%s: result=%+v wantSuccess=%v", label, result, wantSuccess)
						}

						attemptsMu.Lock()
						for address, count := range attempts {
							index := indexes[address]
							if count != 1 || aliveMask&(1<<index) == 0 {
								t.Fatalf("%s: invalid attempts=%v", label, attempts)
							}
						}
						attemptsMu.Unlock()

						if wantSuccess {
							replicas := server.MsgMap[1]
							if len(replicas) != tolerance {
								t.Fatalf("%s: replicas=%v", label, replicas)
							}
							seen := make(map[string]struct{}, tolerance)
							for _, address := range replicas {
								index := indexes[address]
								if aliveMask&(1<<index) == 0 || successMask&(1<<index) == 0 {
									t.Fatalf("%s: invalid replica %s", label, address)
								}
								if _, duplicate := seen[address]; duplicate {
									t.Fatalf("%s: duplicate replica %s", label, address)
								}
								seen[address] = struct{}{}
							}
						}
					}
				}
			}
		}
	}
}

func TestStoreInputBoundaryMatrix(t *testing.T) {
	server := &LeaderServer{
		Members:       []string{"member"},
		Tolerance:     1,
		MsgMap:        make(map[int][]string),
		MemberLog:     map[string]*shared.MemberInfo{"member": {Address: "member", Alive: true}},
		persistSignal: make(chan struct{}, 1),
		replicateFn:   func(context.Context, string, *pb.StoredMessage) bool { return true },
	}
	for _, test := range []struct {
		name string
		msg  *pb.StoredMessage
		code codes.Code
	}{
		{name: "nil", msg: nil, code: codes.InvalidArgument},
		{name: "negative-id", msg: &pb.StoredMessage{Id: -1}, code: codes.InvalidArgument},
		{name: "empty", msg: &pb.StoredMessage{Id: 0, Text: ""}, code: codes.OK},
		{name: "unicode", msg: &pb.StoredMessage{Id: 1, Text: "Türkçe 世界 🚀"}, code: codes.OK},
		{name: "maximum", msg: &pb.StoredMessage{Id: 2, Text: strings.Repeat("x", maxMessageBytes)}, code: codes.OK},
		{name: "oversized", msg: &pb.StoredMessage{Id: 3, Text: strings.Repeat("x", maxMessageBytes+1)}, code: codes.ResourceExhausted},
	} {
		result, err := server.Store(context.Background(), test.msg)
		if status.Code(err) != test.code {
			t.Fatalf("%s: result=%+v err=%v", test.name, result, err)
		}
		if test.code == codes.OK && (result == nil || !result.Ok) {
			t.Fatalf("%s: result=%+v", test.name, result)
		}
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := server.Store(cancelled, &pb.StoredMessage{Id: 4, Text: "value"}); status.Code(err) != codes.Canceled {
		t.Fatalf("cancelled context: %v", err)
	}
}

func TestRetrieveCombinationMatrix(t *testing.T) {
	const messageID int32 = 42
	const (
		outcomeSuccess = iota
		outcomeNotFound
		outcomeUnavailable
		outcomeWrongID
	)
	for memberCount := 0; memberCount <= 4; memberCount++ {
		memberLimit := 1 << memberCount
		outcomeLimit := 1
		for range memberCount {
			outcomeLimit *= 4
		}
		for aliveMask := 0; aliveMask < memberLimit; aliveMask++ {
			for replicaMask := -1; replicaMask < memberLimit; replicaMask++ {
				for encodedOutcomes := 0; encodedOutcomes < outcomeLimit; encodedOutcomes++ {
					members := make([]string, memberCount)
					memberLog := make(map[string]*shared.MemberInfo, memberCount)
					outcomes := make(map[string]int, memberCount)
					value := encodedOutcomes
					for index := range memberCount {
						address := fmt.Sprintf("member-%d", index)
						members[index] = address
						memberLog[address] = &shared.MemberInfo{Address: address, Alive: aliveMask&(1<<index) != 0}
						outcomes[address] = value % 4
						value /= 4
					}
					msgMap := make(map[int][]string)
					if replicaMask >= 0 {
						for index, address := range members {
							if replicaMask&(1<<index) != 0 {
								msgMap[int(messageID)] = append(msgMap[int(messageID)], address)
							}
						}
					}
					var attempted []string
					server := &LeaderServer{
						Members:       members,
						MemberLog:     memberLog,
						MsgMap:        msgMap,
						persistSignal: make(chan struct{}, 1),
					}
					server.retrieveFn = func(_ context.Context, address string, request *pb.MessageID) (*pb.StoredMessage, error) {
						attempted = append(attempted, address)
						switch outcomes[address] {
						case outcomeSuccess:
							return &pb.StoredMessage{Id: request.Id, Text: address}, nil
						case outcomeNotFound:
							return nil, status.Error(codes.NotFound, "missing")
						case outcomeUnavailable:
							return nil, status.Error(codes.Unavailable, "down")
						default:
							return &pb.StoredMessage{Id: request.Id + 1, Text: "wrong"}, nil
						}
					}

					targets := members
					if replicaMask > 0 {
						targets = msgMap[int(messageID)]
					}
					var wantAttempts []string
					wantSuccess := false
					wantText := ""
					wantCode := codes.NotFound
					skippedUnavailable := false
					sawUnavailable := false
					sawDataLoss := false
					for _, address := range targets {
						index := indexesAddress(members, address)
						if aliveMask&(1<<index) == 0 {
							skippedUnavailable = true
							continue
						}
						wantAttempts = append(wantAttempts, address)
						if outcomes[address] == outcomeSuccess {
							wantSuccess = true
							wantText = address
							break
						}
						if outcomes[address] == outcomeWrongID {
							sawDataLoss = true
						} else if outcomes[address] == outcomeUnavailable {
							sawUnavailable = true
						}
					}
					if !wantSuccess {
						if sawDataLoss {
							wantCode = codes.DataLoss
						} else if sawUnavailable || skippedUnavailable {
							wantCode = codes.Unavailable
						}
					}

					result, err := server.Retrieve(context.Background(), &pb.MessageID{Id: messageID})
					label := fmt.Sprintf("members=%d alive=%04b replicas=%d outcomes=%d", memberCount, aliveMask, replicaMask, encodedOutcomes)
					if wantSuccess {
						if err != nil || result == nil || result.Text != wantText {
							t.Fatalf("%s: result=%+v err=%v", label, result, err)
						}
					} else if status.Code(err) != wantCode || result != nil {
						t.Fatalf("%s: result=%+v code=%s want=%s err=%v", label, result, status.Code(err), wantCode, err)
					}
					if len(attempted) != len(wantAttempts) {
						t.Fatalf("%s: attempted=%v want=%v", label, attempted, wantAttempts)
					}
					for index := range attempted {
						if attempted[index] != wantAttempts[index] {
							t.Fatalf("%s: attempted=%v want=%v", label, attempted, wantAttempts)
						}
					}
				}
			}
		}
	}
}

func indexesAddress(members []string, address string) int {
	for index, member := range members {
		if member == address {
			return index
		}
	}
	return -1
}
