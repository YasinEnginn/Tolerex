package leader

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime/pprof"
	"testing"

	"tolerex/internal/server/shared"
	pb "tolerex/proto/gen"

	"google.golang.org/grpc"
)

func TestLeader_CPUProfile(t *testing.T) {
	if os.Getenv("TOLEREX_RUN_PROFILE_TEST") != "1" {
		t.Skip("set TOLEREX_RUN_PROFILE_TEST=1 to run CPU profiling test")
	}

	tmp := t.TempDir()
	t.Setenv("TOLEREX_TEST_MODE", "1")
	t.Setenv("TOLEREX_TEST_DIR", tmp)

	conf := filepath.Join(tmp, "tolerance.conf")
	if err := os.WriteFile(conf, []byte("TOLERANCE=1\n"), 0644); err != nil {
		t.Fatal(err)
	}

	leader, err := NewLeaderServer(nil, conf)
	if err != nil {
		t.Fatal(err)
	}

	leader.Members = []string{"m1", "m2", "m3"}
	leader.MemberLog = map[string]*shared.MemberInfo{
		"m1": {Address: "m1", Alive: true},
		"m2": {Address: "m2", Alive: true},
		"m3": {Address: "m3", Alive: true},
	}

	leader.replicateFn = func(ctx context.Context, addr string, msg *pb.StoredMessage) bool {
		return false
	}

	leader.dialFn = func(addr string) (*grpc.ClientConn, error) {
		return nil, errors.New("connection closed")
	}

	prof := filepath.Join(tmp, "leader_cpu.prof")
	f, err := os.Create(prof)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	pprof.StartCPUProfile(f)

	defer pprof.StopCPUProfile()

	ctx := context.Background()

	for i := 0; i < 30_000; i++ {
		_, _ = leader.Store(ctx, &pb.StoredMessage{
			Id:   int32(i),
			Text: "hello",
		})

		_, _ = leader.Retrieve(ctx, &pb.MessageID{
			Id: int32(i),
		})
	}

	t.Logf("CPU profile written to %s", prof)
}
