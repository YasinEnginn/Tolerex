package balancer

import (
	"fmt"
	"testing"
)

// Helper to generate N dummy members
func generateMembers(n int) []MemberLoad {
	m := make([]MemberLoad, n)
	for i := 0; i < n; i++ {
		m[i] = MemberLoad{
			Addr:  fmt.Sprintf("member-%d", i),
			Count: i * 10, // Deterministic load
		}
	}
	return m
}

func benchmarkLeastLoadedSortCost(b *testing.B, n, tolerance int) {
	balancer := &LeastLoadedBalancer{}
	members := generateMembers(n)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// LeastLoaded sorts in-place, so we copy to emulate a fresh per-request snapshot.
		snapshot := append([]MemberLoad(nil), members...)
		_ = balancer.Pick(snapshot, tolerance)
	}
}

func benchmarkP2CSparsePoolCost(b *testing.B, n, tolerance int) {
	balancer := &PowerOfTwoChoicesBalancer{}
	members := generateMembers(n)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Sparse-pool P2C avoids full O(N) candidate copies for small tolerance.
		_ = balancer.Pick(members, tolerance)
	}
}

func BenchmarkLeastLoaded_SortBased_O_NLogN_100(b *testing.B) {
	benchmarkLeastLoadedSortCost(b, 100, 3)
}

func BenchmarkLeastLoaded_SortBased_O_NLogN_10000(b *testing.B) {
	benchmarkLeastLoadedSortCost(b, 10000, 3)
}

func BenchmarkP2C_SparsePool_NearO_Tolerance_100(b *testing.B) {
	benchmarkP2CSparsePoolCost(b, 100, 3)
}

func BenchmarkP2C_SparsePool_NearO_Tolerance_10000(b *testing.B) {
	benchmarkP2CSparsePoolCost(b, 10000, 3)
}
