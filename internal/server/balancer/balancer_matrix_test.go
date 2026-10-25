package balancer

import (
	"fmt"
	"slices"
	"sort"
	"testing"
)

func forEachLoadVector(size, valueCount int, visit func([]MemberLoad)) {
	total := 1
	for range size {
		total *= valueCount
	}
	for encoded := 0; encoded < total; encoded++ {
		value := encoded
		members := make([]MemberLoad, size)
		for index := range members {
			members[index] = MemberLoad{Addr: fmt.Sprintf("member-%02d", index), Count: value % valueCount}
			value /= valueCount
		}
		visit(members)
	}
}

func expectedSelectionSize(candidateCount, tolerance int) int {
	if tolerance <= 0 || candidateCount == 0 {
		return 0
	}
	return min(candidateCount, tolerance)
}

func assertValidSelection(t *testing.T, candidates, selected []MemberLoad, tolerance int) {
	t.Helper()
	wantSize := expectedSelectionSize(len(candidates), tolerance)
	if len(selected) != wantSize {
		t.Fatalf("candidates=%v tolerance=%d: selected %d, want %d", candidates, tolerance, len(selected), wantSize)
	}
	available := make(map[string]MemberLoad, len(candidates))
	for _, candidate := range candidates {
		available[candidate.Addr] = candidate
	}
	seen := make(map[string]struct{}, len(selected))
	for _, member := range selected {
		original, ok := available[member.Addr]
		if !ok || original != member {
			t.Fatalf("unknown or modified selection %+v from %v", member, candidates)
		}
		if _, duplicate := seen[member.Addr]; duplicate {
			t.Fatalf("duplicate selection %q from %v", member.Addr, candidates)
		}
		seen[member.Addr] = struct{}{}
	}
}

func TestBalancerCombinationMatrix(t *testing.T) {
	leastLoaded := &LeastLoadedBalancer{}
	p2c := &PowerOfTwoChoicesBalancer{}

	for size := 0; size <= 6; size++ {
		forEachLoadVector(size, 4, func(candidates []MemberLoad) {
			for tolerance := -2; tolerance <= size+2; tolerance++ {
				leastInput := append([]MemberLoad(nil), candidates...)
				got := leastLoaded.Pick(leastInput, tolerance)
				assertValidSelection(t, candidates, got, tolerance)

				want := append([]MemberLoad(nil), candidates...)
				sort.Slice(want, func(i, j int) bool {
					if want[i].Count != want[j].Count {
						return want[i].Count < want[j].Count
					}
					return want[i].Addr < want[j].Addr
				})
				want = want[:expectedSelectionSize(len(want), tolerance)]
				if !slices.Equal(got, want) {
					t.Fatalf("least-loaded candidates=%v tolerance=%d: got=%v want=%v", candidates, tolerance, got, want)
				}

				p2cInput := append([]MemberLoad(nil), candidates...)
				p2cResult := p2c.Pick(p2cInput, tolerance)
				assertValidSelection(t, candidates, p2cResult, tolerance)
				if !slices.Equal(p2cInput, candidates) {
					t.Fatalf("P2C mutated its input: got=%v want=%v", p2cInput, candidates)
				}
			}
		})
	}
}
