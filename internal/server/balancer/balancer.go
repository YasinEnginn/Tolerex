package balancer

import (
	"math/rand"
	"sort"
)

// MemberLoad is the minimal load snapshot needed for replica selection.
type MemberLoad struct {
	Addr  string
	Count int // MessageCnt
}

// LoadBalancer abstracts pluggable replica selection strategies.
type LoadBalancer interface {
	// Pick selects up to tolerance targets from candidates.
	// candidates are expected to be pre-filtered (alive members only).
	Pick(candidates []MemberLoad, tolerance int) []MemberLoad
}

// LeastLoadedBalancer selects members with the lowest message count.
type LeastLoadedBalancer struct{}

func (b *LeastLoadedBalancer) Pick(candidates []MemberLoad, tolerance int) []MemberLoad {
	if tolerance <= 0 || len(candidates) == 0 {
		return nil
	}

	// Deterministic sort:
	// 1) lower Count first
	// 2) Addr lexical order as tie-breaker
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].Count != candidates[j].Count {
			return candidates[i].Count < candidates[j].Count
		}
		return candidates[i].Addr < candidates[j].Addr
	})

	if tolerance > len(candidates) {
		tolerance = len(candidates)
	}

	out := make([]MemberLoad, 0, tolerance)
	for i := 0; i < tolerance; i++ {
		out = append(out, candidates[i])
	}
	return out
}

// PowerOfTwoChoicesBalancer applies "Power of Two Choices".
// This implementation keeps an implicit sparse candidate pool and removes only
// winners, avoiding full O(N) candidate copies for small tolerance values.
type PowerOfTwoChoicesBalancer struct{}

func (b *PowerOfTwoChoicesBalancer) Pick(candidates []MemberLoad, tolerance int) []MemberLoad {
	if tolerance <= 0 || len(candidates) == 0 {
		return nil
	}

	// If pool size is already <= tolerance, return all.
	if len(candidates) < tolerance {
		return candidates
	}

	out := make([]MemberLoad, 0, tolerance)
	poolSize := len(candidates)
	// sparse maps virtual pool positions to candidate indices when swapped.
	sparse := make(map[int]int, tolerance)

	getAt := func(pos int) int {
		if v, ok := sparse[pos]; ok {
			return v
		}
		return pos
	}
	setAt := func(pos, val int) {
		if pos == val {
			delete(sparse, pos)
			return
		}
		sparse[pos] = val
	}
	removeAt := func(pos int) {
		lastPos := poolSize - 1
		if pos != lastPos {
			setAt(pos, getAt(lastPos))
		}
		delete(sparse, lastPos)
		poolSize--
	}

	for i := 0; i < tolerance && poolSize > 0; i++ {
		if poolSize == 1 {
			out = append(out, candidates[getAt(0)])
			break
		}

		pos1 := rand.Intn(poolSize)
		// Uniform distinct position without retry loop.
		pos2 := rand.Intn(poolSize - 1)
		if pos2 >= pos1 {
			pos2++
		}

		idx1 := getAt(pos1)
		idx2 := getAt(pos2)

		winnerPos := pos1
		winnerIdx := idx1
		if candidates[idx2].Count < candidates[idx1].Count {
			winnerPos = pos2
			winnerIdx = idx2
		}

		out = append(out, candidates[winnerIdx])
		removeAt(winnerPos)
	}

	return out
}
