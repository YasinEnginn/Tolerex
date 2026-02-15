package shared

import "time"

// MemberInfo: Metadata tracked by the Leader for each Member.
type MemberInfo struct {
	Address    string
	AddedAt    time.Time
	MessageCnt int
	LastSeen   time.Time
	Alive      bool
}

// --- LOAD BALANCER STRATEGIES ---
const (
	StrategyLeastLoaded = "least_loaded"
	StrategyP2C         = "p2c"
)
