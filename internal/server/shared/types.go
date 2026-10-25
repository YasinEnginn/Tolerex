package shared

import "time"

// MemberInfo is the leader's state for one member.
type MemberInfo struct {
	Address    string
	AddedAt    time.Time
	MessageCnt int
	LastSeen   time.Time
	Alive      bool
}

const (
	StrategyLeastLoaded = "least_loaded"
	StrategyP2C         = "p2c"
)
