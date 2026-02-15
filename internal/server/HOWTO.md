# Tolerex Server Operations

This directory contains cluster coordination logic (leader/member/balancer).

## Tests

Unit and integration tests:

```bash
go test ./internal/server/... -v
```

Race detection:

```bash
go test -race ./internal/server/...
```

## CPU Profile Test

The leader profile test is gated by an env var.

Run:

```powershell
$env:TOLEREX_RUN_PROFILE_TEST='1'
go test ./internal/server/leader -run TestLeader_CPUProfile -v
```

The test writes `leader_cpu.prof` into a temporary directory and prints the full path in test logs.
Use that path with pprof:

```bash
go tool pprof <PROFILE_PATH>
go tool pprof -http=:8080 <PROFILE_PATH>
```

## Package Layout

- `leader/`: cluster state, membership, replication orchestration
- `member/`: storage RPC handlers
- `balancer/`: member selection strategies
- `shared/`: shared models
