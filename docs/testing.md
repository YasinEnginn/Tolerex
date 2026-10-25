# Testing

Run all package tests:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\run-tests.ps1
```

Run all tests with the race detector:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\run-tests.ps1 -Race
```

Run storage and balancer benchmarks:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\run-tests.ps1 -Bench
```

Run the three-minute process chaos test:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\run-chaos-test.ps1
```

The chaos test drives six unbuffered members and six load clients while repeatedly killing one, two, or four members, rapidly flapping a member, and hard-killing the leader immediately after an acknowledged write. It validates live reads/writes, leader recovery, exact replica counts, replica-file presence, and sampled replica contents. Generated binaries, state, and member data are removed after the run; pass `-KeepArtifacts` only when logs are needed for diagnosis.

Direct quality checks:

```powershell
go test ./... -count=1 -cover
go test -race ./... -count=1
go vet ./...
go mod verify
go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...
```

## Combination matrix

The normal `go test ./...` run includes bounded exhaustive matrices:

- both load balancers with 0–6 candidates, four load levels, and every boundary tolerance;
- leader replication with 0–5 members and every alive/success mask;
- retrieval with 0–4 members and every replica-map, liveness, and response outcome;
- buffered/unbuffered storage, payload and ID boundaries, overwrite order, and concurrent same-ID writes;
- TCP commands, response limits, bulk flush sizes, metrics, logging, recovery, and mTLS roles.

The three core decision matrices cover 214,756 deterministic cases. Infinite input space is handled with boundary and property checks rather than claiming literal exhaustiveness.

The CI workflow runs formatting, module verification, vet, tests, the race detector, vulnerability scanning, and Compose validation.

Cleanup commands support `-DryRun` where available:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\clean-test-artifacts.ps1 -DryRun
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\clean-runtime-artifacts.ps1 -DryRun
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\clean-docker-artifacts.ps1 -DryRun
```

CPU profiling for the leader is opt-in:

```powershell
$env:TOLEREX_RUN_PROFILE_TEST='1'
go test ./internal/server/leader -run TestLeader_CPUProfile -v
```
