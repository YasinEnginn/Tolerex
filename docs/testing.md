# Testing Commands

Use the helper script from repository root:

```powershell
.\scripts\run-tests.ps1
```

Or with `make`:

```bash
make test
```

Optional modes:

```powershell
.\scripts\run-tests.ps1 -Race
.\scripts\run-tests.ps1 -Bench
.\scripts\run-tests.ps1 -Race -Bench
.\scripts\run-tests.ps1 -Profile
```

Or with `make`:

```bash
make test-race
make bench
make test-all
```

Equivalent direct `go test` commands:

```powershell
go test ./internal/config ./internal/storage ./internal/server/balancer ./internal/server/leader
go test -race ./internal/server/leader
go test -run '^$' -bench 'Benchmark(WriteMessage_|LeastLoaded_|P2C_)' ./internal/storage ./internal/server/balancer -benchmem
```

Cleanup test/profiling artifacts:

```bash
make clean-test-artifacts
make clean-test-artifacts-dry
```

Cleanup runtime outputs (logs + member data + measured files):

```bash
make clean-runtime-artifacts
make clean-runtime-artifacts-dry
```

Cleanup Docker artifacts created by this repository:

```bash
make clean-docker-artifacts
make clean-docker-artifacts-dry
```

Cleanup everything:

```bash
make clean-all-artifacts
```
