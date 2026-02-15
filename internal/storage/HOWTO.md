# Tolerex Storage Operations

This directory contains storage read/write logic for member data.

## Run Tests

```bash
go test ./internal/storage -v
```

## Run Benchmarks

Basic storage benchmarks:

```bash
go test ./internal/storage -run '^$' -bench 'BenchmarkWriteMessage_' -benchmem
```

Long profiling run (optional):

```powershell
go test ./internal/storage -run '^$' -bench 'BenchmarkWriteMessage_' -benchtime=300s -benchmem -count=1 -timeout=30m -cpuprofile=cpu.pprof -memprofile=mem.pprof -blockprofile=block.pprof -mutexprofile=mutex.pprof -trace=trace.out | Tee-Object -FilePath "benchmark_detailed_report.txt"
```

Generated files:

- `benchmark_detailed_report.txt`
- `cpu.pprof`
- `mem.pprof`
- `block.pprof`
- `mutex.pprof`
- `trace.out`

Analysis commands:

1. `go tool pprof -http=:8080 cpu.pprof`
2. `go tool trace trace.out`

## I/O Modes

- `buffered`: higher throughput for sequential write workload.
- `unbuffered`: simpler write path with stronger immediate durability semantics.
