<#
.SYNOPSIS
    Runs Go tests and benchmarks for the Tolerex project.

.DESCRIPTION
    This script executes 'go test' for specific packages.
    It supports flags for race detection (-Race) and benchmarking (-Bench).
    It also handles setting up environment variables for optional profiling.

.PARAMETER Race
    Enables the Go race detector (-race). Slower, but essential for concurrent code.

.PARAMETER Bench
    Runs benchmarks (-bench) instead of standard tests.

.PARAMETER Profile
    Runs a special CPU profiling test case (TestLeader_CPUProfile).
#>
param(
    [switch]$Race,
    [switch]$Bench,
    [switch]$Profile
)

$ErrorActionPreference = "Stop"

Write-Host "==> Running targeted unit tests"
go test ./internal/config ./internal/storage ./internal/server/balancer ./internal/server/leader

if ($Race) {
    Write-Host "==> Running race test (leader package)"
    go test -race ./internal/server/leader
}

if ($Bench) {
    Write-Host "==> Running storage + balancer benchmarks"
    go test -run '^$' -bench 'Benchmark(WriteMessage_|LeastLoaded_|P2C_)' ./internal/storage ./internal/server/balancer -benchmem
}

if ($Profile) {
    Write-Host "==> Running optional CPU profile test"
    $old = $env:TOLEREX_RUN_PROFILE_TEST
    $env:TOLEREX_RUN_PROFILE_TEST = "1"
    try {
        go test ./internal/server/leader -run TestLeader_CPUProfile -v
    }
    finally {
        if ($null -eq $old) {
            Remove-Item Env:TOLEREX_RUN_PROFILE_TEST -ErrorAction SilentlyContinue
        } else {
            $env:TOLEREX_RUN_PROFILE_TEST = $old
        }
    }
}

Write-Host "==> Done"
