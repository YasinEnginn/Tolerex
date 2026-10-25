param(
    [switch]$Race,
    [switch]$Bench,
    [switch]$Profile
)

$ErrorActionPreference = "Stop"

function Invoke-Go {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)
    & go @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "go $($Arguments -join ' ') failed with exit code $LASTEXITCODE"
    }
}

Write-Host "==> Running all unit tests"
Invoke-Go @("test", "./...", "-count=1")

if ($Race) {
    Write-Host "==> Running race detector for all packages"
    Invoke-Go @("test", "-race", "./...", "-count=1")
}

if ($Bench) {
    Write-Host "==> Running storage + balancer benchmarks"
    Invoke-Go @("test", "-run", '^$', "-bench", 'Benchmark(WriteMessage_|LeastLoaded_|P2C_)', "./internal/storage", "./internal/server/balancer", "-benchmem")
}

if ($Profile) {
    Write-Host "==> Running optional CPU profile test"
    $old = $env:TOLEREX_RUN_PROFILE_TEST
    $env:TOLEREX_RUN_PROFILE_TEST = "1"
    try {
        Invoke-Go @("test", "./internal/server/leader", "-run", "TestLeader_CPUProfile", "-v")
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
