<#
.SYNOPSIS
    Removes temporary artifacts generated during testing.

.DESCRIPTION
    Deletes files and directories created by 'go test' runs, such as:
    - Coverage profiles (*.out)
    - CPU profiles (*.prof)
    - Compiled test binaries
    - Test logs

.PARAMETER DryRun
    If set, prints what would be deleted without actually deleting anything.
#>
param(
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$staticTargets = @(
    "coverage",
    "coverage.out",
    "leader",
    "logs",
    "internal/server/leader_cpu.prof",
    "internal/server/leader/leader_cpu.prof",
    "internal/server/test_race",
    "internal/server/leader/test_race"
)

$dynamicProfTargets = @()
if (Test-Path "internal/server") {
    $dynamicProfTargets = Get-ChildItem -Path "internal/server" -Recurse -File -Filter "*.prof" -ErrorAction SilentlyContinue |
        ForEach-Object { $_.FullName }
}

$targets = ($staticTargets + $dynamicProfTargets) | Sort-Object -Unique

foreach ($target in $targets) {
    if (-not (Test-Path -LiteralPath $target)) {
        continue
    }

    if ($DryRun) {
        Write-Host "[DRY-RUN] remove $target"
        continue
    }

    try {
        $item = Get-Item -LiteralPath $target -Force
        if ($item.PSIsContainer) {
            [System.IO.Directory]::Delete($item.FullName, $true)
        } else {
            [System.IO.File]::Delete($item.FullName)
        }
        Write-Host "removed: $target"
    }
    catch {
        Write-Warning "failed to remove $target : $($_.Exception.Message)"
    }
}

Write-Host "clean-test-artifacts complete"
