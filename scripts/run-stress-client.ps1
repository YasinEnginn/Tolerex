[CmdletBinding()]
param(
    [string]$Addr = "127.0.0.1:6666",
    [int]$DurationMinutes = 30,
    [string]$ClientID = "stress-client",
    [int]$Offset = 0,
    [int]$Flush = 1000,
    [switch]$NoMeasure
)

$ErrorActionPreference = "Stop"

if ($DurationMinutes -lt 0 -or $Flush -lt 1) {
    throw "DurationMinutes must be >= 0 and Flush must be >= 1"
}

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$clientArgs = @(
    "./cmd/client",
    "-addr", $Addr,
    "-stress", $DurationMinutes,
    "-id", $ClientID,
    "-offset", $Offset,
    "-flush", $Flush
)

if (-not $NoMeasure) {
    $measuredDir = Join-Path $repoRoot "measured"
    New-Item -ItemType Directory -Path $measuredDir -Force | Out-Null

    $timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $csvPath = Join-Path $measuredDir "$ClientID-$timestamp.csv"

    $clientArgs += @("-measure", "-csv")

    Write-Host "Running measured stress client for $DurationMinutes minutes..."
    Write-Host "Addr=$Addr | ClientID=$ClientID | Flush=$Flush | Offset=$Offset"
    Write-Host "CSV output: $csvPath"

    & go run @clientArgs 1> $csvPath
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }

    Write-Host "Stress run completed. CSV saved to $csvPath"
    exit 0
}

Write-Host "Running stress client (measurement disabled) for $DurationMinutes minutes..."
Write-Host "Addr=$Addr | ClientID=$ClientID | Flush=$Flush | Offset=$Offset"

& go run @clientArgs
exit $LASTEXITCODE
