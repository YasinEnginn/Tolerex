[CmdletBinding()]
param(
    [string]$Addr = "127.0.0.1:6666",
    [int]$DurationMinutes = 30,
    [int]$Clients = 4,
    [int]$OffsetStep = 100000,
    [int]$Flush = 1000,
    [switch]$NoMeasure
)

$ErrorActionPreference = "Stop"

if ($Clients -lt 1) {
    throw "Clients must be >= 1"
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$runnerScript = Join-Path $repoRoot "scripts\run-stress-client.ps1"

Write-Host "Starting $Clients stress clients for $DurationMinutes minutes..."
Write-Host "Addr=$Addr | Flush=$Flush | OffsetStep=$OffsetStep | Measure=$(-not $NoMeasure)"

$jobs = @()
for ($i = 1; $i -le $Clients; $i++) {
    $clientId = "stress-client-$i"
    $offset = ($i - 1) * $OffsetStep

    $job = Start-Job -ScriptBlock {
        param($scriptPath, $addrArg, $durationArg, $clientIdArg, $offsetArg, $flushArg, $disableMeasure)
        $args = @(
            "-ExecutionPolicy", "Bypass",
            "-File", $scriptPath,
            "-Addr", $addrArg,
            "-DurationMinutes", $durationArg,
            "-ClientID", $clientIdArg,
            "-Offset", $offsetArg,
            "-Flush", $flushArg
        )
        if ($disableMeasure) {
            $args += "-NoMeasure"
        }
        & powershell @args
        exit $LASTEXITCODE
    } -ArgumentList $runnerScript, $Addr, $DurationMinutes, $clientId, $offset, $Flush, [bool]$NoMeasure

    $jobs += $job
}

$failed = $false
foreach ($job in $jobs) {
    $null = Wait-Job -Job $job
    Receive-Job -Job $job
    if ($job.State -ne "Completed") {
        $failed = $true
    }
}

Remove-Job -Job $jobs -Force

if ($failed) {
    throw "One or more stress client jobs failed."
}

Write-Host "All stress clients completed successfully."
