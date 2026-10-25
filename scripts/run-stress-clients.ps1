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

if ($Clients -lt 1 -or $DurationMinutes -lt 0 -or $OffsetStep -lt 1 -or $Flush -lt 1) {
    throw "Clients and OffsetStep and Flush must be >= 1; DurationMinutes must be >= 0"
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
        $processArgs = @(
            "-ExecutionPolicy", "Bypass",
            "-File", $scriptPath,
            "-Addr", $addrArg,
            "-DurationMinutes", $durationArg,
            "-ClientID", $clientIdArg,
            "-Offset", $offsetArg,
            "-Flush", $flushArg
        )
        if ($disableMeasure) {
            $processArgs += "-NoMeasure"
        }
        & powershell @processArgs
        if ($LASTEXITCODE -ne 0) {
            throw "stress client $clientIdArg failed with exit code $LASTEXITCODE"
        }
    } -ArgumentList $runnerScript, $Addr, $DurationMinutes, $clientId, $offset, $Flush, [bool]$NoMeasure

    $jobs += $job
}

$failed = $false
foreach ($job in $jobs) {
    $null = Wait-Job -Job $job
    Receive-Job -Job $job -ErrorAction Continue
    if ($job.State -ne "Completed") {
        $failed = $true
    }
}

Remove-Job -Job $jobs -Force

if ($failed) {
    throw "One or more stress client jobs failed."
}

Write-Host "All stress clients completed successfully."
