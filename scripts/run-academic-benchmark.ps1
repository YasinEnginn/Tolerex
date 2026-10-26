[CmdletBinding()]
param(
    [string]$OutputRoot = "E:\Tolerex-Benchmarks",
    [string]$ResumeDirectory = "",
    [ValidatePattern("^[a-z0-9-]+$")][string]$Profile = "read-heavy-80-20",
    [ValidateRange(0, 100)][int]$ReadRatio = 80,
    [ValidateRange(32, 4194304)][int]$PayloadBytes = 1024,
    [ValidateRange(1, 100000)][int]$WorkingSetPerClient = 1024,
    [ValidateRange(0, 3600)][int]$WarmupSeconds = 120,
    [ValidateRange(1, 7200)][int]$MeasurementSeconds = 600,
    [ValidateRange(1, 20)][int]$Repeats = 5,
    [int[]]$ClientCounts = @(1, 2, 4, 8, 16),
    [string[]]$Strategies = @("least_loaded", "p2c"),
    [string[]]$IOModes = @("buffered", "unbuffered"),
    [ValidateRange(0, 600)][int]$CooldownSeconds = 30,
    [ValidateRange(1024, 60000)][int]$BasePort = 45555,
    [int]$Seed = 20261025,
    [switch]$KeepRuntime
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
$outputRootFull = [System.IO.Path]::GetFullPath($OutputRoot)
$leaderGRPCPort = $BasePort
$leaderTCPPort = $BasePort + 1
$leaderMetricsPort = $BasePort + 2
$memberPorts = 1..6 | ForEach-Object { $BasePort + 10 + $_ }
$memberMetricsPorts = 1..6 | ForEach-Object { $BasePort + 30 + $_ }
$allProcesses = [System.Collections.Generic.List[System.Diagnostics.Process]]::new()
$serial = 0
$runFailed = $false

Add-Type -TypeDefinition @"
using System;
using System.Runtime.InteropServices;
public static class TolerexExecutionState {
    [DllImport("kernel32.dll")]
    public static extern uint SetThreadExecutionState(uint flags);
}
"@

function Write-JsonFile([string]$Path, $Value, [int]$Depth = 6) {
    $json = $Value | ConvertTo-Json -Depth $Depth
    [System.IO.File]::WriteAllText($Path, $json, [System.Text.UTF8Encoding]::new($false))
}

function Assert-PortFree([int]$Port) {
    if (Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue) {
        throw "port $Port is already in use"
    }
}

function Wait-Port([int]$Port, [int]$TimeoutSeconds = 30) {
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    while ([DateTime]::UtcNow -lt $deadline) {
        try {
            $connection = [System.Net.Sockets.TcpClient]::new()
            $connection.Connect("127.0.0.1", $Port)
            $connection.Dispose()
            return
        }
        catch {
            Start-Sleep -Milliseconds 100
        }
    }
    throw "port $Port did not become ready"
}

function Start-CapturedProcess(
    [string]$File,
    [string[]]$Arguments,
    [string]$Name,
    [string]$WorkingDirectory,
    [string]$LogDirectory,
    [string]$StandardOutput = ""
) {
    $script:serial++
    $tag = "{0}-{1:D4}" -f $Name, $script:serial
    $stdoutPath = if ($StandardOutput) { $StandardOutput } else { Join-Path $LogDirectory "$tag.out.log" }
    $params = @{
        FilePath = $File
        WorkingDirectory = $WorkingDirectory
        WindowStyle = "Hidden"
        PassThru = $true
        RedirectStandardOutput = $stdoutPath
        RedirectStandardError = (Join-Path $LogDirectory "$tag.err.log")
    }
    if ($Arguments.Count -gt 0) {
        $params.ArgumentList = $Arguments
    }
    $process = Start-Process @params
    # Windows PowerShell can lose ExitCode unless the native process handle is
    # materialized while the process is still running.
    $null = $process.Handle
    [void]$allProcesses.Add($process)
    return $process
}

function Stop-CapturedProcess($Process) {
    if ($null -eq $Process) {
        return
    }
    try {
        if (-not $Process.HasExited) {
            Stop-Process -Id $Process.Id -Force -ErrorAction SilentlyContinue
        }
    }
    catch {}
    try {
        $Process.WaitForExit(5000) | Out-Null
    }
    catch {}
}

function Invoke-Tolerex([string]$Command, [int]$TimeoutMilliseconds = 10000) {
    $connection = [System.Net.Sockets.TcpClient]::new()
    $connection.ReceiveTimeout = $TimeoutMilliseconds
    $connection.SendTimeout = $TimeoutMilliseconds
    $connection.Connect("127.0.0.1", $leaderTCPPort)
    try {
        $stream = $connection.GetStream()
        $encoding = [System.Text.UTF8Encoding]::new($false)
        $reader = [System.IO.StreamReader]::new($stream, $encoding, $false, 65536, $true)
        $writer = [System.IO.StreamWriter]::new($stream, $encoding, 65536, $true)
        $writer.NewLine = "`r`n"
        $writer.AutoFlush = $true
        if ($reader.ReadLine() -ne "Tolerex ready") {
            throw "invalid leader greeting"
        }
        $writer.WriteLine($Command)
        return $reader.ReadLine()
    }
    finally {
        $connection.Dispose()
    }
}

function Wait-Alive([int]$Expected, [int]$TimeoutSeconds = 30) {
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    while ([DateTime]::UtcNow -lt $deadline) {
        try {
            $status = Invoke-Tolerex "STATUS"
            if ($status -match "alive=$Expected( |$)") {
                return $status
            }
        }
        catch {}
        Start-Sleep -Milliseconds 100
    }
    throw "cluster did not reach alive=$Expected"
}

function Remove-SafeRuntime([string]$Path, [string]$RunRoot) {
    $fullPath = [System.IO.Path]::GetFullPath($Path)
    $fullRoot = [System.IO.Path]::GetFullPath($RunRoot).TrimEnd("\") + "\"
    if (-not $fullPath.StartsWith($fullRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "refusing to remove runtime outside run directory: $fullPath"
    }
    if ([System.IO.Directory]::Exists($fullPath)) {
        [System.IO.Directory]::Delete($fullPath, $true)
    }
}

function New-BenchmarkPlan {
    $rows = [System.Collections.Generic.List[object]]::new()
    $order = 0
    for ($repeat = 1; $repeat -le $Repeats; $repeat++) {
        $block = [System.Collections.Generic.List[object]]::new()
        foreach ($strategy in $Strategies) {
            foreach ($ioMode in $IOModes) {
                foreach ($clients in $ClientCounts) {
                    [void]$block.Add([pscustomobject]@{
                        Strategy = $strategy
                        IOMode = $ioMode
                        Clients = $clients
                        Repeat = $repeat
                    })
                }
            }
        }
        $random = [System.Random]::new($Seed + $repeat)
        for ($index = $block.Count - 1; $index -gt 0; $index--) {
            $swap = $random.Next($index + 1)
            $temp = $block[$index]
            $block[$index] = $block[$swap]
            $block[$swap] = $temp
        }
        foreach ($item in $block) {
            $order++
            [void]$rows.Add([pscustomobject]@{
                Order = $order
                Strategy = $item.Strategy
                IOMode = $item.IOMode
                Clients = $item.Clients
                Repeat = $item.Repeat
                Seed = $Seed + $order
            })
        }
    }
    return $rows
}

function Update-Summary([string]$RunDirectory, $Plan) {
    $summary = [System.Collections.Generic.List[object]]::new()
    foreach ($item in $Plan) {
        $rawPath = Join-Path $RunDirectory ("results\run-{0:D4}.json" -f [int]$item.Order)
        if (-not (Test-Path -LiteralPath $rawPath)) {
            continue
        }
        $raw = Get-Content -LiteralPath $rawPath -Raw -Encoding UTF8 | ConvertFrom-Json
        [void]$summary.Add([pscustomobject]@{
            RunOrder = [int]$item.Order
            Profile = $raw.profile
            Strategy = $item.Strategy
            IOMode = $item.IOMode
            Clients = [int]$item.Clients
            Repeat = [int]$item.Repeat
            PayloadBytes = [int]$raw.payload_bytes
            ReadRatioTargetPercent = [int]$raw.read_ratio_target_percent
            ReadRatioActualPercent = [double]$raw.read_ratio_actual_percent
            Distribution = $raw.distribution
            ZipfS = [double]$raw.zipf_s
            WarmupSeconds = [double]$raw.warmup_seconds
            MeasuredSeconds = [double]$raw.measured_seconds
            Reads = [int64]$raw.reads
            Writes = [int64]$raw.writes
            SuccessfulOperations = [int64]$raw.successful_operations
            FailedOperations = [int64]$raw.failed_operations
            ThroughputOPS = [double]$raw.throughput_ops_per_second
            ErrorRatePercent = [double]$raw.error_rate_percent
            LatencyP50MS = [double]$raw.latency.p50_ms
            LatencyP95MS = [double]$raw.latency.p95_ms
            LatencyP99MS = [double]$raw.latency.p99_ms
            ReadP95MS = [double]$raw.read_latency.p95_ms
            WriteP95MS = [double]$raw.write_latency.p95_ms
            TimestampUTC = $raw.timestamp_utc
        })
    }
    $summaryPath = Join-Path $RunDirectory "results.csv"
    $summary | Export-Csv -LiteralPath $summaryPath -NoTypeInformation -Encoding UTF8
    return $summary.Count
}

foreach ($strategy in $Strategies) {
    if ($strategy -notin @("least_loaded", "p2c")) {
        throw "unsupported strategy: $strategy"
    }
}
foreach ($ioMode in $IOModes) {
    if ($ioMode -notin @("buffered", "unbuffered")) {
        throw "unsupported I/O mode: $ioMode"
    }
}
foreach ($clients in $ClientCounts) {
    if ($clients -lt 1 -or $clients -gt 128) {
        throw "client counts must be between 1 and 128"
    }
}
if ($Strategies.Count -eq 0 -or $IOModes.Count -eq 0 -or $ClientCounts.Count -eq 0) {
    throw "strategy, I/O mode, and client-count lists must not be empty"
}
if (-not [System.IO.Path]::IsPathRooted($outputRootFull)) {
    throw "OutputRoot must be an absolute path"
}
$driveRoot = [System.IO.Path]::GetPathRoot($outputRootFull)
$driveName = $driveRoot.TrimEnd("\").TrimEnd(":")
$drive = Get-PSDrive -Name $driveName -ErrorAction Stop
if ($drive.Free -lt 20GB) {
    throw "at least 20 GiB free space is required on $driveRoot"
}

$requiredFiles = @(
    "config/tls/ca.crt",
    "config/tls/leader.crt",
    "config/tls/leader.key",
    "config/tls/member.crt",
    "config/tls/member.key",
    "config/tolerance.conf"
)
foreach ($relativePath in $requiredFiles) {
    if (-not (Test-Path -LiteralPath (Join-Path $repo $relativePath))) {
        throw "missing $relativePath"
    }
}
foreach ($port in @($leaderGRPCPort, $leaderTCPPort, $leaderMetricsPort) + $memberPorts + $memberMetricsPorts) {
    Assert-PortFree $port
}

if ($ResumeDirectory) {
    $runDir = [System.IO.Path]::GetFullPath($ResumeDirectory)
    $rootPrefix = $outputRootFull.TrimEnd("\") + "\"
    if (-not $runDir.StartsWith($rootPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "ResumeDirectory must be inside OutputRoot"
    }
    $planPath = Join-Path $runDir "plan.csv"
    if (-not (Test-Path -LiteralPath $planPath)) {
        throw "resume plan not found: $planPath"
    }
    $plan = @(Import-Csv -LiteralPath $planPath)
}
else {
    $runName = "academic-{0}-{1}" -f $Profile, (Get-Date -Format "yyyyMMdd-HHmmss")
    $runDir = Join-Path $outputRootFull $runName
    if (Test-Path -LiteralPath $runDir) {
        throw "run directory already exists: $runDir"
    }
    New-Item -ItemType Directory -Path $runDir -Force | Out-Null
    $plan = @(New-BenchmarkPlan)
    $plan | Export-Csv -LiteralPath (Join-Path $runDir "plan.csv") -NoTypeInformation -Encoding UTF8
}

$binDir = Join-Path $runDir "bin"
$logsDir = Join-Path $runDir "logs"
$resultsDir = Join-Path $runDir "results"
$runtimeRoot = Join-Path $runDir "runtime"
New-Item -ItemType Directory -Path $binDir, $logsDir, $resultsDir, $runtimeRoot -Force | Out-Null

$leaderBinary = Join-Path $binDir "leader.exe"
$memberBinary = Join-Path $binDir "member.exe"
$benchBinary = Join-Path $binDir "benchclient.exe"

Push-Location $repo
try {
    & go build -trimpath -o $leaderBinary ./cmd/leader
    if ($LASTEXITCODE -ne 0) { throw "leader build failed" }
    & go build -trimpath -o $memberBinary ./cmd/member
    if ($LASTEXITCODE -ne 0) { throw "member build failed" }
    & go build -trimpath -o $benchBinary ./cmd/benchclient
    if ($LASTEXITCODE -ne 0) { throw "benchmark client build failed" }
}
finally {
    Pop-Location
}

$cpu = Get-CimInstance Win32_Processor | Select-Object -First 1 Name, NumberOfCores, NumberOfLogicalProcessors, MaxClockSpeed
$computer = Get-CimInstance Win32_ComputerSystem | Select-Object Manufacturer, Model, TotalPhysicalMemory
$operatingSystem = Get-CimInstance Win32_OperatingSystem | Select-Object Caption, Version, BuildNumber
$volume = Get-Volume -DriveLetter $driveName | Select-Object DriveLetter, FileSystemLabel, FileSystem, Size, SizeRemaining
$battery = Get-CimInstance Win32_Battery -ErrorAction SilentlyContinue | Select-Object -First 1 BatteryStatus, EstimatedChargeRemaining
$powerScheme = (& powercfg /getactivescheme 2>&1) -join " "
$gitCommit = (& git -C $repo rev-parse HEAD).Trim()
$goVersion = (& go version) -join " "
$metadata = [pscustomobject]@{
    SchemaVersion = 1
    CreatedAt = (Get-Date).ToString("o")
    Repository = $repo
    GitCommit = $gitCommit
    GoVersion = $goVersion
    CPU = $cpu
    Computer = $computer
    OperatingSystem = $operatingSystem
    Volume = $volume
    Battery = $battery
    PowerScheme = $powerScheme
    Parameters = [pscustomobject]@{
        Profile = $Profile
        ReadRatio = $ReadRatio
        PayloadBytes = $PayloadBytes
        WorkingSetPerClient = $WorkingSetPerClient
        WarmupSeconds = $WarmupSeconds
        MeasurementSeconds = $MeasurementSeconds
        Repeats = $Repeats
        ClientCounts = $ClientCounts
        Strategies = $Strategies
        IOModes = $IOModes
        CooldownSeconds = $CooldownSeconds
        Seed = $Seed
        Members = 6
        ReplicationFactor = 2
        Distribution = "zipf"
        ZipfS = 1.1
    }
}
Write-JsonFile (Join-Path $runDir "metadata.json") $metadata 8

if ($battery -and [int]$battery.BatteryStatus -eq 1) {
    throw "academic benchmark requires AC power; the computer is currently discharging"
}

$executionState = [TolerexExecutionState]::SetThreadExecutionState([uint32]2147483651)
Write-Host "Academic benchmark directory: $runDir"
Write-Host "Runs: $($plan.Count) | warm-up: ${WarmupSeconds}s | measurement: ${MeasurementSeconds}s | cooldown: ${CooldownSeconds}s"

try {
    foreach ($item in $plan) {
        $order = [int]$item.Order
        $rawPath = Join-Path $resultsDir ("run-{0:D4}.json" -f $order)
        $pendingPath = Join-Path $resultsDir ("run-{0:D4}.pending.json" -f $order)
        if (Test-Path -LiteralPath $rawPath) {
            Write-Host ("skip completed run {0}/{1}" -f $order, $plan.Count)
            continue
        }
        # Only validated results receive the final .json name. A crashed or
        # failed client may still leave partial stdout behind; keeping that in
        # a .pending file ensures a resumed benchmark reruns the case.
        if ([System.IO.File]::Exists($pendingPath)) {
            [System.IO.File]::Delete($pendingPath)
        }

        $strategy = [string]$item.Strategy
        $ioMode = [string]$item.IOMode
        $clients = [int]$item.Clients
        $repeat = [int]$item.Repeat
        $runSeed = [int]$item.Seed
        $runTag = "run-{0:D4}" -f $order
        $runtimeDir = Join-Path $runtimeRoot $runTag
        $runLogDir = Join-Path $logsDir $runTag
        New-Item -ItemType Directory -Path $runtimeDir, $runLogDir -Force | Out-Null

        $status = [pscustomobject]@{
            State = "running"
            RunDirectory = $runDir
            Current = $order
            Total = $plan.Count
            Strategy = $strategy
            IOMode = $ioMode
            Clients = $clients
            Repeat = $repeat
            StartedAt = (Get-Date).ToString("o")
        }
        Write-JsonFile (Join-Path $runDir "status.json") $status
        Write-Host ("[{0}/{1}] strategy={2} io={3} clients={4} repeat={5}" -f $order, $plan.Count, $strategy, $ioMode, $clients, $repeat)

        $leader = $null
        $members = [System.Collections.Generic.List[System.Diagnostics.Process]]::new()
        $benchmark = $null
        $currentRunFailed = $false
        try {
            $env:BALANCER_STRATEGY = $strategy
            $env:LEADER_GRPC_PORT = "$leaderGRPCPort"
            $env:LEADER_GRPC_BIND = "127.0.0.1"
            $env:LEADER_TCP_PORT = "$leaderTCPPort"
            $env:LEADER_TCP_BIND = "127.0.0.1"
            $env:LEADER_METRICS_PORT = "$leaderMetricsPort"
            $env:LEADER_METRICS_BIND = "127.0.0.1"
            $env:LEADER_CERT_FILE = Join-Path $repo "config\tls\leader.crt"
            $env:LEADER_KEY_FILE = Join-Path $repo "config\tls\leader.key"
            $env:CA_CERT_FILE = Join-Path $repo "config\tls\ca.crt"
            $env:TOLERANCE_CONF = Join-Path $repo "config\tolerance.conf"
            $env:TOLEREX_LOG_FILE = "off"
            $env:TOLEREX_LOG_LEVEL = "warn"
            $leader = Start-CapturedProcess $leaderBinary @() "leader" $runtimeDir $runLogDir
            Wait-Port $leaderTCPPort

            for ($index = 0; $index -lt 6; $index++) {
                $port = $memberPorts[$index]
                $env:LEADER_ADDR = "localhost:$leaderGRPCPort"
                $env:MEMBER_ADDR = "localhost:$port"
                $env:MEMBER_GRPC_BIND = "127.0.0.1"
                $env:MEMBER_METRICS_BIND = "127.0.0.1"
                $env:MEMBER_CERT_FILE = Join-Path $repo "config\tls\member.crt"
                $env:MEMBER_KEY_FILE = Join-Path $repo "config\tls\member.key"
                $member = Start-CapturedProcess $memberBinary @(
                    "-port=$port",
                    "-metrics=$($memberMetricsPorts[$index])",
                    "-io=$ioMode"
                ) "member-$index" $runtimeDir $runLogDir
                [void]$members.Add($member)
                Wait-Port $port
            }
            $clusterStatus = Wait-Alive 6
            Write-Host "  cluster: $clusterStatus"

            $arguments = @(
                "-addr=127.0.0.1:$leaderTCPPort",
                "-clients=$clients",
                "-read-ratio=$ReadRatio",
                "-payload-bytes=$PayloadBytes",
                "-working-set=$WorkingSetPerClient",
                "-warmup=${WarmupSeconds}s",
                "-duration=${MeasurementSeconds}s",
                "-distribution=zipf",
                "-zipf-s=1.1",
                "-seed=$runSeed",
                "-profile=$Profile"
            )
            $benchmark = Start-CapturedProcess $benchBinary $arguments "bench" $runtimeDir $runLogDir $pendingPath
            $benchmark.WaitForExit()
            $benchmark.Refresh()
            if ($benchmark.ExitCode -ne 0) {
                throw "benchmark client failed with exit code $($benchmark.ExitCode)"
            }
            $result = Get-Content -LiteralPath $pendingPath -Raw -Encoding UTF8 | ConvertFrom-Json
            $hasFatalErrors = $null -ne $result.fatal_errors -and @($result.fatal_errors).Count -ne 0
            if ([int64]$result.failed_operations -ne 0 -or $hasFatalErrors) {
                throw "benchmark reported failed operations"
            }
            if ([Math]::Abs([double]$result.read_ratio_actual_percent - $ReadRatio) -gt 1.0) {
                throw "actual read ratio differs from the target by more than one percentage point"
            }
            [System.IO.File]::Move($pendingPath, $rawPath)
            $completed = Update-Summary $runDir $plan
            Write-Host ("  throughput={0:N2} ops/s p95={1:N3} ms completed={2}/{3}" -f `
                [double]$result.throughput_ops_per_second, [double]$result.latency.p95_ms, $completed, $plan.Count)
        }
        catch {
            $currentRunFailed = $true
            $runFailed = $true
            $failedStatus = [pscustomobject]@{
                State = "failed"
                RunDirectory = $runDir
                Current = $order
                Total = $plan.Count
                Strategy = $strategy
                IOMode = $ioMode
                Clients = $clients
                Repeat = $repeat
                FailedAt = (Get-Date).ToString("o")
                Error = $_.Exception.Message
            }
            Write-JsonFile (Join-Path $runDir "status.json") $failedStatus
            throw
        }
        finally {
            Stop-CapturedProcess $benchmark
            foreach ($member in $members) {
                Stop-CapturedProcess $member
            }
            Stop-CapturedProcess $leader
            if (-not $KeepRuntime -and -not $currentRunFailed) {
                Remove-SafeRuntime $runtimeDir $runDir
            }
        }

        if ($CooldownSeconds -gt 0 -and $order -lt $plan.Count) {
            Start-Sleep -Seconds $CooldownSeconds
        }
    }

    $completed = Update-Summary $runDir $plan
    $completeStatus = [pscustomobject]@{
        State = "complete"
        RunDirectory = $runDir
        Completed = $completed
        Total = $plan.Count
        FinishedAt = (Get-Date).ToString("o")
    }
    Write-JsonFile (Join-Path $runDir "status.json") $completeStatus
    Write-Host "Academic benchmark complete: $completed/$($plan.Count) runs"
}
finally {
    foreach ($process in $allProcesses) {
        Stop-CapturedProcess $process
    }
    [void][TolerexExecutionState]::SetThreadExecutionState([uint32]2147483648)
    if (-not $runFailed -and -not $KeepRuntime -and (Test-Path -LiteralPath $runtimeRoot) -and @(Get-ChildItem -LiteralPath $runtimeRoot -Force).Count -eq 0) {
        [System.IO.Directory]::Delete($runtimeRoot)
    }
}
