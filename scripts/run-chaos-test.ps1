[CmdletBinding()]
param(
    [ValidateRange(30, 3600)][int]$DurationSeconds = 180,
    [ValidateRange(1, 16)][int]$Clients = 6,
    [ValidateRange(1000, 500000)][int]$MaxMessages = 60000,
    [ValidateRange(1024, 60000)][int]$BasePort = 42555,
    [switch]$KeepArtifacts,
    [switch]$RequireImmediateDurability
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
$runID = "chaos-{0}-{1}" -f (Get-Date -Format "yyyyMMdd-HHmmss"), $PID
$runDir = Join-Path $repo ".tmp\$runID"
$clientWorkDir = Join-Path $runDir "client-work"
$stateDir = Join-Path $runDir "state"
$leaderBinary = Join-Path $runDir "leader.exe"
$memberBinary = Join-Path $runDir "member.exe"
$clientBinary = Join-Path $runDir "client.exe"
$leaderGRPCPort = $BasePort
$leaderTCPPort = $BasePort + 1
$leaderMetricsPort = $BasePort + 2
$memberPorts = 1..6 | ForEach-Object { $BasePort + 10 + $_ }
$memberMetricsPorts = 1..6 | ForEach-Object { $BasePort + 30 + $_ }
$allProcesses = [System.Collections.Generic.List[System.Diagnostics.Process]]::new()
$currentMembers = New-Object System.Collections.ArrayList
$currentClients = New-Object System.Collections.ArrayList
$leader = $null
$serial = 0
$nextCanaryID = 1900000000
$cycles = 0
$memberKills = 0
$leaderKills = 0
$canaryPasses = 0
$canaryFailures = 0
$immediateDurabilityLosses = 0
$maxObservedMessages = 0
$loadStoppedAtCap = $false
$audit = $null

function Assert-PortFree([int]$Port) {
    $listener = Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue
    if ($listener) {
        throw "port $Port is already in use"
    }
}

function Start-TestProcess([string]$File, [string[]]$Arguments, [string]$Name, [string]$WorkingDirectory = $repo) {
    $script:serial++
    $tag = "{0}-{1:D4}" -f $Name, $script:serial
    $params = @{
        FilePath = $File
        WorkingDirectory = $WorkingDirectory
        WindowStyle = "Hidden"
        PassThru = $true
        RedirectStandardOutput = (Join-Path $runDir "$tag.out.log")
        RedirectStandardError = (Join-Path $runDir "$tag.err.log")
    }
    if ($Arguments.Count -gt 0) {
        $params.ArgumentList = $Arguments
    }
    $process = Start-Process @params
    $allProcesses.Add($process)
    return $process
}

function Stop-TestProcess($Process) {
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

function Invoke-Tolerex([string[]]$Commands, [int]$TimeoutMilliseconds = 20000) {
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
        $responses = [System.Collections.Generic.List[string]]::new()
        foreach ($command in $Commands) {
            $writer.WriteLine($command)
            $responses.Add($reader.ReadLine())
        }
        return ,$responses.ToArray()
    }
    finally {
        $connection.Dispose()
    }
}

function Wait-Alive([int]$Expected, [int]$TimeoutSeconds = 30) {
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    while ([DateTime]::UtcNow -lt $deadline) {
        try {
            $status = (Invoke-Tolerex @("STATUS"))[0]
            if ($status -match "alive=$Expected( |$)") {
                return $status
            }
        }
        catch {}
        Start-Sleep -Milliseconds 100
    }
    throw "cluster did not reach alive=$Expected"
}

function Start-Leader {
    $env:BALANCER_STRATEGY = "p2c"
    $env:LEADER_GRPC_PORT = "$leaderGRPCPort"
    $env:LEADER_TCP_PORT = "$leaderTCPPort"
    $env:LEADER_TCP_BIND = "127.0.0.1"
    $env:LEADER_METRICS_PORT = "$leaderMetricsPort"
    $env:TOLEREX_TEST_MODE = "1"
    $env:TOLEREX_TEST_DIR = $stateDir
    $script:leader = Start-TestProcess $leaderBinary @() "leader"
    Wait-Port $leaderTCPPort
}

function Start-Member([int]$Index) {
    $port = $memberPorts[$Index]
    $env:LEADER_ADDR = "localhost:$leaderGRPCPort"
    $env:MEMBER_ADDR = "localhost:$port"
    $process = Start-TestProcess $memberBinary @("-port=$port", "-metrics=$($memberMetricsPorts[$Index])", "-io=unbuffered") "member-$Index"
    Wait-Port $port
    $currentMembers[$Index] = $process
}

function Start-LoadClients([DateTime]$Deadline) {
    foreach ($process in $currentClients) {
        Stop-TestProcess $process
    }
    $currentClients.Clear()
    $remaining = [Math]::Max(30, [int]($Deadline - [DateTime]::UtcNow).TotalSeconds + 30)
    for ($index = 0; $index -lt $Clients; $index++) {
        $arguments = @(
            "-addr", "127.0.0.1:$leaderTCPPort",
            "-id", "chaos-$index",
            "-offset", "$($index * 1000000)",
            "-flush", "25",
            "-duration", "${remaining}s"
        )
        $process = Start-TestProcess $clientBinary $arguments "client-$index" $clientWorkDir
        [void]$currentClients.Add($process)
    }
}

function Send-Canaries([int]$Count = 1) {
    for ($index = 0; $index -lt $Count; $index++) {
        $script:nextCanaryID++
        $value = "canary-$($script:nextCanaryID)-$([DateTime]::UtcNow.Ticks)"
        try {
            $responses = Invoke-Tolerex @("SET $($script:nextCanaryID) $value", "GET $($script:nextCanaryID)")
            if ($responses[0] -eq "OK" -and $responses[1] -eq "VALUE $value") {
                $script:canaryPasses++
            }
            else {
                $script:canaryFailures++
            }
        }
        catch {
            $script:canaryFailures++
        }
    }
}

function Restart-Members([int[]]$Indexes) {
    foreach ($index in $Indexes) {
        Start-Member $index
    }
    Wait-Alive 6 | Out-Null
}

function Kill-Members([int[]]$Indexes) {
    foreach ($index in $Indexes) {
        Stop-TestProcess $currentMembers[$index]
        $script:memberKills++
    }
}

function Read-MessageCount {
    try {
        $status = (Invoke-Tolerex @("STATUS"))[0]
        if ($status -match "messages=([0-9]+)") {
            return [int]$Matches[1]
        }
    }
    catch {}
    return 0
}

function Audit-State {
    Start-Sleep -Seconds 2
    $statePath = Join-Path $stateDir "leader_state.json"
    if (-not (Test-Path -LiteralPath $statePath)) {
        throw "leader state file is missing"
    }
    $state = Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json
    $missing = 0
    $wrongReplicaCount = 0
    $contentMismatch = 0
    $checked = 0
    foreach ($property in $state.MsgMap.PSObject.Properties) {
        $replicas = @($property.Value)
        if ($replicas.Count -ne 2) {
            $wrongReplicaCount++
        }
        $contents = [System.Collections.Generic.List[string]]::new()
        foreach ($address in $replicas) {
            $port = [int](($address -split ":")[-1])
            $path = Join-Path $repo "internal\data\member-$port\messages\$($property.Name).msg"
            if (-not (Test-Path -LiteralPath $path)) {
                $missing++
            }
            elseif ($checked -lt 1000) {
                $contents.Add([System.IO.File]::ReadAllText($path))
            }
        }
        if ($checked -lt 1000 -and $contents.Count -gt 1 -and $contents[0] -ne $contents[1]) {
            $contentMismatch++
        }
        $checked++
    }
    return [pscustomobject]@{
        Messages = $checked
        MissingReplicaFiles = $missing
        WrongReplicaCounts = $wrongReplicaCount
        SampledContentMismatches = $contentMismatch
    }
}

function Remove-SafeDirectory([string]$Path) {
    $fullPath = [System.IO.Path]::GetFullPath($Path)
    if (-not $fullPath.StartsWith($repo + "\", [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "refusing to remove path outside repository: $fullPath"
    }
    if ([System.IO.Directory]::Exists($fullPath)) {
        [System.IO.Directory]::Delete($fullPath, $true)
    }
}

try {
    foreach ($path in @("config/tls/ca.crt", "config/tls/leader.crt", "config/tls/leader.key", "config/tls/member.crt", "config/tls/member.key")) {
        if (-not (Test-Path -LiteralPath (Join-Path $repo $path))) {
            throw "missing $path; run: go run ./cmd/certgen"
        }
    }
    foreach ($port in @($leaderGRPCPort, $leaderTCPPort, $leaderMetricsPort) + $memberPorts + $memberMetricsPorts) {
        Assert-PortFree $port
    }
    foreach ($port in $memberPorts) {
        $dataDir = Join-Path $repo "internal\data\member-$port"
        if (Test-Path -LiteralPath $dataDir) {
            throw "test data directory already exists: $dataDir"
        }
    }

    New-Item -ItemType Directory -Path $runDir, $clientWorkDir, $stateDir -Force | Out-Null
    Push-Location $repo
    try {
        & go build -trimpath -o $leaderBinary ./cmd/leader
        if ($LASTEXITCODE -ne 0) { throw "leader build failed" }
        & go build -trimpath -o $memberBinary ./cmd/member
        if ($LASTEXITCODE -ne 0) { throw "member build failed" }
        & go build -trimpath -o $clientBinary ./cmd/client
        if ($LASTEXITCODE -ne 0) { throw "client build failed" }
    }
    finally {
        Pop-Location
    }

    $env:TOLEREX_LOG_FILE = "off"
    $env:TOLEREX_LOG_LEVEL = "warn"
    Start-Leader
    for ($index = 0; $index -lt 6; $index++) {
        [void]$currentMembers.Add($null)
        Start-Member $index
    }
    Wait-Alive 6 | Out-Null

    $deadline = [DateTime]::UtcNow.AddSeconds($DurationSeconds)
    Start-LoadClients $deadline
    while ([DateTime]::UtcNow -lt $deadline) {
        switch ($cycles % 5) {
            0 {
                $index = $cycles % 6
                Kill-Members @($index)
                Send-Canaries 2
                Restart-Members @($index)
            }
            1 {
                $indexes = @(
                    (($cycles + 1) % 6)
                    (($cycles + 3) % 6)
                )
                Kill-Members $indexes
                Send-Canaries 3
                Restart-Members $indexes
            }
            2 {
                $nextCanaryID++
                $value = "durability-$nextCanaryID-$([DateTime]::UtcNow.Ticks)"
                $set = (Invoke-Tolerex @("SET $nextCanaryID $value"))[0]
                Stop-TestProcess $leader
                $leaderKills++
                Start-Sleep -Milliseconds 750
                Start-Leader
                Start-LoadClients $deadline
                try {
                    $get = (Invoke-Tolerex @("GET $nextCanaryID"))[0]
                    if ($set -ne "OK" -or $get -ne "VALUE $value") {
                        $immediateDurabilityLosses++
                    }
                }
                catch {
                    $immediateDurabilityLosses++
                }
            }
            3 {
                $index = ($cycles + 2) % 6
                for ($flap = 0; $flap -lt 3; $flap++) {
                    Kill-Members @($index)
                    Start-Sleep -Milliseconds 150
                    Restart-Members @($index)
                }
                Send-Canaries 2
            }
            4 {
                $indexes = @(0, 1, 2, 3)
                Kill-Members $indexes
                Send-Canaries 5
                Restart-Members $indexes
            }
        }
        $cycles++
        $messages = Read-MessageCount
        if ($messages -gt $maxObservedMessages) {
            $maxObservedMessages = $messages
        }
        if (-not $loadStoppedAtCap -and $messages -ge $MaxMessages) {
            foreach ($process in $currentClients) { Stop-TestProcess $process }
            $currentClients.Clear()
            $loadStoppedAtCap = $true
        }
        Write-Host ("cycle={0} messages={1} member_kills={2} leader_kills={3} canary_failures={4}" -f `
            $cycles, $messages, $memberKills, $leaderKills, $canaryFailures)
        Start-Sleep -Seconds 2
    }

    foreach ($process in $currentClients) { Stop-TestProcess $process }
    $currentClients.Clear()
    Wait-Alive 6 | Out-Null
    Send-Canaries 10
    $audit = Audit-State

    $summary = [pscustomobject]@{
        DurationSeconds = $DurationSeconds
        Cycles = $cycles
        MemberKills = $memberKills
        LeaderKills = $leaderKills
        CanaryPasses = $canaryPasses
        CanaryFailures = $canaryFailures
        ImmediateDurabilityLosses = $immediateDurabilityLosses
        MaxObservedMessages = $maxObservedMessages
        LoadStoppedAtSafetyCap = $loadStoppedAtCap
        AuditedMessages = $audit.Messages
        MissingReplicaFiles = $audit.MissingReplicaFiles
        WrongReplicaCounts = $audit.WrongReplicaCounts
        SampledContentMismatches = $audit.SampledContentMismatches
    }
    $summary | Format-List

    if ($canaryFailures -gt 0 -or $audit.MissingReplicaFiles -gt 0 -or $audit.WrongReplicaCounts -gt 0 -or $audit.SampledContentMismatches -gt 0) {
        throw "chaos test found availability or replica-integrity failures"
    }
    if ($RequireImmediateDurability -and $immediateDurabilityLosses -gt 0) {
        throw "chaos test found acknowledged writes lost by immediate leader termination"
    }
}
finally {
    foreach ($process in $allProcesses) {
        Stop-TestProcess $process
    }
    if (-not $KeepArtifacts) {
        foreach ($port in $memberPorts) {
            Remove-SafeDirectory (Join-Path $repo "internal\data\member-$port")
        }
        Remove-SafeDirectory $runDir
        $dataRoot = Join-Path $repo "internal\data"
        if ((Test-Path -LiteralPath $dataRoot) -and @(Get-ChildItem -LiteralPath $dataRoot -Force).Count -eq 0) {
            [System.IO.Directory]::Delete($dataRoot)
        }
        $tempRoot = Join-Path $repo ".tmp"
        if ((Test-Path -LiteralPath $tempRoot) -and @(Get-ChildItem -LiteralPath $tempRoot -Force).Count -eq 0) {
            [System.IO.Directory]::Delete($tempRoot)
        }
    }
}
