<#
.SYNOPSIS
    Cleans Docker artifacts created by this repository only.

.DESCRIPTION
    Removes Tolerex-related Docker resources:
    - compose stacks (monitoring + cluster)
    - project containers
    - project volumes
    - project networks
    - project images
    - build cache

    This script never runs a global system prune.

.PARAMETER DryRun
    If set, prints commands without executing them.
#>
param(
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

function Invoke-External {
    param(
        [Parameter(Mandatory = $true)]
        [string]$FilePath,

        [Parameter(Mandatory = $true)]
        [string[]]$Arguments,

        [switch]$IgnoreFailure
    )

    $pretty = "$FilePath $($Arguments -join ' ')"
    if ($DryRun) {
        Write-Host "[DRY-RUN] $pretty"
        return
    }

    & $FilePath @Arguments
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
        if ($IgnoreFailure) {
            Write-Warning "command failed (ignored): $pretty (exit=$exitCode)"
            return
        }
        throw "command failed: $pretty (exit=$exitCode)"
    }
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Warning "docker command not found; skipping docker cleanup"
    exit 0
}

$composeFiles = @(
    "deploy/docker-compose.yml",
    "deploy/docker-compose.cluster.yml"
)

foreach ($composeFile in $composeFiles) {
    if (Test-Path -LiteralPath $composeFile) {
        Invoke-External -FilePath "docker" -Arguments @(
            "compose", "-f", $composeFile,
            "down", "--volumes", "--remove-orphans", "--rmi", "local"
        ) -IgnoreFailure
    }
}

# Leftover containers from this repo's compose project.
$containerIds = @(
    docker ps -a --filter "name=deploy-" --format "{{.ID}}" 2>$null |
        Where-Object { $_ -and $_.Trim() -ne "" }
)
if ($containerIds.Count -gt 0) {
    Invoke-External -FilePath "docker" -Arguments (@("rm", "-f") + $containerIds) -IgnoreFailure
}

$volumeNames = @(
    docker volume ls --format "{{.Name}}" 2>$null |
        Where-Object { $_ -match "^deploy_" -or $_ -match "tolerex" }
)
if ($volumeNames.Count -gt 0) {
    Invoke-External -FilePath "docker" -Arguments (@("volume", "rm", "-f") + $volumeNames) -IgnoreFailure
}

$networkNames = @(
    docker network ls --format "{{.Name}}" 2>$null |
        Where-Object { $_ -match "^deploy_" -or $_ -match "tolerex" }
)
if ($networkNames.Count -gt 0) {
    Invoke-External -FilePath "docker" -Arguments (@("network", "rm") + $networkNames) -IgnoreFailure
}

$imageRefs = @(
    docker images --format "{{.Repository}}:{{.Tag}}" 2>$null |
        Where-Object { $_ -like "tolerex-node:*" -or $_ -like "tolerex/*" } |
        Sort-Object -Unique
)
if ($imageRefs.Count -gt 0) {
    Invoke-External -FilePath "docker" -Arguments (@("rmi", "-f") + $imageRefs) -IgnoreFailure
}

Invoke-External -FilePath "docker" -Arguments @("builder", "prune", "-f") -IgnoreFailure

Write-Host "clean-docker-artifacts complete"
