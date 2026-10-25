<#
.SYNOPSIS
    Cleans Docker artifacts created by this repository only.

.DESCRIPTION
    Removes Tolerex-related Docker resources:
    - compose stacks (monitoring + cluster)
    - project containers
    - project volumes
    - project networks
    - project images built by Compose

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

Write-Host "clean-docker-artifacts complete"
