param(
    [string]$Image = "execraft-runtime:local",
    [int]$Port = 18080,
    [switch]$SkipBuild,
    [switch]$Detach
)

$ErrorActionPreference = "Stop"

function Require-Command {
    param([Parameter(Mandatory = $true)][string]$Name)
    if ($null -eq (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' is not available. Please install it first."
    }
}

function Resolve-Port {
    param([int]$RequestedPort)

    if ($RequestedPort -ne 0) {
        $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Any, $RequestedPort)
        try {
            $listener.Start()
            return $RequestedPort
        } catch {
            Write-Warning "[dev] Port $RequestedPort is unavailable, selecting a free port automatically."
        } finally {
            try { $listener.Stop() } catch {}
        }
    }

    $ephemeral = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Any, 0)
    try {
        $ephemeral.Start()
        return ([int]($ephemeral.LocalEndpoint.Port))
    } finally {
        $ephemeral.Stop()
    }
}

function Wait-Ready {
    param(
        [Parameter(Mandatory = $true)][string]$BaseUrl,
        [int]$MaxAttempts = 120,
        [int]$DelayMs = 250
    )
    for ($i = 1; $i -le $MaxAttempts; $i++) {
        try {
            $resp = Invoke-RestMethod -Uri "$BaseUrl/readyz" -Method Get -TimeoutSec 2
            if ($resp.status -eq "ready") {
                return $true
            }
        } catch {
            Start-Sleep -Milliseconds $DelayMs
        }
    }
    return $false
}

Require-Command -Name "docker"

$projectRoot = Split-Path -Parent $PSScriptRoot
$Port = Resolve-Port -RequestedPort $Port
$baseUrl = "http://127.0.0.1:$Port"
$containerName = "execraft-runtime-dev-$PID"

if (-not $SkipBuild) {
    Write-Host "[dev] Building image: $Image"
    docker build -t $Image $projectRoot | Out-Host
}

Write-Host "[dev] Starting runtime container on port $Port"
$runSucceeded = $false
for ($attempt = 1; $attempt -le 3; $attempt++) {
    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $runOutput = docker run --rm -d `
        --name $containerName `
        -p "${Port}:8080" `
        -e RUST_LOG=info `
        $Image 2>&1
    $ErrorActionPreference = $previousErrorActionPreference

    if ($LASTEXITCODE -eq 0) {
        $runSucceeded = $true
        break
    }

    $outputText = ($runOutput -join [Environment]::NewLine)
    if ($outputText -match "port is already allocated" -and $attempt -lt 3) {
        $Port = Resolve-Port -RequestedPort 0
        $baseUrl = "http://127.0.0.1:$Port"
        Write-Warning "[dev] Port conflict detected. Retrying with port $Port."
        continue
    }

    throw "Failed to start container:`n$outputText"
}

if (-not $runSucceeded) {
    throw "Failed to start container after retries."
}
$containerId = ($runOutput | Select-Object -First 1).Trim()
Write-Host "[dev] Container ID: $containerId"

if (-not (Wait-Ready -BaseUrl $baseUrl)) {
    docker stop $containerName 2>$null | Out-Null
    throw "Runtime did not become ready in time."
}

Write-Host "[dev] Ready at $baseUrl"
Write-Host "[dev] Health check: $baseUrl/healthz"
Write-Host "[dev] API base: $baseUrl/api/v1"

if ($Detach) {
    Write-Host "[dev] Running in background (detached)."
    Write-Host "[dev] View logs: docker logs -f $containerName"
    Write-Host "[dev] Stop: docker stop $containerName"
    exit 0
}

Write-Host "[dev] Streaming logs. Press Ctrl+C to stop and cleanup."
try {
    docker logs -f $containerName
} finally {
    Write-Host "[dev] Stopping container"
    docker stop $containerName 2>$null | Out-Host
}
