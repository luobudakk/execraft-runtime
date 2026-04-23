param(
    [string]$Image = "execraft-runtime:local",
    [int]$Port = 18080,
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"

function Require-Command {
    param([Parameter(Mandatory = $true)][string]$Name)
    if ($null -eq (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' is not available. Please install it first."
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

function Resolve-Port {
    param([int]$RequestedPort)

    if ($RequestedPort -ne 0) {
        $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Any, $RequestedPort)
        try {
            $listener.Start()
            return $RequestedPort
        } catch {
            Write-Warning "[quickstart] Port $RequestedPort is unavailable, selecting a free port automatically."
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

function Wait-TaskTerminal {
    param(
        [Parameter(Mandatory = $true)][string]$BaseUrl,
        [Parameter(Mandatory = $true)][string]$TaskId,
        [int]$MaxAttempts = 120,
        [int]$DelayMs = 250
    )
    for ($i = 1; $i -le $MaxAttempts; $i++) {
        $status = Invoke-RestMethod -Uri "$BaseUrl/api/v1/tasks/$TaskId" -Method Get -TimeoutSec 5
        if ($status.status -in @("success", "failed", "cancelled")) {
            return $status
        }
        Start-Sleep -Milliseconds $DelayMs
    }
    throw "Task '$TaskId' did not reach a terminal state in time."
}

Require-Command -Name "docker"

$projectRoot = Split-Path -Parent $PSScriptRoot
$Port = Resolve-Port -RequestedPort $Port
$baseUrl = "http://127.0.0.1:$Port"
$containerName = "execraft-runtime-quickstart-$PID"

if (-not $SkipBuild) {
    Write-Host "[quickstart] Building image: $Image"
    docker build -t $Image $projectRoot | Out-Host
}

Write-Host "[quickstart] Starting container on port $Port"
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
        Write-Warning "[quickstart] Port conflict detected. Retrying with port $Port."
        continue
    }

    throw "Failed to start container:`n$outputText"
}

if (-not $runSucceeded) {
    throw "Failed to start container after retries."
}
$runOutput | Out-Host

try {
    Write-Host "[quickstart] Waiting for readiness: $baseUrl/readyz"
    if (-not (Wait-Ready -BaseUrl $baseUrl)) {
        throw "Runtime did not become ready in time."
    }

    $body = @{
        execution = @{
            kind = "command"
            program = "/bin/sh"
            args = @("-c", "echo quickstart-ok")
        }
    } | ConvertTo-Json -Depth 8

    Write-Host "[quickstart] Submitting test task"
    $submit = Invoke-RestMethod -Uri "$baseUrl/api/v1/tasks" -Method Post -ContentType "application/json" -Body $body -TimeoutSec 10
    $taskId = $submit.task_id
    if ([string]::IsNullOrWhiteSpace($taskId)) {
        throw "Task submission response does not contain task_id."
    }
    Write-Host "[quickstart] task_id=$taskId"

    $terminal = Wait-TaskTerminal -BaseUrl $baseUrl -TaskId $taskId
    Write-Host "[quickstart] terminal status: $($terminal.status)"
    if ($terminal.status -ne "success") {
        throw "Smoke task failed with status '$($terminal.status)'."
    }

    Write-Host "[quickstart] stdout:"
    if ($null -ne $terminal.stdout) {
        Write-Host $terminal.stdout
    } else {
        Write-Host "(empty)"
    }

    Write-Host "[quickstart] Smoke test passed."
} finally {
    Write-Host "[quickstart] Stopping container"
    docker stop $containerName 2>$null | Out-Host
}
