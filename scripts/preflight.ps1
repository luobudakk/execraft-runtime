param(
    [switch]$Strict
)

$ErrorActionPreference = "Stop"

function Test-Command {
    param([Parameter(Mandatory = $true)][string]$Name)
    return $null -ne (Get-Command $Name -ErrorAction SilentlyContinue)
}

function Write-Result {
    param(
        [string]$Name,
        [bool]$Ok,
        [string]$Hint
    )
    if ($Ok) {
        Write-Host "[ok] $Name"
    } else {
        Write-Warning "[missing] $Name - $Hint"
    }
}

$checks = @(
    @{ Name = "cargo"; Ok = (Test-Command "cargo"); Hint = "Install Rust toolchain from https://rustup.rs/" },
    @{ Name = "docker"; Ok = (Test-Command "docker"); Hint = "Install Docker Desktop for Windows" },
    @{ Name = "git"; Ok = (Test-Command "git"); Hint = "Install Git for Windows" }
)

$failed = @()
foreach ($check in $checks) {
    Write-Result -Name $check.Name -Ok $check.Ok -Hint $check.Hint
    if (-not $check.Ok) { $failed += $check.Name }
}

if ($failed.Count -eq 0) {
    Write-Host ""
    Write-Host "Environment looks good."
    Write-Host "You can run local tests: cargo test"
    Write-Host "Or run Docker smoke test: .\scripts\quickstart.ps1"
    exit 0
}

Write-Host ""
Write-Host "Missing tools: $($failed -join ', ')"
Write-Host "Recommended next step:"
Write-Host "- If Rust is missing, use Docker path first: .\scripts\quickstart.ps1"

if ($Strict) {
    exit 1
}
