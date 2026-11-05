$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

function Fail([string] $Message) {
    Write-Error $Message
    exit 1
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Fail "Docker CLI was not found. Install and start Docker Desktop first."
}

$context = (docker context show 2>$null).Trim()
if ($context -ne "desktop-linux") {
    Write-Warning "Docker context is '$context'. Select the Docker Desktop Linux context with: docker context use desktop-linux"
}

docker info *> $null
if ($LASTEXITCODE -ne 0) {
    Fail "Docker's Linux daemon is unavailable. Start Docker Desktop and enable its WSL2 backend, then rerun this script."
}

if (-not (Test-Path .env)) {
    Copy-Item .env.example .env
    Write-Host "Created .env from .env.example. Update Databricks values before running the smoke DAG."
}

$envText = Get-Content .env -Raw
if ($envText -match '(?m)^AIRFLOW__CORE__FERNET_KEY=\s*$') {
    $bytes = New-Object byte[] 32
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
    $fernetKey = [Convert]::ToBase64String($bytes).Replace('+', '-').Replace('/', '_')
    $envText = [regex]::Replace($envText, '(?m)^AIRFLOW__CORE__FERNET_KEY=\s*$', "AIRFLOW__CORE__FERNET_KEY=$fernetKey")
    Set-Content -Path .env -Value $envText -NoNewline
    Write-Host "Generated a local Airflow Fernet key in .env."
}

docker compose config *> $null
if ($LASTEXITCODE -ne 0) {
    Fail "Docker Compose validation failed. Check .env and docker-compose.yml."
}

Write-Host "Local environment prerequisites and Compose configuration are valid."

