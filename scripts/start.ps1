$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

& "$PSScriptRoot\prepare.ps1"
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

docker compose up --build airflow-init
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

docker compose up -d airflow-webserver airflow-scheduler
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Airflow is starting at http://localhost:8080"
Write-Host "Run .\scripts\status.ps1 to inspect service health."

