$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

docker compose ps
Write-Host ""
Write-Host "DAG import errors:"
docker compose exec airflow-scheduler airflow dags list-import-errors 2>$null

