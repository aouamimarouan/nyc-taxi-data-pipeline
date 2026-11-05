$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

# This intentionally preserves the PostgreSQL volume and Airflow logs.
docker compose down

