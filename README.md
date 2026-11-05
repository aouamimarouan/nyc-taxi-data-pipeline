# NYC Taxi Data Pipeline — Local Environment

This repository contains the local development environment for the NYC Taxi pipeline. Airflow runs in Docker Desktop and connects to Databricks Free Edition. The ingestion DAG hands files to an existing Databricks Bronze job.

## Prerequisites

- Windows with Docker Desktop using the WSL2 backend.
- An existing Databricks Free Edition workspace.
- Permission to create or read the Unity Catalog Volume used by the smoke test.
- A Databricks token with access to the target catalog, schema, and volume.

Host Python and Java are not required for this setup because Airflow and its dependencies run inside the Docker image.

## First-time setup

1. Start Docker Desktop and verify that its Linux engine is running.
2. From PowerShell in this directory, run:

   ```powershell
   .\scripts\prepare.ps1
   ```

   This creates `.env` from `.env.example`, generates the local Airflow Fernet key, and validates Docker Compose. `.env` is ignored by Git.

3. Edit `.env` and set:

   - `DATABRICKS_HOST`
   - `DATABRICKS_TOKEN`
   - `DATABRICKS_CATALOG`
   - `DATABRICKS_SCHEMA`
   - `DATABRICKS_VOLUME`
   - optionally, `DATABRICKS_WAREHOUSE_ID`

4. Start Airflow:

   ```powershell
   .\scripts\start.ps1
   ```

5. Open [http://localhost:8080](http://localhost:8080) and sign in with the `AIRFLOW_ADMIN_USERNAME` and `AIRFLOW_ADMIN_PASSWORD` values from `.env`.

## Smoke test

The `databricks_connectivity_smoke` DAG is paused by default. In the Airflow UI, unpause it and trigger it manually. It checks, in order:

1. Databricks workspace authentication and current-user access.
2. Read access to the configured Unity Catalog Volume.
3. Optional access to the configured SQL warehouse.

The DAG does not upload files, create tables, modify data, or start Databricks jobs.

## Lifecycle commands

```powershell
.\scripts\status.ps1
.\scripts\stop.ps1
```

`stop.ps1` stops containers without deleting the PostgreSQL volume or Airflow logs. To remove the local database volume deliberately, use `docker compose down -v` after confirming that no local state is needed.

## Troubleshooting

If Docker reports that `dockerDesktopLinuxEngine` is unavailable or access is denied, start Docker Desktop, wait for the engine to become ready, and ensure WSL2 integration is enabled. Then rerun `prepare.ps1`.

If the smoke DAG fails, inspect the task log first. The most common causes are an incorrect workspace URL or token, a token without Unity Catalog permissions, or a volume name that does not exist in the configured catalog and schema.

