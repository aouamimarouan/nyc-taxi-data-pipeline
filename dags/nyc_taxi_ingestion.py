"""Discover and ingest new versions of NYC TLC Yellow Taxi monthly Parquet files.

Airflow 2.10+ (TaskFlow dynamic task-group mapping).
The Databricks job also runs on no-change days to reconcile pending Bronze/Silver work.
"""

import hashlib
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import requests
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import get_current_context
from databricks import sql

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
CONTROL_TABLE = "nyc_project.default.ingestion_control"
LANDING_VOLUME = "/Volumes/nyc_project/default/nyc_landing_layer"
STAGING_ROOT = Path("/opt/airflow/staging/nyc_taxi")
FIRST_YEAR = 2026
PUBLICATION_LAG_MONTHS = 2  # In November, scan up through September


def databricks_config():
    connection = BaseHook.get_connection("databricks_default")
    host = connection.host.removeprefix("https://").removeprefix("http://").rstrip("/")
    token = connection.password or connection.extra_dejson.get("token")
    http_path = connection.extra_dejson.get("http_path")
    if not (host and token and http_path):
        raise ValueError("Databricks connection requires host, token, and http_path")
    return host, token, http_path


@task(retries=2, retry_delay=timedelta(minutes=1))
def discover_source_files():
    """Discover published months conservatively, avoiding the newest months.

    The two-month lag is an operational rule, not a publication guarantee.
    Unexpected HTTP 403 remains an error rather than being treated as missing.
    """
    now = datetime.now(timezone.utc)
    last_eligible_month = now.year * 12 + now.month - PUBLICATION_LAG_MONTHS
    found = []

    for year in range(FIRST_YEAR, now.year + 1):
        for month in range(1, 13):
            if year * 12 + month > last_eligible_month:
                continue
            filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
            url = f"{BASE_URL}/{filename}"
            try:
                with urlopen(Request(url, method="HEAD"), timeout=30) as response:
                    size = int(response.headers["Content-Length"])
                    etag = response.headers.get("ETag")
                    if not etag or size <= 0:
                        raise ValueError(f"Missing ETag or invalid file size: {filename}")
                    found.append({
                        "file_name": filename,
                        "source_url": url,
                        "source_version": etag,
                        "file_size": size,
                        "last_modified": response.headers.get("Last-Modified"),
                        "year": year,
                        "month": month,
                    })
                    print(f"Available: {filename} ({size} bytes)")
            except HTTPError as exc:
                if exc.code == 404:
                    print(f"Not published: {filename}")
                    continue
                raise

    print(f"Discovered {len(found)} files")
    return found


@task(retries=2, retry_delay=timedelta(minutes=1))
def filter_new_versions(files):
    """Query successful source versions once; map only missing versions."""
    if not files:
        print("No source files published in the scanned period")
        return []

    host, token, http_path = databricks_config()
    with sql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT file_name, source_version FROM {CONTROL_TABLE} "
                "WHERE status = 'SUCCESS'"
            )
            ingested = {(row[0], row[1]) for row in cursor.fetchall()}

    pending = [
        item for item in files
        if (item["file_name"], item["source_version"]) not in ingested
    ]
    print(f"{len(files)} published, {len(pending)} new or changed")
    for item in pending:
        print("To ingest:", item["file_name"], item["source_version"])
    return pending


@task(retries=2, retry_delay=timedelta(minutes=1))
def download_source_file(metadata):
    """Download using chunks, validate ETag and length, compute SHA-256."""
    run_id = get_current_context()["run_id"]
    run_key = hashlib.sha256(run_id.encode()).hexdigest()[:16]
    staging_dir = STAGING_ROOT / run_key
    staging_dir.mkdir(parents=True, exist_ok=True)
    path = staging_dir / metadata["file_name"]
    temp_path = path.with_suffix(".parquet.part")
    digest = hashlib.sha256()
    size = 0

    try:
        with urlopen(metadata["source_url"], timeout=120) as response:
            current_etag = response.headers.get("ETag")
            if current_etag != metadata["source_version"]:
                raise ValueError(
                    f"Source version changed since discovery for {metadata['file_name']}: "
                    f"{metadata['source_version']} -> {current_etag}"
                )
            with open(temp_path, "wb") as dest:
                while chunk := response.read(1024 * 1024):
                    dest.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)

        if size != metadata["file_size"]:
            raise ValueError(f"Size mismatch: expected {metadata['file_size']}, got {size}")
        os.replace(temp_path, path)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise

    result = {
        "local_path": str(path),
        "file_name": metadata["file_name"],
        "file_size": size,
        "sha256": digest.hexdigest(),
    }
    print("Download verified:", result)
    return result


@task(retries=2, retry_delay=timedelta(minutes=1))
def upload_to_landing(metadata, download):
    """Upload immutable content-hash version and verify remote size."""
    local_path = Path(download["local_path"])
    if not local_path.is_file():
        raise FileNotFoundError(local_path)

    digest = hashlib.sha256()
    size = 0
    with open(local_path, "rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    if size != download["file_size"] or digest.hexdigest() != download["sha256"]:
        raise ValueError("Local staged file failed size or SHA-256 validation")

    host, token, _ = databricks_config()
    base = f"https://{host}/api/2.0/fs"
    directory = (
        f"{LANDING_VOLUME}/yellow/year={metadata['year']}"
        f"/month={metadata['month']:02d}/version={download['sha256']}"
    )
    destination = f"{directory}/{metadata['file_name']}"
    headers = {"Authorization": f"Bearer {token}"}
    requests.put(
        f"{base}/directories{directory}", headers=headers, timeout=60
    ).raise_for_status()

    file_url = f"{base}/files{destination}"
    existing = requests.head(file_url, headers=headers, timeout=60)
    if existing.status_code == 404:
        with open(local_path, "rb") as stream:
            response = requests.put(
                file_url,
                headers={**headers, "Content-Type": "application/octet-stream"},
                params={"overwrite": "false"},
                data=stream,
                timeout=600,
            )
        # A concurrent retry might create it; confirm final remote metadata below.
        if response.status_code not in (409, 412):
            response.raise_for_status()
    elif existing.status_code != 200:
        existing.raise_for_status()

    verified = requests.head(file_url, headers=headers, timeout=60)
    verified.raise_for_status()
    remote_length = verified.headers.get("Content-Length")
    if remote_length is None or int(remote_length) != size:
        raise ValueError(f"Remote file size mismatch: {remote_length}, expected {size}")

    print("Landing file verified:", destination)
    return {"landing_path": destination, "file_size": size, "sha256": download["sha256"]}


@task(retries=2, retry_delay=timedelta(minutes=1))
def update_ingestion_control(metadata, upload):
    """MERGE successful file/version so subsequent daily runs skip it."""
    host, token, http_path = databricks_config()
    query = f"""
        MERGE INTO {CONTROL_TABLE} AS target
        USING (
            SELECT
                %(file_name)s AS file_name,
                %(source_url)s AS source_url,
                %(source_version)s AS source_version,
                %(file_size)s AS file_size,
                %(landing_path)s AS landing_path
        ) AS source
        ON target.file_name = source.file_name
           AND target.source_version = source.source_version
        WHEN MATCHED THEN UPDATE SET
            target.source_url = source.source_url,
            target.file_size = source.file_size,
            target.status = 'SUCCESS',
            target.landing_path = source.landing_path,
            target.ingested_at = current_timestamp(),
            target.error_message = NULL
        WHEN NOT MATCHED THEN INSERT (
            file_name, source_url, source_version, file_size,
            status, landing_path, ingested_at, error_message
        ) VALUES (
            source.file_name, source.source_url, source.source_version,
            source.file_size, 'SUCCESS', source.landing_path,
            current_timestamp(), NULL
        )
    """
    values = {
        "file_name": metadata["file_name"],
        "source_url": metadata["source_url"],
        "source_version": metadata["source_version"],
        "file_size": upload["file_size"],
        "landing_path": upload["landing_path"],
    }
    with sql.connect(server_hostname=host, http_path=http_path, access_token=token) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, values)
    print("Recorded SUCCESS:", metadata["file_name"])
    return upload["landing_path"]


@task
def cleanup_staging(download, landing_path):
    """Remove local staged file only after the control table is updated."""
    path = Path(download["local_path"])
    path.unlink(missing_ok=True)
    # Only delete this DAG run's subdirectory; don't touch other runs.
    if path.parent.parent == STAGING_ROOT and path.parent.exists():
        try:
            path.parent.rmdir()
        except OSError:
            pass  # Other monthly files may still be staged by this run.
    print("Cleaned local staging for:", landing_path)


@task_group(group_id="ingest_month")
def ingest_month(metadata):
    downloaded = download_source_file(metadata)
    uploaded = upload_to_landing(metadata, downloaded)
    recorded = update_ingestion_control(metadata, uploaded)
    cleanup_staging(downloaded, recorded)


# CHANGED: no longer skip Databricks when there are zero newly ingested files.
@task(retries=1, retry_delay=timedelta(minutes=2), trigger_rule="none_failed")
def trigger_databricks_job(pending):
    """Run Databricks even on no-change days for Bronze/Silver reconciliation.

    The upstream mapped ingestion group must have no failures. Empty mapping
    may be SKIPPED, which is allowed by the 'none_failed' trigger rule.
    """
    context = get_current_context()
    if pending:
        print(f"{len(pending)} new or changed file version(s) discovered.")
    else:
        print("No new file versions. Triggering Databricks to reconcile Bronze/Silver.")

    # Set in Airflow UI > Admin > Variables (no job ID in source code).
    job_id = int(Variable.get("nyc_bronze_job_id"))
    host, token, _ = databricks_config()
    api = f"https://{host}/api/2.1/jobs"
    headers = {"Authorization": f"Bearer {token}"}

    # Stable per DAG run and job, including Airflow task retries.
    run_key = f"{context['dag'].dag_id}:{context['run_id']}:{job_id}"
    idem = hashlib.sha256(run_key.encode("utf-8")).hexdigest()

    response = requests.post(
        f"{api}/run-now",
        headers=headers,
        json={"job_id": job_id, "idempotency_token": idem},
        timeout=60,
    )
    response.raise_for_status()
    run_id = response.json()["run_id"]
    print(f"Started Databricks job {job_id}, run {run_id}")

    # Wait for the actual Databricks job outcome.
    deadline = time.monotonic() + 3600
    while time.monotonic() < deadline:
        result = requests.get(
            f"{api}/runs/get",
            headers=headers,
            params={"run_id": run_id},
            timeout=60,
        )
        result.raise_for_status()
        job_run = result.json()
        state = job_run.get("state", {})
        life_cycle = state.get("life_cycle_state")
        outcome = state.get("result_state")
        print(f"Databricks run {run_id}: {life_cycle} / {outcome}")

        if life_cycle in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            if outcome == "SUCCESS":
                return {"job_id": job_id, "run_id": run_id, "status": "SUCCESS"}
            raise RuntimeError(
                f"Databricks job failed: run_id={run_id}, "
                f"state={life_cycle}, result={outcome}, "
                f"message={state.get('state_message')}"
            )
        time.sleep(20)
    raise TimeoutError(f"Databricks job {job_id}, run {run_id} exceeded one hour")


with DAG(
    dag_id="nyc_taxi_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",  # Every day at 06:00 UTC
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    tags=["nyc-taxi", "ingestion"],
) as dag:
    published = discover_source_files()
    pending = filter_new_versions(published)
    mapped_ingest = ingest_month.expand(metadata=pending)
    job = trigger_databricks_job(pending)
    mapped_ingest >> job
