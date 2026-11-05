"""Verify Airflow can authenticate to Databricks and reach Unity Catalog."""

from __future__ import annotations

import os

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from databricks.sdk import WorkspaceClient
from pendulum import datetime


def _workspace_client() -> WorkspaceClient:
    connection = BaseHook.get_connection("databricks_default")
    host = (connection.host or "").rstrip("/")
    token = connection.password or connection.extra_dejson.get("token")

    if not host or not token or "your-workspace" in host or "replace-me" in token:
        raise AirflowException(
            "Configure DATABRICKS_HOST and DATABRICKS_TOKEN in .env before running this DAG."
        )

    return WorkspaceClient(host=host, token=token)


def check_workspace() -> None:
    client = _workspace_client()
    user = client.current_user.me()
    if not user or not user.user_name:
        raise AirflowException("Databricks authentication returned no current user.")
    print(f"Databricks authentication succeeded for {user.user_name}")


def check_unity_catalog() -> None:
    catalog = os.environ["DATABRICKS_CATALOG"]
    schema = os.environ["DATABRICKS_SCHEMA"]
    volume = os.environ["DATABRICKS_VOLUME"]
    full_name = f"{catalog}.{schema}.{volume}"

    client = _workspace_client()
    result = client.volumes.read(name=full_name)
    print(f"Unity Catalog Volume is readable: {result.name}")


def check_warehouse() -> None:
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "").strip()
    if not warehouse_id:
        print("DATABRICKS_WAREHOUSE_ID is blank; skipping SQL warehouse check.")
        return

    client = _workspace_client()
    warehouse = client.warehouses.get(id=warehouse_id)
    print(f"Databricks SQL warehouse is reachable: {warehouse.name} ({warehouse.id})")


with DAG(
    dag_id="databricks_connectivity_smoke",
    description="Validate the Airflow-to-Databricks connection and Unity Catalog access.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "local-dev"},
    tags=["local", "databricks", "smoke-test"],
) as dag:
    workspace = PythonOperator(
        task_id="check_workspace_authentication",
        python_callable=check_workspace,
    )
    unity_catalog = PythonOperator(
        task_id="check_unity_catalog_volume",
        python_callable=check_unity_catalog,
    )
    warehouse = PythonOperator(
        task_id="check_sql_warehouse",
        python_callable=check_warehouse,
    )

    workspace >> unity_catalog >> warehouse

