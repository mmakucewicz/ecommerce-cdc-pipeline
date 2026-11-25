"""
Daily Iceberg maintenance of the tables written by the streaming job.

Every 10-second MERGE creates new small files and a new snapshot. Without maintenance,
query performance and metadata size degrade over time. One task per table, so a failure
(e.g. a commit conflict with a concurrent streaming MERGE) is retried for that table only.
"""

from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

TABLES = [
    "lakehouse.bronze.cdc_events",
    "lakehouse.silver.customers",
    "lakehouse.silver.products",
    "lakehouse.silver.orders",
    "lakehouse.silver.order_items",
]

DBT_RUN_OPERATION = (
    "/opt/airflow/dbt-venv/bin/dbt run-operation iceberg_maintenance "
    "--args '{{\"table\": \"{table}\"}}' "
    "--project-dir /opt/airflow/dbt "
    "--profiles-dir /opt/airflow/dbt "
    "--target-path /tmp/dbt/target-maintenance "
    "--log-path /tmp/dbt/logs-maintenance"
)

with DAG(
    dag_id="iceberg_maintenance",
    description="Compaction, snapshot expiration and orphan file cleanup via Trino",
    schedule="0 3 * * *",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
    tags=["iceberg", "maintenance"],
) as dag:
    for table in TABLES:
        BashOperator(
            task_id=f"maintain_{table.split('.', 1)[1].replace('.', '_')}",
            bash_command=DBT_RUN_OPERATION.format(table=table),
        )
