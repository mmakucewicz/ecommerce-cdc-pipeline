"""
Refreshes the gold layer every 15 minutes.

1. dbt source freshness - fails if streaming stopped delivering data to silver/bronze,
   so stale data is never published as fresh gold tables.
2. dbt build            - rebuilds the gold models and runs all data tests.
"""

from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

DBT = (
    "/opt/airflow/dbt-venv/bin/dbt {command} "
    "--project-dir /opt/airflow/dbt "
    "--profiles-dir /opt/airflow/dbt "
    "--target-path /tmp/dbt/target "
    "--log-path /tmp/dbt/logs"
)

with DAG(
    dag_id="dbt_gold",
    description="Source freshness check, then dbt build of the gold layer",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["dbt", "gold"],
) as dag:
    source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=DBT.format(command="source freshness"),
    )

    build = BashOperator(
        task_id="dbt_build",
        bash_command=DBT.format(command="build"),
    )

    source_freshness >> build
