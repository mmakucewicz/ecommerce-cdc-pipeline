FROM apache/airflow:3.3.1

# dbt lives in its own virtual environment, so its dependencies
# cannot conflict with Airflow's pinned dependencies
RUN python -m venv /opt/airflow/dbt-venv \
    && /opt/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.11.12 dbt-trino==1.10.2
