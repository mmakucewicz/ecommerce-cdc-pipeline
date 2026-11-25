#!/bin/bash
# Creates the Airflow metadata database and its owner.

psql -v ON_ERROR_STOP=1 \
     --username "$POSTGRES_USER" \
     --dbname "$POSTGRES_DB" \
     -v airflow_password="$AIRFLOW_DB_PASSWORD" <<'EOSQL'

CREATE ROLE airflow WITH LOGIN PASSWORD :'airflow_password';
CREATE DATABASE airflow OWNER airflow;

EOSQL
