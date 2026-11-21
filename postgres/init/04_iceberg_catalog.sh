#!/bin/bash
# Creates a separate database and owner for the Iceberg JDBC catalog.
# The catalog stores only table metadata pointers (which metadata.json is current);
# the actual data and metadata files live in S3 (SeaweedFS).

psql -v ON_ERROR_STOP=1 \
     --username "$POSTGRES_USER" \
     --dbname "$POSTGRES_DB" \
     -v iceberg_password="$ICEBERG_CATALOG_PASSWORD" <<'EOSQL'

CREATE ROLE iceberg WITH LOGIN PASSWORD :'iceberg_password';
CREATE DATABASE iceberg_catalog OWNER iceberg;

EOSQL
