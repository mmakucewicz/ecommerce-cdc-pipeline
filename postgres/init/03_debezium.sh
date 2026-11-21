#!/bin/bash
# Creates a dedicated, least-privilege user for Debezium and a publication
# listing the tables captured by CDC. The password comes from the environment
# (.env), so no secret is stored in the repository.

psql -v ON_ERROR_STOP=1 \
     --username "$POSTGRES_USER" \
     --dbname "$POSTGRES_DB" \
     -v dbz_password="$DEBEZIUM_PASSWORD" <<'EOSQL'

CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD :'dbz_password';

GRANT CONNECT ON DATABASE shop TO debezium;
GRANT USAGE ON SCHEMA ecommerce TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA ecommerce TO debezium;

CREATE PUBLICATION dbz_publication FOR TABLE
    ecommerce.customers,
    ecommerce.products,
    ecommerce.orders,
    ecommerce.order_items;

EOSQL