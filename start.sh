#!/bin/bash
# Renders spark-defaults.conf from the template, injecting secrets from
# environment variables, so no password is stored in the repository or image.
set -euo pipefail

: "${ICEBERG_CATALOG_PASSWORD:?ICEBERG_CATALOG_PASSWORD is not set}"

sed -e "s|__ICEBERG_CATALOG_PASSWORD__|${ICEBERG_CATALOG_PASSWORD}|" \
    /opt/spark/conf-templates/spark-defaults.conf.template \
    > /opt/spark/conf-runtime/spark-defaults.conf

exec "$@"
