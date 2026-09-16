#!/usr/bin/env bash
# Superset bootstrap: pinned drivers, Trino dialect fix, admin user, meta DB
# init, then start the web server. Idempotent - safe on every start.
set -euo pipefail

pip install -r /app/requirements-local.txt

# Vendored fix for Iceberg $partitions detection in trino==0.339.0 SQLAlchemy
# dialect (see docker/patches/trino_iceberg_partitions.py for rationale).
python /app/patches/trino_iceberg_partitions.py

superset fab create-admin \
  --username "${SUPERSET_ADMIN_USERNAME:-admin}" \
  --firstname "${SUPERSET_ADMIN_FIRSTNAME:-Admin}" \
  --lastname "${SUPERSET_ADMIN_LASTNAME:-User}" \
  --email "${SUPERSET_ADMIN_EMAIL:-admin@superset.local}" \
  --password "${SUPERSET_ADMIN_PASSWORD:-admin}" || true

superset db upgrade
superset init

exec /usr/bin/run-server.sh "$@"