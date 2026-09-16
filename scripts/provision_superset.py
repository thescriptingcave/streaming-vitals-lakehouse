"""Provision Superset read-models against Trino/Iceberg (REST API only).

Gotcha from the prior project: Superset's python client bypasses CSRF and
connects through the _/api/v1/superset/ views; provisioning must use the REST
API with a session carrying csrf_token -> X-CSRFToken (docs/ARCHITECTURE.md §4).

Creates idempotently:
  * DB connection "Trino (Iceberg)" -> trino://trino@trino:8080/healthcare
    (catalog iceberg, schema healthcare), allow_dml=false (SELECT-only SQL Lab)
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Any

try:
    import requests
except ImportError as exc:  # pragma: no cover - env would lack dev deps
    sys.exit(f"requests is required (uv sync --extra dev); {exc}")

SUPERSET_BASE = os.environ.get("SUPERSET_BASE", "http://localhost:8088")
DB_NAME = "Trino (Iceberg)"
TRINO_URI = os.environ.get(
    "TRINO_ICEBERG_URI", "trino://trino@trino:8080/healthcare?catalog=iceberg"
)


def _headers(csrf: str | None = None) -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if csrf:
        headers["X-CSRFToken"] = csrf
    return headers


def login(session: requests.Session, base: str, username: str, password: str) -> str:
    resp = session.post(
        f"{base}/api/v1/security/login",
        json={"username": username, "password": password, "provider": "db", "refresh": True},
        timeout=15,
    )
    resp.raise_for_status()
    return str(resp.json()["access_token"])


def csrf(session: requests.Session, base: str, token: str) -> str:
    resp = session.get(
        f"{base}/api/v1/security/csrf_token/",
        headers={"Authorization": f"Bearer {token}"},
        timeout=15,
    )
    resp.raise_for_status()
    return str(resp.json()["result"])


def ensure_database(session: requests.Session, base: str, csrf_token: str) -> int:
    payload: dict[str, Any] = {
        "database_name": DB_NAME,
        "sqlalchemy_uri": TRINO_URI,
        "expose_in_sqllab": True,
        "allow_dml": False,
        "configuration_method": "sqlalchemy_form",
    }
    resp = session.post(
        f"{base}/api/v1/database/",
        json=payload,
        headers=_headers(csrf_token),
        timeout=15,
    )
    resp.raise_for_status()
    return int(resp.json()["id"])


def find_database(session: requests.Session, base: str) -> int | None:
    resp = session.get(
        f"{base}/api/v1/database/?q=(filters:!((col:database_name,opr:eq,value:'{DB_NAME}')))",
        timeout=15,
    )
    resp.raise_for_status()
    rows = resp.json().get("result") or []
    return int(rows[0]["id"]) if rows else None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Idempotently provision the Trino(Iceberg) DB in Superset."
    )
    parser.add_argument("--base", default=SUPERSET_BASE)
    args = parser.parse_args()
    username = os.environ.get("SUPERSET_ADMIN_USER", "admin")
    password = os.environ.get("SUPERSET_ADMIN_PASSWORD", "admin")

    session = requests.Session()
    token = login(session, args.base, username, password)
    csrf_token = csrf(session, args.base, token)

    existing = find_database(session, args.base)
    if existing is not None:
        print(f"database '{DB_NAME}' already exists (id={existing}); skipping")
        return 0
    db_id = ensure_database(session, args.base, csrf_token)
    print(f"database '{DB_NAME}' created (id={db_id}) allow_dml=false uri={TRINO_URI}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
