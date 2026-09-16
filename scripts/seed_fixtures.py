"""Seed DynamoDB read-models with deterministic fixtures so L1/L3 + dashboards
work before Flink runs: latest_vitals and unsent alerts for the P-coded
cohort. Idempotent (upsert by known PKs). Tables honor AWS_ENDPOINT_URL.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import UTC, datetime, timedelta
from typing import Any

from lambdas.common import client
from producer.vitals import generate_reading, normalize

ALERTS_TABLE = os.environ.get("ALERTS_TABLE", "alerts")
LATEST_VITALS_TABLE = os.environ.get("LATEST_VITALS_TABLE", "latest_vitals")
PATIENTS = ("P0001", "P0002", "P0003")
TTL_DAYS = 14


def _now() -> datetime:
    return datetime.now(UTC)


def _ttl(days: int) -> int:
    return int((_now() + timedelta(days=days)).timestamp())


def seed_latest(ddb: Any) -> list[str]:
    items: list[str] = []
    for idx, patient in enumerate(PATIENTS):
        raw = generate_reading(patient, f"bed-{idx:03d}", abnormal=(idx % 2 == 1), seed=idx)
        reading = normalize(raw)
        item = {"patient_id": reading["patient_id"], "expires_at": _ttl(2)}
        for key in (
            "heart_rate",
            "spo2",
            "systolic_bp",
            "diastolic_bp",
            "temperature",
            "resp_rate",
            "event_time",
        ):
            item[key] = reading.get(key, "")
        item["is_abnormal"] = reading["is_abnormal"]
        ddb.put_item(TableName=LATEST_VITALS_TABLE, Item=item)
        items.append(patient)
    return items


def seed_alerts(ddb: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for idx, patient in enumerate(PATIENTS):
        if idx % 2 == 1:
            continue
        alert = {
            "patient_id": patient,
            "alert_id": f"fix-{patient}",
            "rule": "SPO2_LOW",
            "severity": "warning",
            "value": "90.0",
            "threshold": "92.0",
            "event_ts": (_now() - timedelta(minutes=idx + 1)).isoformat(timespec="seconds"),
            "sent": False,
            "expires_at": _ttl(TTL_DAYS),
        }
        ddb.put_item(TableName=ALERTS_TABLE, Item=alert)
        rows.append(alert)
    return rows


def main() -> int:
    ddb = client("dynamodb")
    try:
        latest = seed_latest(ddb)
        alerts = seed_alerts(ddb)
    except Exception as exc:  # noqa: BLE001 - CLI surfacing
        print(f"seeding failed: {exc}", file=sys.stderr)
        return 1
    print(
        json.dumps({"latest_vitals": latest, "alerts": [a["alert_id"] for a in alerts]}, indent=2)
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
