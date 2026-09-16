"""L4 Scheduled Iceberg job — EventBridge Scheduler -> Lambda.

Pattern learned: serverless cron + idempotency. Daily 02:00 run: acquire an
advisory DynamoDB lock, run Iceberg maintenance (expire_snapshots,
remove_orphan_files) and export the ML feature table to Parquet with a
train/val/test split by patient (DATA_FLOW.md §7, DATA_MODEL.md §6).

Idempotency keys: run_date partition; atomic manifest rename; lock TTL.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

from lambdas.common import client, get_logger, to_attrs
from ml.export_features import run_ml_export

logger = get_logger(__name__)

LOCK_TABLE = "maintenance-lock"
LOCK_KEY = "maintenance-job"
LOCK_EVENT = {"ConditionalCheckFailedException", "TransactionCanceledException"}


def _run_date(event: dict[str, Any]) -> str:
    explicit = (event.get("detail") or {}).get("run_date")
    if explicit:
        # EventBridge Scheduler may inject key=value form; tolerate that shape.
        return str(explicit).split("=")[-1]
    return datetime.now(UTC).strftime("%Y-%m-%d")


def acquire_lock(ddb: Any, run_date: str, ttl_seconds: int = 3600) -> bool:
    """Try to take the advisory lock; False means another run is active."""
    ttl = int(datetime.now(UTC).timestamp()) + ttl_seconds
    try:
        ddb.put_item(
            TableName=os.environ.get("LOCK_TABLE", LOCK_TABLE),
            Item=to_attrs({"lock": LOCK_KEY, "run_date": run_date, "expires_at": ttl}),
            ConditionExpression="attribute_not_exists(#lock)",
            ExpressionAttributeNames={"#lock": "lock"},
        )
        return True
    except ddb.exceptions.ConditionalCheckFailedException:
        return False


def release_lock(ddb: Any) -> None:
    try:
        ddb.delete_item(
            TableName=os.environ.get("LOCK_TABLE", LOCK_TABLE),
            Key=to_attrs({"lock": LOCK_KEY}),
        )
    except Exception as exc:  # noqa: BLE001 - lock expiry protects overlap anyway
        logger.warning("lock release failed", data={"error": str(exc)})


def handler(event: dict[str, Any]) -> dict[str, Any]:
    run_date = _run_date(event)
    ddb = client("dynamodb")
    if not acquire_lock(ddb, run_date):
        logger.info("skipping run: lock held", data={"run_date": run_date})
        return {"skipped": True, "run_date": run_date}
    try:
        result = run_ml_export(run_date=run_date)
        logger.info("ml export complete", data={"run_date": run_date, "partitions": result})
        return {"skipped": False, "run_date": run_date, "export": result}
    finally:
        release_lock(ddb)
