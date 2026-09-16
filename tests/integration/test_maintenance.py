"""Maintenance integration: advisory lock + ML export manifest (L4)."""

from __future__ import annotations

import uuid

import pytest

import lambdas.l4_scheduled_iceberg.handler as l4
import ml.export_features as export
from lambdas.common import to_attrs

pytestmark = pytest.mark.floci


def _create_lock_table(dynamodb, table: str) -> None:
    dynamodb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "lock", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "lock", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    dynamodb.get_waiter("table_exists").wait(TableName=table)


@pytest.mark.floci
def test_it_maintenance_runs_export_and_releases_lock(aws_clients, monkeypatch):
    dynamodb = aws_clients["dynamodb"]
    s3 = aws_clients["s3"]
    bucket = "it-lake-" + uuid.uuid4().hex[:6]
    table = "it-lock-" + uuid.uuid4().hex[:6]
    s3.create_bucket(Bucket=bucket)

    _create_lock_table(dynamodb, table)
    monkeypatch.setenv("LOCK_TABLE", table)
    monkeypatch.setenv("ML_BUCKET", bucket)
    monkeypatch.setattr(l4, "client", lambda service: dynamodb)
    monkeypatch.setattr(export, "client", lambda service: s3)

    result = l4.handler({"detail": {"run_date": "2026-09-15"}})
    assert result["skipped"] is False
    assert result["export"]["patients"] > 0

    manifest = result["export"]["manifest"]
    meta = s3.head_object(Bucket=bucket, Key=manifest)
    assert meta["ResponseMetadata"]["HTTPStatusCode"] == 200

    lock_rows = dynamodb.scan(TableName=table).get("Items", [])
    assert lock_rows == [], "advisory lock must be released after the run"


@pytest.mark.floci
def test_it_maintenance_skips_when_lock_held(aws_clients, monkeypatch):
    dynamodb = aws_clients["dynamodb"]
    table = "it-lock-" + uuid.uuid4().hex[:6]
    _create_lock_table(dynamodb, table)
    monkeypatch.setenv("LOCK_TABLE", table)
    # Occupied lock from a previous run (expires_at far in the future).
    dynamodb.put_item(
        TableName=table,
        Item=to_attrs(
            {"lock": "maintenance-job", "run_date": "2025-01-01", "expires_at": 4_000_000_000}
        ),
    )
    monkeypatch.setattr(l4, "client", lambda service: dynamodb)

    result = l4.handler({"detail": {"run_date": "2026-09-15"}})
    assert result["skipped"] is True
