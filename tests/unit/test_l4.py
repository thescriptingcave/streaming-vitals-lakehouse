"""Unit tests for L4 Scheduled Iceberg (advisory lock + run-date idempotency)."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import lambdas.l4_scheduled_iceberg.handler as l4
from lambdas.common import from_attrs


class _DdbExceptions:
    class ConditionalCheckFailedException(Exception):
        pass


class FakeLockManager:
    """Simulates DynamoDB conditional writes; exposes last condition."""

    exceptions = _DdbExceptions

    def __init__(self, *, occupied: bool = False) -> None:
        self.occupied = occupied
        self.last_condition: str | None = None
        self.items: list[dict[str, Any]] = []

    def put_item(self, **kwargs: Any) -> dict[str, Any]:
        self.last_condition = kwargs.get("ConditionExpression")
        if self.occupied:
            raise self.exceptions.ConditionalCheckFailedException()
        self.items.append(kwargs["Item"])
        return {}

    def delete_item(self, **kwargs: Any) -> dict[str, Any]:
        self.items = []
        return {}


def test_acquire_lock_uses_conditional_write():
    ddb = FakeLockManager()
    assert l4.acquire_lock(ddb, "2026-09-15") is True
    assert ddb.last_condition == "attribute_not_exists(#lock)"
    item = from_attrs(ddb.items[0])
    assert item["run_date"] == "2026-09-15"
    assert isinstance(item["expires_at"], (int, Decimal))


def test_acquire_lock_rejects_when_held():
    assert l4.acquire_lock(FakeLockManager(occupied=True), "2026-09-15") is False


def test_run_date_defaults_to_today_when_missing():
    event: dict[str, Any] = {}
    run_date = l4._run_date(event)
    assert run_date.count("-") == 2


def test_run_date_from_event_detail():
    assert l4._run_date({"detail": {"run_date": "2026-09-15"}}) == "2026-09-15"


def test_run_date_tolerates_querystring_form():
    # EventBridge Scheduler injects key=value style when templated input used.
    assert l4._run_date({"detail": {"run_date": "run_date=2026-09-15"}}) == "2026-09-15"
