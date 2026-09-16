"""L1 Lambda entry point (python3.12 runtime)."""

from __future__ import annotations

from typing import Any

from lambdas.l1_alert_notifier.handler import handler as _handler


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    return _handler(event)
