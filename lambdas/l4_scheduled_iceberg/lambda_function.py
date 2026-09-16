"""L4 Lambda entry point (python3.12 runtime)."""

from __future__ import annotations

from typing import Any

from lambdas.l4_scheduled_iceberg.handler import handler as _handler


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    return _handler(event)
