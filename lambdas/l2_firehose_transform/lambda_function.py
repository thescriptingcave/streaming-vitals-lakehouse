"""L2 Lambda entry point (python3.12 runtime)."""

from __future__ import annotations

from typing import Any

from lambdas.l2_firehose_transform.handler import handler as _handler


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    return _handler(event)
