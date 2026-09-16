"""L3 Lambda entry point (python3.12 runtime, APIGW v2 payload 2.0)."""

from __future__ import annotations

from typing import Any

from lambdas.l3_patients_api.handler import handler as _handler


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    return _handler(event)
