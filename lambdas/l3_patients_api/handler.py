"""L3 Patients REST API — API Gateway v2 -> Lambda -> DynamoDB.

Pattern learned: request/response + auth. Serves the read-model for the
dashboards/UI: latest vitals and alert history per patient.

Routes (payload-format 2.0):
  GET /patients/{id}/latest-vitals
  GET /patients/{id}/alerts?limit=10
Validation: patient_id pattern, max length, 400/404 mapping, Cache-Control.
"""

from __future__ import annotations

import json
import os
import re
from typing import Any

from lambdas.common import client, from_attrs, get_logger, to_attrs

logger = get_logger(__name__)

PATIENT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
MAX_LIMIT = 100

ALERTS_TABLE = "alerts"
LATEST_VITALS_TABLE = "latest_vitals"


class HttpError(Exception):
    """Raise inside the handler to return a specific HTTP response."""

    def __init__(self, status: int, detail: str) -> None:
        super().__init__(detail)
        self.status = status
        self.detail = detail


def _env(name: str) -> str:
    return os.environ[name]


def _json_response(status: int, body: dict[str, Any]) -> dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json", "Cache-Control": "no-store"},
        "body": json.dumps(body, default=str),
    }


def parse_request(event: dict[str, Any]) -> tuple[str, str, int]:
    """Extract (patient_id, kind, limit) from an APIGW v2 event."""
    params = event.get("pathParameters") or {}
    query = event.get("queryStringParameters") or {}
    patient_id = str(params.get("patient_id") or params.get("id") or "")
    path: str = event.get("rawPath", "")
    kind = "alerts" if path.rstrip("/").endswith("alerts") else "latest"
    try:
        limit = int(query.get("limit", 10))
    except (TypeError, ValueError):
        limit = 10
    limit = max(1, min(limit, MAX_LIMIT))
    return patient_id, kind, limit


def handler(event: dict[str, Any]) -> dict[str, Any]:
    """Route a request to latest_vitals or alerts and map HTTP semantics."""
    try:
        patient_id, kind, limit = parse_request(event)
        if not PATIENT_ID_RE.match(patient_id):
            raise HttpError(400, f"invalid patient_id: {patient_id!r}")
        ddb = client("dynamodb")
        if kind == "latest":
            item = ddb.get_item(
                TableName=os.environ.get("LATEST_VITALS_TABLE", LATEST_VITALS_TABLE),
                Key=to_attrs({"patient_id": patient_id}),
            ).get("Item")
            if not item:
                raise HttpError(404, f"no latest vitals for {patient_id}")
            return _json_response(200, from_attrs(item))
        rows = ddb.query(
            TableName=os.environ.get("ALERTS_TABLE", ALERTS_TABLE),
            KeyConditionExpression="patient_id = :p",
            ExpressionAttributeValues=to_attrs({":p": patient_id}),
            Limit=limit,
            ScanIndexForward=False,
        ).get("Items", [])
        alerts = [from_attrs(row) for row in rows]
        return _json_response(200, {"patient_id": patient_id, "alerts": alerts})
    except HttpError as exc:
        logger.info("http error", data={"status": exc.status, "detail": exc.detail})
        return _json_response(exc.status, {"error": exc.detail})
    except Exception as exc:  # noqa: BLE001 - no unhandled 5xx escapes to the gateway
        logger.error("handler failure", data={"error": str(exc)})
        return _json_response(500, {"error": "internal error"})
