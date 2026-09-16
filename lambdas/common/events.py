"""Parsers for the event shapes L1-L4 receive.

- DynamoDB Streams: binary/image shapes with S/N/BOOL/L/M attribute types.
- Firehose processing: {"records": [{"recordId", "data" (base64)}]}.
Each handler relies on these so the plumbing is tested once.
"""

from __future__ import annotations

import base64
import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable

FirehoseAction = tuple[str, str, str]  # recordId, result, data(base64)


def _img(attributes: dict[str, Any]) -> dict[str, Any]:
    """Decode a DynamoDB attribute map into plain Python values."""
    if not isinstance(attributes, dict):
        return {}
    out: dict[str, Any] = {}
    for key, value in attributes.items():
        if not isinstance(value, dict) or len(value) != 1:
            out[key] = value
            continue
        (kind, raw), *_ = value.items()
        if kind == "S":
            out[key] = str(raw)
        elif kind == "N":
            out[key] = float(raw) if any(c in str(raw) for c in ".Ee") else int(raw)
        elif kind == "BOOL":
            out[key] = bool(raw)
        elif kind in {"L", "SS", "NS", "BS"}:
            out[key] = list(raw) if isinstance(raw, list) else list(raw)
        else:
            out[key] = raw
    return out


def ddb_records(event: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Yield decoded INSERT/MODIFY records with their keys and new-image."""
    for record in event.get("Records", []):
        if record.get("eventName") not in {"INSERT", "MODIFY"}:
            continue
        dynamodb = record.get("dynamodb", {})
        keys = _img(dynamodb.get("Keys", {}))
        new_image = _img(dynamodb.get("NewImage", {}))
        yield {"keys": keys, "new_image": new_image, "event_name": record["eventName"]}


def alert_from_ddb_event(event: dict[str, Any]) -> list[dict[str, Any]]:
    """Return only records that are unresolved alerts."""
    alerts: list[dict[str, Any]] = []
    for rec in ddb_records(event):
        image = rec["new_image"]
        # Only act on rows that still need notification (idempotent `sent` guard).
        # Missing `sent` == new alert; explicit false == retried delivery.
        sent = image.get("sent")
        pending = sent is None or sent is False or str(sent).lower() == "false"
        if pending:
            alerts.append(image)
    return alerts


def firehose_records(event: dict[str, Any]) -> list[tuple[str, str]]:
    """Return (recordId, decoded-text) pairs from a Firehose processing event."""
    pairs: list[tuple[str, str]] = []
    for item in event.get("records", []):
        record_id = item.get("recordId", "")
        payload = base64.b64decode(item.get("data", "") or "").decode("utf-8", errors="replace")
        pairs.append((record_id, payload))
    return pairs


def encode_payload(obj: dict[str, Any]) -> str:
    """Base64-encode a JSON object for a Firehose response record."""
    return base64.b64encode(json.dumps(obj, default=str).encode("utf-8")).decode("ascii")
