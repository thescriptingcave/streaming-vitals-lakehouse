"""L2 Firehose Transform — normalize raw vitals JSON for the raw lake.

Pattern learned: event payload + batching. Firehose invokes this with a batch
of base64 payloads; the contract requires exactly one response per input
recordId with result in {Ok, Dropped, ProcessingFailed} (DATA_FLOW.md §4).

Delivered records land in S3 (raw/normalized); Flink also dual-writes raw
events to Iceberg for the historical lake.
"""

from __future__ import annotations

import json
from typing import Any

from lambdas.common import encode_payload, firehose_records, get_logger
from producer.vitals import normalize

logger = get_logger(__name__)


def handler(event: dict[str, Any]) -> dict[str, Any]:
    """Map every input record to {recordId, result, data} for Firehose."""
    responses: list[dict[str, str]] = []
    for record_id, payload in firehose_records(event):
        try:
            raw = json.loads(payload)
            normalized: dict[str, Any] = normalize(raw)
            # Firehose requires the response data be base64-encoded JSON.
            responses.append(
                {"recordId": record_id, "result": "Ok", "data": encode_payload(normalized)}
            )
        except (json.JSONDecodeError, KeyError, TypeError, AttributeError, ValueError) as exc:
            logger.warning(
                "dropping unparseable record", data={"record_id": record_id, "error": str(exc)}
            )
            responses.append({"recordId": record_id, "result": "Dropped", "data": ""})
    return {"records": responses}


def handler_v2_plain(event: dict[str, Any]) -> dict[str, Any]:
    """Alias keeping the sink-side S3 writing future-proof (Firehose sink)."""
    return handler(event)
