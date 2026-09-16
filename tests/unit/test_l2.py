"""Unit tests for L2 Firehose transform (Ok / Dropped / no record leakage)."""

from __future__ import annotations

import base64
import json
from typing import Any

import lambdas.l2_firehose_transform.handler as l2
from producer.vitals import generate_reading, normalize


def firehose_event(payloads: list[dict[str, object]]) -> dict[str, Any]:
    return {
        "records": [
            {
                "recordId": f"rec-{idx}",
                "data": base64.b64encode(json.dumps(payload).encode()).decode(),
            }
            for idx, payload in enumerate(payloads)
        ]
    }


def test_handler_returns_ok_with_normalized_payload():
    reading = generate_reading("P0001", "bed-001", seed=1)
    result = l2.handler(firehose_event([reading]))
    assert len(result["records"]) == 1
    record = result["records"][0]
    assert record["result"] == "Ok"
    normalized = json.loads(base64.b64decode(record["data"]))
    assert normalized["patient_id"] == "P0001"
    assert "abnormal_flags" in normalized


def test_handler_drops_unparseable_json():
    event = {
        "records": [{"recordId": "rec-0", "data": base64.b64encode(b"####not-json####").decode()}]
    }
    result = l2.handler(event)
    assert result["records"][0]["result"] == "Dropped"


def test_handler_drops_missing_fields():
    result = l2.handler(firehose_event([{"not_vitals": True}]))
    assert result["records"][0]["result"] == "Dropped"


def test_handler_one_response_per_input_record():
    reading = generate_reading("P0001", "bed-001", seed=1)
    event = firehose_event([reading, {"broken": 1}, reading])
    result = l2.handler(event)
    assert len(result["records"]) == 3
    assert [r["result"] for r in result["records"]] == ["Ok", "Dropped", "Ok"]


def test_normalize_matches_generated_reading():
    reading = generate_reading("P0001", "bed-001", abnormal=True, seed=2)
    base_norm = normalize(reading)
    assert base_norm["is_abnormal"] is True
