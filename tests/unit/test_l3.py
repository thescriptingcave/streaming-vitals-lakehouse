"""Unit tests for L3 Patients API (APIGW v2 payload 2.0 -> DDB)."""

from __future__ import annotations

from typing import Any

import lambdas.l3_patients_api.handler as l3
from lambdas.common import from_attrs, to_attrs


def apigw_event(path: str, query: dict[str, str] | None = None) -> dict[str, Any]:
    patient = path.split("/")[2] if "/patients/" in path else "P0001"
    return {
        "version": "2.0",
        "rawPath": path,
        "pathParameters": {"id": patient},
        "queryStringParameters": query or {},
        "requestContext": {"http": {"method": "GET"}},
    }


class FakeDdb:
    def __init__(self) -> None:
        self.items: dict[str, dict[str, Any]] = {}
        self.queries: list[dict[str, Any]] = []

    def get_item(self, **kwargs: Any) -> dict[str, Any]:
        key = from_attrs(kwargs["Key"])
        return {"Item": self.items.get(key["patient_id"])}

    def query(self, **kwargs: Any) -> dict[str, Any]:
        self.queries.append(kwargs)
        value = from_attrs(kwargs["ExpressionAttributeValues"])[":p"]
        rows = [item for item in self.items.values() if from_attrs(item)["patient_id"] == value][
            : kwargs.get("Limit", 10)
        ]
        return {"Items": rows}


def test_parse_request_route_kinds():
    assert l3.parse_request(apigw_event("/patients/P0001/latest-vitals")) == ("P0001", "latest", 10)
    assert l3.parse_request(apigw_event("/patients/P0001/alerts", {"limit": "5"})) == (
        "P0001",
        "alerts",
        5,
    )


def test_parse_request_clamps_limit():
    _, _, limit = l3.parse_request(apigw_event("/patients/P0001/alerts", {"limit": "1000"}))
    assert limit == l3.MAX_LIMIT


def test_handler_latest_returns_item(monkeypatch):
    ddb = FakeDdb()
    ddb.items["P0001"] = to_attrs({"patient_id": "P0001", "heart_rate": 82, "spo2": 98})
    monkeypatch.setattr(l3, "client", lambda service: ddb)
    resp = l3.handler(apigw_event("/patients/P0001/latest-vitals"))
    assert resp["statusCode"] == 200
    assert resp["headers"]["Cache-Control"] == "no-store"
    assert '"patient_id": "P0001"' in resp["body"]


def test_handler_latest_404_for_unknown(monkeypatch):
    monkeypatch.setattr(l3, "client", lambda service: FakeDdb())
    resp = l3.handler(apigw_event("/patients/P9999/latest-vitals"))
    assert resp["statusCode"] == 404


def test_handler_rejects_bad_patient_id(monkeypatch):
    monkeypatch.setattr(l3, "client", lambda service: FakeDdb())
    event = apigw_event("/patients/P0001/latest-vitals")
    event["pathParameters"] = {"id": "bad id!"}
    resp = l3.handler(event)
    assert resp["statusCode"] == 400
    assert "invalid patient_id" in resp["body"]


def test_handler_alerts_scans_backwards(monkeypatch):
    ddb = FakeDdb()
    for i in range(3):
        ddb.items[f"P0001-{i}"] = to_attrs({"patient_id": "P0001", "alert_id": f"a-{i}"})
    monkeypatch.setattr(l3, "client", lambda service: ddb)
    resp = l3.handler(apigw_event("/patients/P0001/alerts"))
    assert resp["statusCode"] == 200
    assert ddb.queries[0]["ScanIndexForward"] is False


def test_handler_500_on_client_error(monkeypatch):
    def boom(service: str) -> None:
        raise RuntimeError("network down")

    monkeypatch.setattr(l3, "client", boom)
    resp = l3.handler(apigw_event("/patients/P0001/latest-vitals"))
    assert resp["statusCode"] == 500


def test_env_override_for_table_names(monkeypatch):
    ddb = FakeDdb()
    ddb.items["P0001"] = to_attrs({"patient_id": "P0001"})
    monkeypatch.setattr(l3, "client", lambda service: ddb)
    monkeypatch.setenv("LATEST_VITALS_TABLE", "custom-table")
    resp = l3.handler(apigw_event("/patients/P0001/latest-vitals"))
    assert resp["statusCode"] == 200
