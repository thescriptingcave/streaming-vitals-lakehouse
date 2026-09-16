"""Unit tests for L1 Alert Notifier (SNS publish + idempotent sent-guard)."""

from __future__ import annotations

from typing import Any

import lambdas.l1_alert_notifier.handler as l1
from lambdas.common import to_attrs

DDB_ALERT = {
    "patient_id": {"S": "P0001"},
    "alert_id": {"S": "a-1"},
    "rule": {"S": "SPO2_LOW"},
    "severity": {"S": "warning"},
    "value": {"N": "90.0"},
    "threshold": {"N": "92.0"},
    "event_ts": {"S": "2026-09-15T00:01:00Z"},
}


def ddb_stream_event(records: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "Records": [
            {
                "eventName": "INSERT",
                "eventSource": "aws:dynamodb",
                "dynamodb": {
                    "NewImage": record,
                    "Keys": {"patient_id": record["patient_id"], "alert_id": record["alert_id"]},
                },
            }
            for record in records
        ]
    }


class FakeSns:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def publish(self, **kwargs: Any) -> dict[str, str]:
        self.calls.append(kwargs)
        return {"MessageId": f"mid-{len(self.calls)}"}


class ConditionalError(Exception):
    def __init__(self) -> None:
        super().__init__("the conditional request failed")


class FakeDdb:
    def __init__(self, *, fail_sent_condition: bool = False) -> None:
        self.fail_sent_condition = fail_sent_condition
        self.updates: list[dict[str, Any]] = []

    def update_item(self, **kwargs: Any) -> dict[str, Any]:
        if self.fail_sent_condition:
            raise ConditionalError()
        self.updates.append(kwargs)
        return {"Attributes": {}}


def test_alert_from_ddb_event_extracts_payload(monkeypatch):
    monkeypatch.setattr(l1, "client", lambda service: object())
    alert = l1.alert_from_ddb_event(ddb_stream_event([DDB_ALERT]))[0]
    assert alert["patient_id"] == "P0001"
    assert alert["rule"] == "SPO2_LOW"


def test_handler_publishes_and_marks_sent(monkeypatch):
    sns = FakeSns()
    ddb = FakeDdb()
    monkeypatch.setattr(l1, "client", lambda service: sns if service == "sns" else ddb)
    monkeypatch.setattr(l1, "ALERTS_TABLE", "alerts")
    monkeypatch.setattr(l1, "ALERT_TOPIC_ARN", "arn:aws:sns:us-east-1:000000000000:vitals-alerts")

    result = l1.handler(ddb_stream_event([DDB_ALERT]))

    assert result["delivered"] == 1
    assert len(sns.calls) == 1
    assert sns.calls[0]["TopicArn"].endswith("vitals-alerts")
    assert ddb.updates[0]["UpdateExpression"].startswith("SET sent")
    assert ddb.updates[0]["ConditionExpression"] == "attribute_not_exists(sent) OR sent = :false"


def test_handler_counts_failures_and_keeps_going(monkeypatch):
    sns = FakeSns()
    ddb = FakeDdb(fail_sent_condition=True)
    monkeypatch.setattr(l1, "client", lambda service: sns if service == "sns" else ddb)
    result = l1.handler(ddb_stream_event([DDB_ALERT, DDB_ALERT]))
    assert result["delivered"] == 0
    assert len(result["failed"]) == 2


def test_mark_sent_uses_condition_expression(monkeypatch):
    ddb = FakeDdb()
    l1.mark_sent(ddb, {"patient_id": "P0001", "alert_id": "a-1"})
    assert ddb.updates[0]["Key"] == to_attrs({"patient_id": "P0001", "alert_id": "a-1"})


def test_publish_alert_carries_patient_context(monkeypatch):
    sns = FakeSns()
    l1.publish_alert(
        sns,
        "arn:topic",
        {
            "patient_id": "P0001",
            "rule": "SPO2_LOW",
            "severity": "warning",
            "value": "90.0",
            "threshold": "92.0",
            "event_ts": "t",
            "alert_id": "a-1",
        },
    )
    body = sns.calls[0]["Message"]
    assert '"patient_id": "P0001"' in body
    assert sns.calls[0]["Subject"] == "Vitals alert: SPO2_LOW"
