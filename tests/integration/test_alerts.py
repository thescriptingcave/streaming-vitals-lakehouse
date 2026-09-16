"""Alerting integration: DynamoDB stream event -> SNS publish (L1 contract)."""

from __future__ import annotations

import datetime
import os
import uuid

import pytest

import lambdas.l1_alert_notifier.handler as l1
from lambdas.common import from_attrs, to_attrs
from producer.vitals import generate_reading, normalize

pytestmark = pytest.mark.floci


def _create_alerts_table(dynamodb, table_name: str) -> None:
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "patient_id", "KeyType": "HASH"},
            {"AttributeName": "alert_id", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "patient_id", "AttributeType": "S"},
            {"AttributeName": "alert_id", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    waiter = dynamodb.get_waiter("table_exists")
    waiter.wait(TableName=table_name)


@pytest.mark.floci
def test_it_alerts_ddb_event_to_sns(aws_clients, monkeypatch):
    dynamodb = aws_clients["dynamodb"]
    sns = aws_clients["sns"]
    topic_arn = sns.create_topic(Name="it-alerts-" + uuid.uuid4().hex[:6])["TopicArn"]
    table = "it-alerts-" + uuid.uuid4().hex[:6]
    # L1 requires a configured topic + table via env (its module defaults apply).
    monkeypatch.setitem(os.environ, "ALERTS_TABLE", table)
    monkeypatch.setitem(os.environ, "ALERT_TOPIC_ARN", topic_arn)

    _create_alerts_table(dynamodb, table)
    monkeypatch.setattr(l1, "client", lambda service: sns if service == "sns" else dynamodb)

    reading = normalize(generate_reading("P0001", "bed-000", abnormal=True, seed=1))
    alert = {
        "patient_id": "P0001",
        "alert_id": str(uuid.uuid4()),
        "rule": "HR_HIGH",
        "severity": "warning",
        "value": str(reading["heart_rate"]),
        "event_ts": datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds"),
        "sent": False,
    }
    dynamodb.put_item(TableName=table, Item=to_attrs(alert))

    # Fabricate the DDB Streams event Lambda would consume.
    event = {
        "Records": [
            {
                "eventName": "INSERT",
                "eventSource": "aws:dynamodb",
                "dynamodb": {
                    "NewImage": {
                        "patient_id": {"S": alert["patient_id"]},
                        "alert_id": {"S": alert["alert_id"]},
                    }
                },
            }
        ]
    }

    result = l1.handler(event)
    assert result["delivered"] == 1

    stored = from_attrs(
        dynamodb.get_item(
            TableName=table,
            Key=to_attrs({"patient_id": "P0001", "alert_id": alert["alert_id"]}),
        )["Item"]
    )
    assert stored["sent"] is True
    assert "delivery_ts" in stored
