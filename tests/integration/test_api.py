"""Patients API integration: APIGW-style event -> DynamoDB read model (L3)."""

from __future__ import annotations

import uuid
from decimal import Decimal

import pytest

import lambdas.l3_patients_api.handler as l3
from lambdas.common import to_attrs
from producer.vitals import generate_reading, normalize

pytestmark = pytest.mark.floci


@pytest.mark.floci
def test_it_api_latest_vitals(aws_clients, monkeypatch):
    dynamodb = aws_clients["dynamodb"]
    table = "it-latest-vitals-" + uuid.uuid4().hex[:6]
    dynamodb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "patient_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "patient_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    dynamodb.get_waiter("table_exists").wait(TableName=table)
    monkeypatch.setenv("LATEST_VITALS_TABLE", table)
    monkeypatch.setattr(l3, "client", lambda service: dynamodb)

    reading = normalize(generate_reading("P0001", "bed-000", abnormal=True, seed=2))
    dynamodb.put_item(
        TableName=table,
        Item=to_attrs(
            {
                "patient_id": "P0001",
                "heart_rate": Decimal(str(reading["heart_rate"])),
                "spo2": Decimal(str(reading["spo2"])),
            }
        ),
    )

    event = {
        "rawPath": "/patients/P0001/latest-vitals",
        "pathParameters": {"id": "P0001"},
        "queryStringParameters": {},
    }
    resp = l3.handler(event)
    assert resp["statusCode"] == 200
    assert '"patient_id": "P0001"' in resp["body"]
    assert resp["headers"]["Cache-Control"] == "no-store"


@pytest.mark.floci
def test_it_api_rejects_unknown_patient(aws_clients, monkeypatch):
    dynamodb = aws_clients["dynamodb"]
    table = "it-latest-vitals-" + uuid.uuid4().hex[:6]
    dynamodb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "patient_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "patient_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    dynamodb.get_waiter("table_exists").wait(TableName=table)
    monkeypatch.setenv("LATEST_VITALS_TABLE", table)
    monkeypatch.setattr(l3, "client", lambda service: dynamodb)

    event = {
        "rawPath": "/patients/P9999/latest-vitals",
        "pathParameters": {"id": "P9999"},
        "queryStringParameters": {},
    }
    resp = l3.handler(event)
    assert resp["statusCode"] == 404
