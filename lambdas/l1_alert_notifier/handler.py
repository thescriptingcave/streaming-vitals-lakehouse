"""L1 Alert Notifier — DynamoDB Streams -> SNS.

Pattern learned: event-driven + stream consumption, retries/DLQ, idempotent
delivery. Flink writes alert items to DynamoDB; the stream triggers this
function; each item is published to SNS exactly once (guarded by `sent`).

Trigger contract: synch trigger, batch window ~1s/100 items,
BisectBatchOnErrorEnabled=true, DLQ on retry exhaustion.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

from lambdas.common import alert_from_ddb_event, client, get_logger, to_attrs

logger = get_logger(__name__)

ALERTS_TABLE = "alerts"
ALERT_TOPIC_ARN = ""


def _env(name: str) -> str:
    return os.environ[name]


def _delivery_ts() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


def publish_alert(sns: Any, topic_arn: str, alert: dict[str, Any]) -> str:
    """Publish one alert item to SNS; returns the MessageId."""
    message = {
        "patient_id": alert.get("patient_id"),
        "alert_id": alert.get("alert_id"),
        "rule": alert.get("rule"),
        "severity": alert.get("severity"),
        "value": alert.get("value"),
        "threshold": alert.get("threshold"),
        "event_ts": alert.get("event_ts"),
    }
    resp = sns.publish(
        TopicArn=topic_arn,
        Subject=f"Vitals alert: {alert.get('rule')}",
        Message=_json(message),
    )
    return str(resp["MessageId"])


def mark_sent(ddb: Any, alert: dict[str, Any]) -> None:
    """Flip `sent` to true under a condition so retried batches stay idempotent."""
    ddb.update_item(
        TableName=os.environ.get("ALERTS_TABLE", ALERTS_TABLE),
        Key=to_attrs(
            {"patient_id": alert.get("patient_id", ""), "alert_id": alert.get("alert_id", "")}
        ),
        UpdateExpression="SET sent = :true, delivery_ts = :ts",
        ConditionExpression="attribute_not_exists(sent) OR sent = :false",
        ExpressionAttributeValues=to_attrs({":true": True, ":false": False, ":ts": _delivery_ts()}),
    )


def handler(event: dict[str, Any]) -> dict[str, Any]:
    topic_arn = os.environ.get("ALERT_TOPIC_ARN", ALERT_TOPIC_ARN)
    table = os.environ.get("ALERTS_TABLE", ALERTS_TABLE)
    sns = client("sns")
    ddb = client("dynamodb")

    delivered = 0
    failed: list[str] = []
    for alert in alert_from_ddb_event(event):
        try:
            message_id = publish_alert(sns, topic_arn, alert)
            mark_sent(ddb, alert)
            delivered += 1
            logger.info(
                "alert delivered",
                data={"alert_id": alert.get("alert_id"), "sns_message_id": message_id},
            )
        except Exception as exc:  # noqa: BLE001 - batch isolation is a Lambda concern
            alert_id = alert.get("alert_id", "?")
            failed.append(alert_id)
            logger.error("alert delivery failed", data={"alert_id": alert_id, "error": str(exc)})
    return {"delivered": delivered, "failed": failed, "table": table}


def _json(value: dict[str, Any]) -> str:
    import json

    return json.dumps(value, default=str)
