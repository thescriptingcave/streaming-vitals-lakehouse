"""DynamoDB AttributeValue <-> plain Python conversions.

boto3 applies payload validation to every call, so `Item`/`Key`/expression
values passed to put_item/get_item/query must be AttributeValue shapes
(e.g. {"S": ...}, {"N": ...}, {"BOOL": ...}). These wrappers keep the handlers
readable while staying compatible with real DynamoDB (and Floci).
"""

from __future__ import annotations

from typing import Any

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

_serializer = TypeSerializer()
_deserializer = TypeDeserializer()


def to_attrs(item: dict[str, Any]) -> dict[str, Any]:
    """Serialize a plain Python dict into DynamoDB AttributeValue shape."""
    return {key: _serializer.serialize(value) for key, value in item.items()}


def from_attrs(attributes: dict[str, Any]) -> dict[str, Any]:
    """Deserialize an AttributeValue dict back into plain Python values."""
    return {key: _deserializer.deserialize(value) for key, value in attributes.items()}
