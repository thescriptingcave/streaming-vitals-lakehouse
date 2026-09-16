"""Boto3 client factory that honours the Floci endpoint override.

Everything reaches the emulated AWS through AWS_ENDPOINT_URL (set to
http://localhost:4566 on the host and http://floci:4566 inside Lambda
sidecars). Credentials are dummy values; any non-empty pair works on Floci,
and the same factory is portable to real AWS by unsetting AWS_ENDPOINT_URL.
"""

from __future__ import annotations

import os
from typing import Any

import boto3
from botocore.config import Config


def client(service: str, *, region: str | None = None, endpoint_url: str | None = None) -> Any:
    """Return a boto3 client with the configured endpoint/region applied."""
    resolved_region = region or os.environ.get("AWS_REGION", "us-east-1")
    resolved_endpoint = endpoint_url or os.environ.get("AWS_ENDPOINT_URL")
    kwargs: dict[str, Any] = {
        "region_name": resolved_region,
        "config": Config(retries={"max_attempts": 3, "mode": "standard"}),
    }
    if resolved_endpoint:
        kwargs["endpoint_url"] = resolved_endpoint
    return boto3.client(service, **kwargs)
