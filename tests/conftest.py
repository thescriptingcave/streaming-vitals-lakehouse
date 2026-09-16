"""Shared pytest fixtures.

Unit tests (default: `-m "not e2e"`) never start Docker. Floci-backed
integration tests opt in with `-m floci` (CI integration stage runs them
against the pre-started compose stack via AWS_ENDPOINT_URL).
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import boto3
import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator

FLOCI_IMAGE = "floci/floci:2.1.0"


def _client_kwargs(endpoint: str, region: str, access_key: str, secret_key: str) -> dict[str, str]:
    return dict(
        endpoint_url=endpoint,
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=boto3.session.Config(s3={"addressing_style": "path"}),
    )


@pytest.fixture(scope="session")
def aws_clients() -> Iterator[dict[str, object]]:
    """boto3 clients at the Floci endpoint.

    CI mode: reuse the running compose stack via AWS_ENDPOINT_URL env.
    Local mode: spin a dedicated testcontainers-floci container.
    """
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    if not endpoint:
        try:
            from floci import FlociContainer
        except ImportError as exc:  # pragma: no cover - dev extra missing
            pytest.skip(f"testcontainers-floci not installed: {exc}")
        with FlociContainer(image=FLOCI_IMAGE) as container:
            endpoint = container.get_endpoint()
            kwargs = _client_kwargs(
                endpoint,
                container.get_region(),
                container.get_access_key(),
                container.get_secret_key(),
            )
            yield _build_clients(kwargs)
        return
    kwargs = _client_kwargs(
        endpoint,
        os.environ.get("AWS_REGION", "us-east-1"),
        os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
    )
    yield _build_clients(kwargs)


def _build_clients(kwargs: dict[str, str]) -> dict[str, object]:
    return {
        "kinesis": boto3.client("kinesis", **kwargs),
        "dynamodb": boto3.client("dynamodb", **kwargs),
        "s3": boto3.client("s3", **kwargs),
        "sns": boto3.client("sns", **kwargs),
        "firehose": boto3.client("firehose", **kwargs),
        "lambda": boto3.client("lambda", **kwargs),
    }


@pytest.fixture
def vitals_batch():
    """Deterministic batch of vitals records for producer/L2 tests."""
    from producer.vitals import generate_reading

    return [
        generate_reading(f"P{i:04d}", f"bed-{i:03d}", abnormal=(i % 2 == 1), seed=i)
        for i in range(1, 5)
    ]


def pytest_collection_modifyitems(config, items) -> None:  # type: ignore[no-untyped-def]
    """Auto-tag everything under tests/unit as `unit` (no external deps)."""
    for item in items:
        if "tests/unit" in str(item.fspath):
            item.add_marker(pytest.mark.unit)
