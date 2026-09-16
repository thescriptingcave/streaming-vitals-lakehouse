"""Streaming integration tests -> Floci (Kinesis, Firehose, Trino/Iceberg)."""

from __future__ import annotations

import json
import os
import time
import uuid

import pytest

from producer.vitals import generate_reading

pytestmark = pytest.mark.floci


def _unique(name: str) -> str:
    return f"{name}-{uuid.uuid4().hex[:8]}"


def _empty_bucket(s3, bucket: str) -> None:
    for _ in range(40):
        objects = s3.list_objects_v2(Bucket=bucket).get("Contents", [])
        if not objects:
            return
        s3.delete_objects(
            Bucket=bucket, Delete={"Objects": [{"Key": obj["Key"]} for obj in objects]}
        )


def _ensure_warehouse_bucket() -> None:
    """Create the Iceberg warehouse bucket on the compose Floci when wired up."""
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    if not endpoint:
        return
    import boto3
    from botocore.exceptions import ClientError

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        config=boto3.session.Config(s3={"addressing_style": "path"}),
    )
    try:
        s3.create_bucket(Bucket="healthcare-lake")
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
            raise


@pytest.mark.floci
def test_it_kinesis_put_and_get(aws_clients):
    kinesis = aws_clients["kinesis"]
    stream = _unique("it-vitals")
    kinesis.create_stream(StreamName=stream, ShardCount=1)
    try:
        for _ in range(20):
            status = kinesis.describe_stream_summary(StreamName=stream)
            if status["StreamDescriptionSummary"]["StreamStatus"] == "ACTIVE":
                break
            time.sleep(0.5)
        record = generate_reading("P0001", "bed-000", seed=1)
        put = kinesis.put_record(
            StreamName=stream, Data=json.dumps(record), PartitionKey=record["patient_id"]
        )
        expected_seq = put["SequenceNumber"]

        shard = kinesis.list_shards(StreamName=stream)["Shards"][0]["ShardId"]
        iterator = kinesis.get_shard_iterator(
            StreamName=stream, ShardId=shard, ShardIteratorType="TRIM_HORIZON"
        )["ShardIterator"]
        seen = None
        for _ in range(30):
            batch = kinesis.get_records(ShardIterator=iterator)
            iterator = batch["NextShardIterator"]
            for item in batch.get("Records", []):
                if item["SequenceNumber"] == expected_seq:
                    seen = json.loads(item["Data"])
                    break
            if seen:
                break
            time.sleep(0.5)
        assert seen is not None, "put record never became readable"
        assert seen["patient_id"] == "P0001"
    finally:
        kinesis.delete_stream(StreamName=stream, EnforceConsumerDeletion=True)


@pytest.mark.floci
def test_it_firehose_delivers_to_s3(aws_clients):
    firehose = aws_clients["firehose"]
    s3 = aws_clients["s3"]
    bucket = _unique("it-raw")
    stream = _unique("it-raw-stream")
    s3.create_bucket(Bucket=bucket)
    firehose.create_delivery_stream(
        DeliveryStreamName=stream,
        DeliveryStreamType="DirectPut",
        ExtendedS3DestinationConfiguration={
            "RoleARN": "arn:aws:iam::000000000000:role/firehose",
            "BucketARN": f"arn:aws:s3:::{bucket}",
            "Prefix": "raw/vitals/",
            "BufferingHints": {"IntervalInSeconds": 5, "SizeInMBs": 1},
        },
    )
    try:
        reading = generate_reading("P0001", "bed-000", seed=2)
        firehose.put_record_batch(
            DeliveryStreamName=stream,
            Records=[{"Data": json.dumps(reading).encode()}],
        )
        objects = []
        for _ in range(40):  # Floci flush interval drives S3 delivery
            objects = list(s3.list_objects_v2(Bucket=bucket).get("Contents", []))
            if objects:
                break
            time.sleep(1)
        assert objects, "firehose never delivered an object to S3"
        body = s3.get_object(Bucket=bucket, Key=objects[0]["Key"])["Body"].read()
        assert b'"patient_id": "P0001"' in body
    finally:
        firehose.delete_delivery_stream(DeliveryStreamName=stream)
        _empty_bucket(s3, bucket)
        s3.delete_bucket(Bucket=bucket)


@pytest.mark.floci
def test_it_trino_iceberg_roundtrip():
    """CREATE -> INSERT -> SELECT through the Trino Iceberg (nessie) catalog.

    Requires the compose stack (Trino + Nessie + Floci) reachable via env.
    """
    host = os.environ.get("TRINO_HOST")
    port = os.environ.get("TRINO_PORT")
    if not host or not port:
        pytest.skip("TRINO_HOST/TRINO_PORT not set (compose stack offline)")
    import trino

    _ensure_warehouse_bucket()
    conn = trino.dbapi.connect(
        host=host, port=int(port), user="trino", catalog="iceberg", schema="healthcare"
    )
    schema = "it_" + uuid.uuid4().hex[:6]
    table = f"vitals_rt_{uuid.uuid4().hex[:6]}"
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA " + schema)
    try:
        cur.execute(
            f"CREATE TABLE {schema}.{table} "
            '(patient_id VARCHAR, heart_rate DOUBLE, "event_time" TIMESTAMP(3)) '
            "WITH (format = 'PARQUET')"
        )
        cur.execute(
            f"INSERT INTO {schema}.{table} VALUES ('P0001', 82.0, TIMESTAMP '2026-09-15 00:00:00')"
        )
        cur.execute(f"SELECT COUNT(*), AVG(heart_rate) FROM {schema}.{table}")
        assert [tuple(row) for row in cur.fetchall()] == [(1, 82.0)]
    finally:
        cur.execute("DROP SCHEMA " + schema + " CASCADE")


@pytest.mark.floci
def test_it_window_iceberg_sink_rows():
    """1-min Flink windows land queryable rows in Iceberg (needs a live job).

    Skips unless FLINK_E2E=1 to keep the stream integration stage decoupled
    from Flink provisioning. With FLINK_E2E=1 the Flink job (flink/sql/job.sql)
    must have run at least once so healthcare.vitals_1m holds windows.
    """
    if os.environ.get("FLINK_E2E") != "1":
        pytest.skip("set FLINK_E2E=1 to run the Flink window pipeline end-to-end")
    host = os.environ.get("TRINO_HOST")
    port = os.environ.get("TRINO_PORT")
    if not host or not port:
        pytest.skip("TRINO_HOST/TRINO_PORT not set (compose stack offline)")
    import trino

    conn = trino.dbapi.connect(
        host=host, port=int(port), user="trino", catalog="iceberg", schema="healthcare"
    )
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*), COALESCE(SUM(reading_count), 0) FROM iceberg.healthcare.vitals_1m"
    )
    rows, readings = cur.fetchall()[0]
    assert rows > 0 and readings > 0
