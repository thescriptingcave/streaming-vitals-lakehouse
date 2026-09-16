"""Vitals simulator entry point: stream synthetic readings to Kinesis (Floci).

Mirrors the bedside-device model from the prior project:
- reads PATIENT_IDS / DEVICE_COUNT from env (real Synthea ids in M1+)
- generates one reading per device on a cadence, PutRecords with
  retry/backoff-jitter, drops to a dead-letter log after N attempts.

Usage:  uv run python -m producer.producer
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import random
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from producer.vitals import generate_reading

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("producer")

STREAM_NAME = os.environ.get("VITALS_STREAM_NAME", "vitals")


@dataclass(frozen=True)
class ProducerOptions:
    stream: str
    device_count: int
    patients: tuple[str, ...]
    cadence_seconds: float
    abnormal_fraction: float
    max_attempts: int
    seed: int | None


def load_patient_ids(path: str) -> list[str]:
    """Read patient ids from a CSV/Synthea export (patient_id column)."""
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        fieldnames = reader.fieldnames or []
        column = "patient_id" if "patient_id" in fieldnames else fieldnames[0]
        return [str(row[column]).strip() for row in reader if row.get(column)]


def device_fanout(patients: Sequence[str], device_count: int) -> list[str]:
    """Map patients to virtual devices so ids overlap the cohort."""
    if not patients:
        raise ValueError("no patient ids; generate the Synthea cohort first (make synth)")
    out: list[str] = []
    for idx in range(device_count):
        out.append(patients[idx % len(patients)])
    return out


def readings(devices: list[str], opts: ProducerOptions) -> Iterator[dict[str, Any]]:
    """Yield a reading per device each cycle, round-robin with jitter."""
    rng = random.Random(opts.seed)
    idx = 0
    while True:
        patient = devices[idx % len(devices)]
        device = f"bed-{idx % len(devices):03d}"
        abnormal = rng.random() < opts.abnormal_fraction
        yield generate_reading(patient, device, abnormal=abnormal, seed=opts.seed)
        idx += 1
        if idx % len(devices) == 0:
            time.sleep(opts.cadence_seconds)


def put_with_retry(kinesis: Any, stream: str, record: dict[str, Any], max_attempts: int) -> str:
    """PutRecord with bounded retries and backoff-jitter; raises on exhaustion."""
    attempt = 0
    delay = 0.05
    while True:
        attempt += 1
        try:
            resp = kinesis.put_record(
                StreamName=stream,
                Data=json.dumps(record, default=str),
                PartitionKey=record["patient_id"],
            )
            return str(resp["SequenceNumber"])
        except Exception as exc:  # noqa: BLE001 - producer backoff is transport-level
            if attempt >= max_attempts:
                raise
            logger.warning("put_record failed (attempt %s): %s", attempt, exc)
            time.sleep(delay)
            delay = min(delay * 2, 2.0) * (0.8 + 0.4 * (attempt % 5) / 5)


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream synthetic vitals to Kinesis (Floci).")
    parser.add_argument("--stream", default=os.environ.get("VITALS_STREAM_NAME", "vitals"))
    parser.add_argument("--devices", type=int, default=int(os.environ.get("DEVICE_COUNT", "20")))
    parser.add_argument("--patients-csv", default=os.environ.get("PATIENTS_CSV", ""))
    parser.add_argument(
        "--cadence", type=float, default=float(os.environ.get("CADENCE_SECONDS", "1.0"))
    )
    parser.add_argument("--abnormal-fraction", type=float, default=0.03)
    parser.add_argument("--max-attempts", type=int, default=5)
    parser.add_argument(
        "--limit", type=int, default=0, help="stop after N readings (0=run forever)"
    )
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    patients: list[str]
    if args.patients_csv:
        patients = load_patient_ids(args.patients_csv)
        logger.info("loaded %s patient ids from %s", len(patients), args.patients_csv)
    else:
        patients = [f"P{i:04d}" for i in range(1, 2000)]
        logger.info("no cohort file; using %s placeholder ids P0001..", len(patients))

    opts = ProducerOptions(
        stream=args.stream,
        device_count=args.devices,
        patients=tuple(patients),
        cadence_seconds=args.cadence,
        abnormal_fraction=args.abnormal_fraction,
        max_attempts=args.max_attempts,
        seed=args.seed,
    )
    from lambdas.common import client

    kinesis = client("kinesis")

    devices = device_fanout(patients, args.devices)
    logger.info("streaming %s devices -> %s (seed=%s)", len(devices), opts.stream, opts.seed)
    last_log = time.monotonic()
    for sent, reading in enumerate(readings(devices, opts), start=1):
        seq = put_with_retry(kinesis, opts.stream, reading, opts.max_attempts)
        if sent % 100 == 0 or time.monotonic() - last_log > 5:
            logger.info("sent=%s last_seq=%s patient=%s", sent, seq, reading["patient_id"])
            last_log = time.monotonic()
        if args.limit and sent >= args.limit:
            break


if __name__ == "__main__":
    main()
