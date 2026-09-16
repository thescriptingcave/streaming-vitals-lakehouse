"""ML-ready Parquet export from the Iceberg lake (shared by L4 + local jobs).

DATA_MODEL.md §6: `vitals_features` wide table, one row per patient x window,
label `is_anomaly` and a train/val/test `split` assigned BY PATIENT (never by
row) to prevent leakage. Exports land under s3://healthcare-lake/ml/exports/
with an atomic manifest in ml/manifests/.

Real Trino/Iceberg execution lands in M4/M6; the split logic and manifest
writer are pure and unit-tested now.
"""

from __future__ import annotations

import json
import os
import random
from datetime import UTC, datetime
from typing import Any

from lambdas.common import client, get_logger

logger = get_logger(__name__)

ML_BUCKET = "healthcare-lake"
SPLIT_RATIOS = (0.7, 0.15, 0.15)  # train, val, test


def split_by_patient(
    patient_ids: list[str], *, ratios: tuple[float, float, float] = SPLIT_RATIOS, seed: int = 42
) -> dict[str, str]:
    """Assign every patient to a deterministic split. No per-row leakage."""
    total = len(patient_ids)
    if total == 0:
        return {}
    ordered = sorted(set(patient_ids))
    # Deterministic shuffle independent of insertion order.
    rng = random.Random(seed)
    rng.shuffle(ordered)
    train_end = int(total * ratios[0])
    val_end = train_end + int(total * ratios[1])
    mapping: dict[str, str] = {}
    for idx, pid in enumerate(ordered):
        if idx < train_end:
            mapping[pid] = "train"
        elif idx < val_end:
            mapping[pid] = "val"
        else:
            mapping[pid] = "test"
    return mapping


def write_manifest(s3: Any, *, run_date: str, mapping: dict[str, str], feature_count: int) -> str:
    """Write the split manifest atomically (.tmp then rename-in-place)."""
    payload = {
        "run_date": run_date,
        "created_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "feature_count": feature_count,
        "split_by": "patient",
        "counts": {k: sum(1 for v in mapping.values() if v == k) for k in ("train", "val", "test")},
    }
    bucket = os.environ.get("ML_BUCKET", ML_BUCKET)
    tmp_key = f"ml/manifests/vitals_features.{run_date}.parquet.manifest.tmp"
    final_key = f"ml/manifests/vitals_features.{run_date}.parquet.manifest.json"
    body = json.dumps(payload, indent=2).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=tmp_key, Body=body)
    s3.copy_object(Bucket=bucket, Key=final_key, CopySource={"Bucket": bucket, "Key": tmp_key})
    s3.delete_object(Bucket=bucket, Key=tmp_key)
    logger.info("manifest written", data={"key": final_key})
    return final_key


def run_ml_export(
    *, run_date: str | None = None, patient_ids: list[str] | None = None
) -> dict[str, Any]:
    """Entry point used by L4 and local `ml` tooling.

    For the scaffold this builds the split + manifest; the Iceberg -> Parquet
    wide-table write (Trino UNLOAD / DuckDB read) is wired in M6.
    """
    run_date = run_date or datetime.now(UTC).strftime("%Y-%m-%d")
    if patient_ids is None:
        # M6: read distinct patient_id from vitals via Trino. Placeholder safe
        # set keeps the lock/export flow testable end-to-end on Floci.
        patient_ids = [f"P{i:04d}" for i in range(1, 201)]
    mapping = split_by_patient(patient_ids)
    s3 = client("s3")
    final_key = write_manifest(
        s3, run_date=run_date, mapping=mapping, feature_count=len(patient_ids)
    )
    return {"run_date": run_date, "manifest": final_key, "patients": len(mapping)}
