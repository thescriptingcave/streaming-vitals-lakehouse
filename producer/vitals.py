"""Synthetic real-time vitals generation for simulated bedside devices."""

from __future__ import annotations

import math
import random
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

# Physiological bounds (per reading); used to roll abnormal values and flags.
HR_MIN, HR_MAX = 30.0, 220.0
SYS_MIN, SYS_MAX = 60.0, 240.0
DIA_MIN, DIA_MAX = 40.0, 160.0
SPO2_MIN, SPO2_MAX = 70.0, 100.0
TEMP_MIN, TEMP_MAX = 32.0, 42.0
RESP_MIN, RESP_MAX = 6.0, 60.0

ABNORMAL_RULES: dict[str, tuple[float, float]] = {
    "HR_HIGH": (120.0, HR_MAX),
    "CRITICAL_HR": (180.0, HR_MAX),
    "SPO2_LOW": (SPO2_MIN, 92.0),
    "BP_HIGH": (140.0, SYS_MAX),
}


def _rng(seed: int | None) -> random.Random:
    if seed is None:
        return random.Random()
    return random.Random(seed)


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _round3(value: float) -> float:
    return round(value, 3)


def generate_reading(
    patient_id: str,
    device_id: str,
    *,
    event_ts: datetime | None = None,
    abnormal: bool = False,
    seed: int | None = None,
    trace_id: str | None = None,
) -> dict[str, Any]:
    """Build one vitals reading (heart rate, BP, SpO2, temperature, resp)."""
    rng = _rng(seed)
    if abnormal:
        hr = rng.choice([128.0, 150.0, 132.5])
        spo2 = rng.choice([91.0, 88.0, 84.0])
        systolic = rng.choice([142.0, 151.0, 138.0])
        diastolic = systolic - rng.choice([38.0, 42.0])
    else:
        hr = round(rng.gauss(82, 12), 2)
        spo2 = round(rng.gauss(98, 0.8), 2)
        systolic = round(rng.gauss(118, 10), 2)
        diastolic = round(rng.gauss(76, 7), 2)

    reading = {
        "patient_id": patient_id,
        "device_id": device_id,
        "event_time": (event_ts or datetime.now(UTC)).isoformat(timespec="milliseconds"),
        "heart_rate": _clamp(hr, HR_MIN, HR_MAX),
        "systolic_bp": _clamp(systolic, SYS_MIN, SYS_MAX),
        "diastolic_bp": _clamp(diastolic, DIA_MIN, DIA_MAX),
        "spo2": _clamp(spo2, SPO2_MIN, SPO2_MAX),
        "temperature": _round3(rng.gauss(37.0, 0.4) if not abnormal else 38.4),
        "resp_rate": _clamp(
            round(rng.gauss(16, 3), 2) if not abnormal else 26.0, RESP_MIN, RESP_MAX
        ),
    }
    if trace_id is None:
        trace_id = str(uuid4())
    reading["trace_id"] = trace_id
    return reading


def abnormal_flags(reading: dict[str, Any]) -> list[str]:
    """Derive flags the raw-normalized row records (DATA_MODEL.md §3)."""
    flags: list[str] = []
    values: dict[str, float] = {
        "HR_HIGH": float(reading["heart_rate"]),
        "CRITICAL_HR": float(reading["heart_rate"]),
        "SPO2_LOW": float(reading["spo2"]),
        "BP_HIGH": float(reading["systolic_bp"]),
    }
    for rule, (low, high) in ABNORMAL_RULES.items():
        value = values.get(rule)
        if value is None:
            continue
        if rule.startswith("SPO2"):
            if value < high:
                flags.append(rule)
        elif value >= low:
            flags.append(rule)
    return flags


def normalize(raw: dict[str, Any]) -> dict[str, Any]:
    """Coerce a raw vitals payload into the normalized Iceberg row shape.

    Idempotent and purely functional (L2 uses this; Flink raw-write reuses it).
    Raises KeyError/TypeError on missing/undecodable fields -> Dropped by L2.
    """
    required = {"patient_id", "device_id", "event_time", "heart_rate", "spo2", "systolic_bp"}
    missing = required.difference(raw)
    if missing:
        raise KeyError(f"missing fields: {sorted(missing)}")
    for field in ("heart_rate", "spo2", "systolic_bp", "diastolic_bp", "temperature", "resp_rate"):
        value = raw.get(field)
        if value is not None and not (
            isinstance(value, (int, float)) and math.isfinite(float(value))
        ):
            raise TypeError(f"non-numeric {field}: {value!r}")
    out = {
        "patient_id": str(raw["patient_id"]),
        "device_id": str(raw["device_id"]),
        "event_time": str(raw["event_time"]),
        "ingestion_time": raw.get(
            "ingestion_time", datetime.now(UTC).isoformat(timespec="milliseconds")
        ),
        "heart_rate": _round3(float(raw.get("heart_rate", 0.0))),
        "systolic_bp": _round3(float(raw.get("systolic_bp", 0.0))),
        "diastolic_bp": _round3(float(raw.get("diastolic_bp", 0.0))),
        "spo2": _round3(float(raw.get("spo2", 0.0))),
        "temperature": _round3(float(raw.get("temperature", 0.0))),
        "resp_rate": _round3(float(raw.get("resp_rate", 0.0))),
        "trace_id": str(raw.get("trace_id", "")),
    }
    flags = abnormal_flags(out)
    out["abnormal_flags"] = flags
    out["is_abnormal"] = bool(flags)
    return out
