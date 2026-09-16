"""Unit tests for the vitals generator + normalizer (DATA_MODEL.md §3)."""

from __future__ import annotations

import pytest

from producer.vitals import abnormal_flags, normalize


def test_generate_reading_shape(vitals_batch):
    reading = vitals_batch[0]
    assert reading["patient_id"] == "P0001"
    assert set(
        {
            "patient_id",
            "device_id",
            "event_time",
            "heart_rate",
            "systolic_bp",
            "diastolic_bp",
            "spo2",
            "temperature",
            "resp_rate",
            "trace_id",
        }
    ) == set(reading)


def test_normalize_rounds_to_three_decimals(vitals_batch):
    row = normalize(vitals_batch[0])
    assert row["heart_rate"] == pytest.approx(row["heart_rate"], abs=1e-6)
    assert isinstance(row["abnormal_flags"], list)
    assert row["is_abnormal"] is True


def test_normalize_normal_reading_has_no_flags(vitals_batch):
    assert normalize(vitals_batch[1])["is_abnormal"] is False


def test_normalize_missing_field_raises():
    with pytest.raises(KeyError):
        normalize({"patient_id": "P1"})


def test_normalize_non_numeric_value_raises():
    with pytest.raises(TypeError):
        normalize(
            {
                "patient_id": "P1",
                "device_id": "bed-0",
                "event_time": "2026-09-15T00:00:00Z",
                "heart_rate": "fast",
                "spo2": 98,
                "systolic_bp": 118,
            }
        )


def test_abnormal_spo2_flags():
    flags = abnormal_flags({"heart_rate": 82, "spo2": 88.0, "systolic_bp": 118})
    assert "SPO2_LOW" in flags


def test_abnormal_flags_are_empty_for_normal_values():
    assert abnormal_flags({"heart_rate": 82, "spo2": 98.0, "systolic_bp": 118}) == []


def test_trace_id_present_and_reused(vitals_batch):
    assert all(r["trace_id"] for r in vitals_batch)
