"""Unit tests for the producer CLI helpers (producer.producer)."""

from __future__ import annotations

import itertools

from producer.producer import ProducerOptions, device_fanout, load_patient_ids, readings


def test_load_patient_ids_first_column(tmp_path):
    csv_path = tmp_path / "cohort.csv"
    csv_path.write_text("patient_id,name\nP0001,A\nP0002,B\n", encoding="utf-8")
    assert load_patient_ids(str(csv_path)) == ["P0001", "P0002"]


def test_load_patient_ids_fallback_columns(tmp_path):
    csv_path = tmp_path / "cohort.csv"
    csv_path.write_text("Id,Name\nxyz,Alice\nabc,Bob\n", encoding="utf-8")
    assert load_patient_ids(str(csv_path)) == ["xyz", "abc"]


def test_device_fanout_cycles_patients():
    patients = ["P1", "P2", "P3"]
    assert device_fanout(patients, 5) == ["P1", "P2", "P3", "P1", "P2"]


def test_device_fanout_requires_patients():
    import pytest

    with pytest.raises(ValueError):
        device_fanout([], 1)


def test_readings_yield_valid_records_without_sleep():
    opts = ProducerOptions(
        stream="vitals",
        device_count=2,
        patients=("P1", "P2"),
        cadence_seconds=1.0,
        abnormal_fraction=0.0,
        max_attempts=3,
        seed=7,
    )
    devices = device_fanout(["P1", "P2"], opts.device_count)
    stream = itertools.islice(readings(devices, opts), 4)
    produced = list(stream)
    assert len(produced) == 4
    assert all(r["patient_id"] in {"P1", "P2"} for r in produced)
    assert all(r["device_id"] in {"bed-000", "bed-001"} for r in produced)
    assert produced[0]["trace_id"] != produced[1]["trace_id"]
