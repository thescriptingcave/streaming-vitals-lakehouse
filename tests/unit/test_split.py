"""Unit tests for the patient-based train/val/test split (DATA_MODEL.md §6)."""

from __future__ import annotations

from ml.export_features import split_by_patient


def test_split_is_deterministic_and_covers_everyone():
    patients = [f"P{i:04d}" for i in range(1, 101)]
    first = split_by_patient(patients)
    second = split_by_patient(patients)
    assert first == second
    assert set(first) == set(patients)
    assert set(first.values()) == {"train", "val", "test"}


def test_split_ratios_approximate_targets():
    patients = [f"P{i:04d}" for i in range(1000)]
    mapping = split_by_patient(patients, ratios=(0.7, 0.15, 0.15))
    counts = {k: sum(1 for v in mapping.values() if v == k) for k in ("train", "val", "test")}
    assert counts["train"] == 700
    assert counts["val"] == 150
    assert counts["test"] == 150


def test_split_by_patient_is_order_independent():
    patients = [f"P{i:03d}" for i in range(50)]
    mixed = list(reversed(patients))
    assert split_by_patient(patients) == split_by_patient(mixed)


def test_single_patient_assigned_to_a_split():
    value = split_by_patient(["P001"])["P001"]
    assert value in {"train", "val", "test"}


def test_no_split_for_empty_input():
    assert split_by_patient([]) == {}
