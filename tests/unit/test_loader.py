"""Unit tests for the Synthea cohort discovery + mapping pipeline."""

from __future__ import annotations

import json

from patient_data.synthea_loader import discover_bundles, map_all


def _bundle(tmp_path) -> None:
    (tmp_path / "fhir").mkdir(parents=True, exist_ok=True)
    bundle = {
        "resourceType": "Bundle",
        "entry": [
            {
                "resource": {
                    "resourceType": "Patient",
                    "id": "aaa",
                    "name": [{"given": ["X"], "family": "Y"}],
                }
            },
            {
                "resource": {
                    "resourceType": "Encounter",
                    "id": "enc-1",
                    "class": {"code": "ambulatory"},
                    "period": {"start": "2025-01-01T00:00:00Z"},
                    "subject": {"reference": "Patient/aaa"},
                }
            },
        ],
    }
    (tmp_path / "fhir" / "100.json").write_text(json.dumps(bundle), encoding="utf-8")


def test_discover_bundles_ignores_missing_dir(tmp_path):
    assert discover_bundles(tmp_path / "nope") == []


def test_discover_bundles_finds_fhir_subdir(tmp_path):
    _bundle(tmp_path)
    bundles = discover_bundles(tmp_path)
    assert len(bundles) == 1


def test_map_all_skips_corrupt_files_and_counts(tmp_path):
    (tmp_path / "fhir").mkdir(parents=True, exist_ok=True)
    (tmp_path / "fhir" / "bad.json").write_text("{not json", encoding="utf-8")
    _bundle(tmp_path)
    counts = map_all(discover_bundles(tmp_path))
    assert counts["patients"] == 1
    assert counts["encounters"] == 1
