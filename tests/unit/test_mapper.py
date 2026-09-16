"""Unit tests for Synthea FHIR -> table mapping (patient_data.mapper)."""

from __future__ import annotations

from patient_data.mapper import rows_from_bundle

BUNDLE = {
    "resourceType": "Bundle",
    "entry": [
        {
            "resource": {
                "resourceType": "Patient",
                "id": "1a2b",
                "name": [{"given": ["Jane"], "family": "Doe"}],
                "gender": "female",
                "birthDate": "1975-03-14",
            }
        },
        {
            "resource": {
                "resourceType": "Encounter",
                "id": "enc-1",
                "class": {"code": "ambulatory"},
                "period": {"start": "2025-01-01T09:00:00Z", "end": "2025-01-01T09:30:00Z"},
                "subject": {"reference": "Patient/1a2b"},
            }
        },
        {
            "resource": {
                "resourceType": "Condition",
                "id": "cond-1",
                "code": {"coding": [{"code": "E11.9", "display": "Type 2 diabetes"}]},
                "subject": {"reference": "Patient/1a2b"},
                "encounter": {"reference": "Encounter/enc-1"},
            }
        },
        {
            "resource": {
                "resourceType": "MedicationRequest",
                "id": "med-1",
                "code": {"coding": [{"code": "00378-3005", "display": "metformin"}]},
                "authoredOn": "2025-01-01T09:10:00Z",
                "subject": {"reference": "Patient/1a2b"},
                "encounter": {"reference": "Encounter/enc-1"},
            }
        },
    ],
}


def test_rows_from_bundle_shapes():
    rows = rows_from_bundle(BUNDLE)
    assert len(rows["patients"]) == 1
    assert len(rows["encounters"]) == 1
    assert len(rows["conditions"]) == 1
    assert len(rows["medications"]) == 1


def test_patient_mapping():
    patient = rows_from_bundle(BUNDLE)["patients"][0]
    assert patient["patient_id"] == "1a2b"
    assert patient["last_name"] == "Doe"
    assert patient["gender"] == "female"


def test_encounter_joins_correct_patient():
    encounter = rows_from_bundle(BUNDLE)["encounters"][0]
    assert encounter["patient_id"] == "1a2b"
    assert encounter["encounter_class"] == "ambulatory"


def test_condition_code_and_display():
    condition = rows_from_bundle(BUNDLE)["conditions"][0]
    assert condition["code"] == "E11.9"
    assert condition["description"] == "Type 2 diabetes"


def test_empty_bundle_is_safe():
    assert rows_from_bundle({"resourceType": "Bundle", "entry": []}) == {
        "patients": [],
        "encounters": [],
        "conditions": [],
        "medications": [],
    }
