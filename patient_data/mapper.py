"""Map Synthea FHIR output to the lakehouse table shapes (DATA_MODEL.md §3).

Synthea emits FHIR R4 bundles (Patient/Encounter/Condition/MedicationRequest/...).
Id mapping preserves Synthea ids so dashboards can be reused from the prior
project. Mapping is pure + deterministic; the load path (staged writes,
INSERT OVERWRITE by batch) lives in synthea_loader.py.
"""

from __future__ import annotations

from typing import Any

FHIR_DATE = "%Y-%m-%d"
FHIR_TIMESTAMP = "%Y-%m-%dT%H:%M:%SZ"


def _text(resource: dict[str, Any], field: str) -> str:
    name = resource.get("name", [{}])[0]
    return str(name.get(field, ""))


def _codings(resource: dict[str, Any]) -> str:
    coding = (resource.get("code") or {}).get("coding") or []
    if not coding:
        return ""
    return str(coding[0].get("code", ""))


def _display(resource: dict[str, Any]) -> str:
    coding = (resource.get("code") or {}).get("coding") or []
    return str(coding[0].get("display", "")) if coding else ""


def map_patient(resource: dict[str, Any]) -> dict[str, Any]:
    assert resource.get("resourceType") == "Patient"
    meta = resource.get("extension") or []
    return {
        "patient_id": str(resource["id"]),
        "first_name": _text(resource, "given"),
        "last_name": _text(resource, "family"),
        "gender": str(resource.get("gender", "")),
        "birth_date": str(resource.get("birthDate", "")),
        "race": str(_extension_value(meta, "race")),
        "ethnicity": str(_extension_value(meta, "ethnicity")),
        "city": str(_extension_value(meta, "city")),
        "state": str(_extension_value(meta, "state")),
        "county": str(_extension_value(meta, "county")),
    }


def map_encounter(resource: dict[str, Any], patient_id: str) -> dict[str, Any]:
    assert resource.get("resourceType") == "Encounter"
    return {
        "encounter_id": str(resource["id"]),
        "patient_id": patient_id,
        "encounter_class": str((resource.get("class") or {}).get("code", "")),
        "start_time": str(resource.get("period", {}).get("start", "")),
        "stop_time": str(resource.get("period", {}).get("end", "")),
        "organization": str((resource.get("serviceProvider", {}) or {}).get("reference", "")),
    }


def map_condition(resource: dict[str, Any], patient_id: str) -> dict[str, Any]:
    assert resource.get("resourceType") == "Condition"
    return {
        "condition_id": str(resource["id"]),
        "patient_id": patient_id,
        "code": _codings(resource),
        "description": _display(resource),
        "onset": str(resource.get("onsetDateTime", "")),
        "recorded": str(resource.get("recordedDate", "")),
        "encounter_id": str((resource.get("encounter", {}) or {}).get("reference", "")),
    }


def map_medication(
    resource: dict[str, Any], patient_id: str, encounter_id: str = ""
) -> dict[str, Any]:
    assert resource.get("resourceType") == "MedicationRequest"
    return {
        "medication_id": str(resource["id"]),
        "patient_id": patient_id,
        "code": _codings(resource),
        "description": _display(resource),
        "start_time": str(resource.get("authoredOn", "")),
        "stop_time": "",
        "encounter_id": encounter_id,
    }


def _extension_value(meta: list[dict[str, Any]], url_suffix: str) -> str:
    for ext in meta:
        url = str(ext.get("url", ""))
        if url.endswith(url_suffix):
            return str(ext.get("valueString", ""))
    return ""


def rows_from_bundle(bundle: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Flatten one Synthea FHIR bundle into table row lists keyed by table."""
    patients: list[dict[str, Any]] = []
    encounters: list[dict[str, Any]] = []
    conditions: list[dict[str, Any]] = []
    medications: list[dict[str, Any]] = []

    patient_id_by_resource: dict[str, str] = {}
    for resource in bundle.get("entry", []):
        resource = resource.get("resource", {})
        rtype = resource.get("resourceType")
        if rtype == "Patient":
            patients.append(map_patient(resource))
        elif rtype == "Encounter":
            ref = str((resource.get("subject", {}) or {}).get("reference", ""))
            patient_id_by_resource[str(resource["id"])] = ref.rsplit("/", 1)[-1]

    for resource in bundle.get("entry", []):
        resource = resource.get("resource", {})
        rtype = resource.get("resourceType")
        if rtype == "Encounter":
            encounters.append(
                map_encounter(resource, patient_id_by_resource.get(str(resource["id"]), ""))
            )
        elif rtype == "Condition":
            ref = str((resource.get("subject", {}) or {}).get("reference", ""))
            conditions.append(map_condition(resource, ref.rsplit("/", 1)[-1]))
        elif rtype == "MedicationRequest":
            ref = str((resource.get("subject", {}) or {}).get("reference", ""))
            encounter_ref = str((resource.get("encounter", {}) or {}).get("reference", "")).rsplit(
                "/", 1
            )[-1]
            medications.append(map_medication(resource, ref.rsplit("/", 1)[-1], encounter_ref))

    return {
        "patients": patients,
        "encounters": encounters,
        "conditions": conditions,
        "medications": medications,
    }
