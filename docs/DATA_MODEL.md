# Data Model Design

Status: Draft v1
Date: 2026-09-15

## 1. Storage Plan

- **Data lake**: S3 (Floci) — bucket layout below; Iceberg tables; metadata in
  Glue Data Catalog.
- **Read-model**: DynamoDB tables for alerts + latest vitals (API/serving).
- **ML**: Parquet exports from Iceberg (see §6).

## 2. S3 Bucket Layout

```
s3://healthcare-lake/
  raw/vitals/year=YYYY/MM/DD/hour=HH/          # post-Firehose/L2 Parquet
  iceberg/warehouse/                           # Iceberg table files + metadata
  ml/exports/vitals_features.parquet/          # L4-generated feature exports
  ml/manifests/                                # training/val split manifests
```

## 3. Iceberg Tables (Glue Catalog)

Database `healthcare`:

### patients
| Column | Type | Notes |
|---|---|---|
| patient_id | varchar | PK, joins vitals |
| first_name / last_name | varchar | Synthea |
| gender | varchar | M/F/NB |
| birth_date | date | |
| race / ethnicity | varchar | |
| city / state / county | varchar | location dims for dashboards |

### encounters
| Column | Type | Notes |
|---|---|---|
| encounter_id | varchar | PK |
| patient_id | varchar | FK→patients |
| encounter_class | varchar | ambulatory/emergency/inpatient… |
| start_time / stop_time | timestamp(3) | event time |
| organization | varchar | |

### conditions
| Column | Type | Notes |
|---|---|---|
| condition_id | varchar | PK |
| patient_id | varchar | |
| code / description | varchar | ICD-10-ish |
| onset / recorded | timestamp | |
| encounter_id | varchar | optional FK |

### medications
| Column | Type | Notes |
|---|---|---|
| medication_id | varchar | PK |
| patient_id | varchar | |
| code / description | varchar | RxNorm-ish |
| start_time / stop_time | timestamp | |
| encounter_id | varchar | optional |

### vitals (streaming, raw-normalized)
| Column | Type | Notes |
|---|---|---|
| patient_id | varchar | keyed to patient layer |
| device_id | varchar | simulated bedside device |
| event_time | timestamp(3) | event time from simulator |
| ingestion_time | timestamp(3) | Kinesis/Firehose arrival |
| heart_rate | double | bpm |
| systolic_bp / diastolic_bp | double | mmHg |
| spo2 | double | % |
| temperature | double | °C |
| resp_rate | double | breaths/min |
| abnormal_flags | array(varchar) | simulator/computation derived |

### vitals_agg_1m (Flink windowed serving)
| Column | Type | Notes |
|---|---|---|
| patient_id | varchar | |
| window_start / window_end | timestamp(3) | 1-min TUMBLE |
| avg_hr / max_hr / min_hr | double | |
| avg_spo2 / min_spo2 | double | |
| hr_ge_120_count | bigint | count buckets for alerting |
| reading_count | bigint | |
| abnormal_count | bigint | |

### alerts (DynamoDB — see §5)

## 4. Partitioning Strategy (Iceberg)

- `vitals`: `bucket(patient_id, 16)`, transform event_time month
  (`DAY(event_time)` optional). Bucket on patient_id keeps per-patient scans
  local; time transform enables range pruning for dashboards.
- `vitals_agg_1m`: `DAY(window_start)`.
- A batch-generated tables: unpartitioned or small (`bucket(patient_id, 4)`)
  — volumes are small.
- Retention: Iceberg `expire_snapshots`/`remove_orphan_files` via L4;
  optional TTL by dropping old partitions.

## 5. DynamoDB Tables

| Table | Key | TTL | Purpose |
|---|---|---|---|
| `alerts` | PK `patient_id`, SK `alert_id` (event_ts+rule) | 90d | alert rows; **streams enabled** for L1 |
| `latest_vitals` | PK `patient_id` | none | last normalized reading per patient (API read model, refreshed by Flink sink) |

Alert item shape:
```json
{
  "patient_id": "P001",
  "alert_id": "A-<alert_ts>-<rule>",
  "rule": "HR_SUSTAINED_HIGH",
  "description": "HR >= 120 for >= 30 s",
  "severity": "HIGH",
  "value": 128.5,
  "threshold": 120,
  "event_ts": "2026-09-15T12:00:00Z",
  "resolved": false,
  "sent": false,
  "created_at": "2026-09-15T12:00:00Z"
}
```

## 6. ML Export (L4 → Parquet)

Feature-wide table `vitals_features` (one row per patient×window):

```
patient_id, window_start, window_end, age_years, gender,
avg_hr, max_hr, hr_p95, hr_std, avg_spo2, min_spo2,
slope_hr_15m, rolling_avg_hr_1h,
is_anomaly (label: rule/statistical outlier),
split (train / val / test)   # by patient, never by row
```

Split by patient prevents leakage; manifest in `s3://healthcare-lake/ml/manifests/`.

## 7. Naming & Conventions

- Snake_case columns; Timestamps as `timestamp(3) with time zone` where
  event time is meaningful.
- Surrogate keys only from Synthea ids (no PII as key material).
- Schemas versioned via Glue catalog; breaking changes = new table version
  + backfill job, never in-place.

## 8. Consistency Guarantees

- Alerts/latest_vitals in DynamoDB converge via Flink exactly-once sinks;
  readers tolerate a short staleness window (seconds).
- Iceberg is source of truth for history; DynamoDB is a serving cache derived
  from it, not authoritative.
- See RELIABILITY.md for exactly-once/at-least-once contract per hop.