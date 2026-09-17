# Data Model Design

Status: Draft v2
Date: 2026-09-16

## 1. Storage Plan

- **Data lake**: S3 (Floci) — bucket layout below; Iceberg tables; metadata in
  the Nessie catalog (dev default; Floci Glue Data Catalog is the M1/M2 spike —
  see FOCI_VERIFICATION.md).
- **Read-model**: DynamoDB tables for alerts + latest vitals (API/serving;
  designed, not yet wired to Flink).
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
| first_name / last_name | varchar | demo cohort |
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
| abnormal_flags | array(varchar) | rule flags, e.g. `[BP_HIGH]`, `[]` |
| is_abnormal | boolean | true when any flag is set |

`abnormal_flags` is a **computed** array (Flink SQL) mirroring
`producer.vitals.abnormal_flags()`: heart_rate ≥ 120 → `HR_HIGH`, ≥ 180 →
`CRITICAL_HR`, spo2 < 92 → `SPO2_LOW`, systolic_bp ≥ 140 → `BP_HIGH`.
`is_abnormal` is true when at least one flag fired. Unnest it for per-flag
analysis (`CROSS JOIN UNNEST(abnormal_flags)`) — see the abnormal-flag CTE in
`sql/workshop/06_chained_ctes.sql`.

### vitals_1m (Flink TUMBLE 1m — non-overlapping)
| Column | Type | Notes |
|---|---|---|
| window_start / window_end | timestamp(3)(tz) | 1-min bucket (TUMBLE_START/END) |
| patient_id | varchar | |
| avg_heart_rate / max_heart_rate | double | HR bpm |
| min_spo2 | double | worst SpO2 in the bucket |
| max_systolic_bp | double | |
| reading_count | bigint | readings in the bucket |

One row per patient×minute; every reading belongs to exactly one bucket.

### vitals_hop_1m (Flink HOP 30s/1m — overlapping)
| Column | Type | Notes |
|---|---|---|
| window_start / window_end | timestamp(3)(tz) | HOP_START/END (size 1m, slide 30s) |
| patient_id | varchar | |
| avg_heart_rate / max_heart_rate | double | HR bpm |
| min_spo2 | double | |
| reading_count | bigint | readings in the bucket (may be shared) |

Each reading falls into up to TWO adjacent buckets, so the row/reading
counts are ~2× `vitals_1m`; the same 1-minute summary simply "refreshes"
every 30 s. Query the difference in `sql/workshop/07_tumbling_vs_hop.sql`.

### alerts (DynamoDB — see §5)

## 4. Partitioning Strategy (Iceberg)

- `vitals`: intended `bucket(patient_id, 16)`, transform event_time month
  (`DAY(event_time)` optional). Bucket on patient_id keeps per-patient scans
  local; time transform enables range pruning for dashboards.
- `vitals_1m` / `vitals_hop_1m`: intended `DAY(window_start)`.
- **Current state:** the Flink sinks create tables without an explicit
  partition spec (job.sql keeps DDLs minimal); Trino reads them fine at demo
  volume. Partitioning is a tuning milestone, not a correctness one.
- A batch-generated tables (encounters/conditions/…): unpartitioned or small
  (`bucket(patient_id, 4)`) — volumes are small.
- Retention: Iceberg `expire_snapshots`/`remove_orphan_files` via L4;
  optional TTL by dropping old partitions.

## 5. DynamoDB Tables

| Table | Key | TTL | Purpose |
|---|---|---|---|
| `alerts` | PK `patient_id`, SK `alert_id` (event_ts+rule) | 90d | alert rows; **streams enabled** for L1 |
| `latest_vitals` | PK `patient_id` | none | last normalized reading per patient (API read model; Flink-sink refresh designed, NOT yet wired — §8) |

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
- Surrogate keys only from demo-cohort ids (no PII as key material).
- Schemas versioned via the Nessie catalog (dev; Glue catalog is the M1 spike
  — see FOCI_VERIFICATION.md); breaking changes = new table version + backfill
  job, never in-place.

## 8. Consistency Guarantees

- Iceberg is the source of truth for history and the current streaming path:
  the Flink job writes `vitals`, `vitals_1m`, `vitals_hop_1m` exactly-once to
  Iceberg (Nessie catalog) and commits on every checkpoint.
- The `alerts` / `latest_vitals` DynamoDB read-model (below) is the designed
  serving layer but is NOT yet wired to the Flink job — the job currently has
  Iceberg sinks only. Readers of the read-model tolerate a short staleness
  window (seconds) once connected.
- See RELIABILITY.md for exactly-once/at-least-once contract per hop.