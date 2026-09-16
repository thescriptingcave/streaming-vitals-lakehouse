# Healthcare Realtime Vitals Lakehouse — Scoping Document

Status: Draft v1
Date: 2026-09-15

## 1. Executive Summary

A real-time vitals streaming + synthetic patient data lakehouse project. We
generate a full synthetic patient cohort (demographics, encounters,
conditions, medications) plus stream real-time vitals (heart rate, blood
pressure, SpO2, temperature) from simulated bedside devices. Streaming
aggregations and alerting run over the vitals; every event lands into an
S3-backed Iceberg lake alongside the patient records, and both live and
historical queries are served through Trino with full SQL power (time-series
functions, window functions, CTEs). Dashboards run on Superset; ML-ready data
is exported as Parquet from the lake.

The stack is validated end-to-end in GitLab CI with Floci (free, MIT
open-source AWS emulator) simulating AWS services (Kinesis, S3, Managed
Flink), so tests run against the same API surface as production AWS without
any cloud spend.

### Key differentiators vs. previous project
- Streaming ingest (Kinesis via Floci) instead of batch JSON producers.
- Two data layers: batch synthetic patient records + real-time vitals stream,
  joinable (vitals reference patient_id).
- CI/CD with GitLab (previous project had no CI).
- AWS service simulation with Floci in CI (free, no auth token — unlike
  token-gated LocalStack).
- Deliberate ML-readiness: enough data + clean Parquet exports for training.

## 2. Goals

1. A synthetic patient cohort: demographics, encounters, conditions,
   medications (reusing SKH/Synthea-style generation, aligned with the
   previous project's patient/encounter/observation schema).
2. Live dashboards with freshness in the order of seconds that join live
   vitals to patient profile data.
3. Time-series analytics: heart-rate trends, sliding windows, lag/lead.
4. Window functions and CTEs available at query time (Trino).
5. A lake (Iceberg on S3) holding both patient records and enough vitals
   history to train ML models (detect/monitor vitals anomalies), exportable
   to Parquet.
6. Streaming alerting rules (e.g. HR > 120 sustained for 30 s) keyed to real
   patients.
7. Full CI/CD via GitLab with Floci-backed integration tests.
8. Reuse of existing skills: Trino SQL, Superset, Iceberg/Nessie, Python.
9. A structured **Lambda learning path**: four real integrations covering the
   core AWS serverless patterns, exercised end-to-end on Floci.

## 3. Non-Goals (v1)

- Production-grade patient safety / HL7 / FHIR integration.
- Authentication beyond dev defaults.
- Multi-tenant or HIPAA compliance.
- Deploying to real AWS (Floci only).
- Anomaly-detection model rollout (dataset readiness only; model is spike/milestone).

## 4. Required Capabilities → Where They Map

| Capability | Fulfilled by |
|---|---|
| Live dashboards | Superset over Trino (+ Kinesis Analytics via Flink for pre-aggregates) |
| Time-series queries | Trino (date/time funcs, sliding windows) |
| Window functions | Trino + Flink SQL (ROW_NUMBER, LAG/LEAD, running avg) |
| CTEs | Trino (`WITH ... SELECT`) |
| Enough data for ML | Iceberg lake, `next_day`-style partitioned exports to Parquet |
| Real-time alerting | Flink SQL windowed rules → DynamoDB → Lambda notifier |
| Serverless patterns | Lambda (event-driven, stream, API, cron) on Floci |
| AWS simulation | Floci (Kinesis, S3, Lambda, DynamoDB, etc.) |

## 5. Architecture (candidate — decision in §7)

```
patient data generator (Python, Synthea-style)      simulated devices (Python)
  │  demographics / encounters / conditions                │  vitals heartbeats
  v                                                        v
  Iceberg lake ──► patients/encounters/...            Kinesis  [Floci]
  (batch, S3)                                              │
                                                           ├──────────────┐
                                                           v              v
                                                    Flink SQL      Firehose ──► Lambda
                                                    streaming job   transform ──► Iceberg sink
                                                    ├ windowed agg ─► Superset live
                                                    ├ alert rules ─► DynamoDB ─► Lambda ─► SNS
                                                    v                     (alert notifier)
                                              serving lake table (latest state)

                       Serverless (Lambda) on Floci:
                       ┌───────────────────────────────────────────────┐
                       │ Firehose transform  → S3/Iceberg (normalize)  │
                       │ Alert notifier     → SNS/SES (DDB Streams)    │
                       │ Patients API       → API GW → DynamoDB        │
                       │ Scheduler cron     → Iceberg maint + ML export │
                       └───────────────────────────────────────────────┘

                        Trino ──► joins (vitals ✕ patients) / time-series / window / CTE
                           │
                           ├──► Superset dashboards (live + historical)
                           └──► Parquet ML exports (training/val split)
```

### 5.1 Patient data layer (batch)
- **Generator**: Python/Synthea-style cohort (~thousands of patients) → the
  same patient/encounter/observation shape as the prior project, written
  directly into Iceberg via Trino INSERT/DDL (or Nessie DDL first).
- **Schema**: patients (id, gender, dob, race, city...), encounters (class,
  start/end...), conditions, medications — compatible with the previous
  project so dashboards can be reused.

### 5.2 Streaming path
- **Producer**: Python simulator generating vitals per "bed/device" keyed to
  real synthetic patient_ids (from the patient layer), writes to Kinesis
  stream (Floci).
- **Flink SQL job**: reads Kinesis (Kinesis connector), performs:
  - Windowed aggregations (TUMBLE/HOP over 1 s / 1 m).
  - Alert rules → results written to an alerts table/topic.
  - Dual-write every raw event to Iceberg (S3) for the historical lake.
- **Serving**: small latest-state table for instant dashboard refresh.

### 5.3 Query path
- Trino (or Athena-equivalent via Floci `floci-duck`) over Iceberg tables.
- Time-series, window functions, CTEs all plain ANSI SQL examples in demo SQL.
- Superset connects to Trino for both live and historical views.
- Demo queries join live vitals to patient demographics (e.g. HR by age cohort).

### 5.4 ML data path
- Daily/periodic job exports Iceberg → Parquet partitions (feature-ready
  wide table: windowed stats, rolling anomalies labeled).
- Catalog the export with a manifest so training can split by patient/time.

### 5.5 Serverless layer — Lambda Learning Path
Deliberate, ordered set of Lambda integrations. All Python (zip-packaged via
Terraform/OpenToFu), all run for real on Floci (real Docker Lambda
execution). Concepts are cumulative — each builds on the previous.

| # | Integration | Pattern learned | Trigger → flow |
|---|---|---|---|
| L1 | **Alert notifier** | Event-driven + DDB Streams | Flink alert → DynamoDB → stream → Lambda → SNS topic |
| L2 | **Firehose transform** | Event payload + batching | Kinesis → Firehose → Lambda transform → S3/Iceberg (normalized Parquet) |
| L3 | **Patients REST API** | Request/response + auth | API GW → Lambda → DynamoDB (GET patient, latest vitals, alerts) |
| L4 | **Scheduled Iceberg job** | Serverless cron + idempotency | EventBridge Scheduler → Lambda (Iceberg compaction + Parquet ML export) |

Learning objectives per pattern (interview-facing):
- L1: event batches, retries/DLQ, IAM roles, `event`/`context` shape.
- L2: response records contract, base64 payloads, failure modes, Firehose
  retry semantics.
- L3: path/query params, status codes, HTTP body validation, (optional) auth
  via API key.
- L4: `schedule`, idempotent execution, env config, cold-start awareness.

Deployment: Terraform/OpenToFu modules in `infra/`, applied against Floci
(`AWS_ENDPOINT_URL=http://localhost:4566`, openToFu compat verified by Floci
suite). Deployed in GitLab CI `deploy` stage.

## 6. Stack Decision

### Candidates
| | ClickHouse | Redpanda+Flink+Iceberg | Kinesis+Flink+Iceberg (LocalStack) | Kafka+Pinot |
|---|---|---|---|---|
| Streaming ingest | Kafka engine / inserts | Redpanda (embedded) | Kinesis (LocalStack) | Kafka |
| Patient records (batch) | Manual | Iceberg (batch) | **Iceberg (batch)** | Not primary |
| Vitals ✕ patient join | SQL (manual) | Trino | **Trino + Superset** | SQL only |
| Real-time agg | MV + projections | Flink SQL | Flink SQL | near-real-time OLAP |
| Window funcs | Good | Excellent | Excellent | Partial |
| CTEs | Good | (Flink limited; Trino yes) | (Flink limited; Trino yes) | Limited |
| ML-ready lake | Parquet export manual | Iceberg + Parquet | Iceberg + Parquet | Not primary |
| Ops burden | Low | Medium | Medium | High (2 svcs) |
| AWS realism (Floci/CI) | Weak | Good | **Best** | Good |

### Recommendation
**Kinesis (Floci) → Flink SQL → Iceberg (S3 via Floci) → Trino →
Superset**, with Parquet ML exports.

Rationale: directly mirrors production AWS APIs (Kinesis + S3), satisfies
every required capability, and reuses existing Trino/Superset/Iceberg skills.
Flink handles alerting + windowing; Trino gives window functions and CTEs;
Iceberg gives the ML-ready lake.

Fallback if streaming job complexity hurts: collapse to
**ClickHouse** (single service, fast, SQL all-in-one) but lose AWS realism
and weakening ML-lake story.

## 7. Floci (AWS Emulation) Usage

- **Floci** (free, MIT, drop-in LocalStack replacement) in `docker compose`
  and GitLab CI. Port 4566, no auth token. Storage modes: `memory` (CI),
  `hybrid`/`persistent` (local dev).
  - S3 — Iceberg warehouse storage (Trino `connector.properties` pointed at
    `s3://...` with Floci endpoint).
  - Kinesis — streams, shards, enhanced fan-out (verified supported).
  - Lambda — **real Docker execution** (warm pool, invoke semantics), used for
    all L1–L4 integrations.
  - DynamoDB + Streams, SNS, API Gateway (REST v2), EventBridge Scheduler —
    serverless building blocks for the Lambda path.
  - Managed Service for Apache Flink (Kinesis Analytics V2) — runs a real
    Flink cluster; option to skip a standalone Flink container.
  - Data Firehose — Lambda transform + delivery to S3.
  - Athena via `floci-duck` (DuckDB sidecar) — alternative query engine.
- Config via `FLOCI_SERVICES_*` vars; boto3/AWS CLI/Terraform point at
  `AWS_ENDPOINT_URL=http://localhost:4566`, creds are any non-empty values.
- Swap endpoint config to go real-AWS without code change (actual AWS prod
  deploy is out of scope for v1).

## 8. GitLab CI/CD Design

`.gitlab-ci.yml` stages (docker-compose based):

```
lint → test (unit) → integration (Floci) → build → deploy(simulated)
```

- **lint**: ruff + mypy (as existing project).
- **unit**: producer/business logic, no infra (`pytest`).
- **integration**: spin up `docker-compose.ci.yml` — Floci (services
  `s3`, `kinesis`, `lambda`, `dynamodb`, `sns`, `apigateway`,
  `events`), Trino, Nessie, Superset, Flink — push fixtures through
  the whole pipeline, assert rows/alerts arrive. Use `testcontainers-floci`
  (PyPI) for isolated per-test Floci instances.
- **build**: build producer/Flink images + zip Lambda packages (GitLab
  Container Registry / job artifacts).
- **deploy**: `terraform apply`/`tofu apply` of `infra/` against Floci
  (Lambda L1–L4 provisioning); then `docker compose up` smoke test
  (or simulated ECS via `aws ecs` on Floci) — TBD milestone.

## 9. Repository Topology (proposed)

```
healthcare-realtime-vitals-lakehouse/
  docs/               # this doc + architecture diagrams
  patient_data/       # Synthea-style patient/encounter/condition generator
  producer/           # real-time vitals simulator (Python)
  flink/              # Flink SQL jobs + jar
  lambda/             # L1–L4 serverless functions (Python, one dir each)
  infra/              # Terraform/OpenToFu: Lambda, DDB, SNS, API GW, Firehose
  trino/              # catalog + connector config
  superset/           # bootstrap + chart/dashboard provisioning
  tests/              # unit + integration
  ci/                 # compose files for GitLab CI
  ml/                 # Parquet export + validation split script
  Makefile
```

## 10. Milestones

- **M0 — Scoping + stack sign-off** (this doc; pick final stack).
- **M1 — Patient layer + local raw pipeline**: Floci up; patient cohort
  generated into Iceberg; producer → Kinesis → raw vitals Iceberg table;
  single Trino SELECT joining both. GitLab CI: lint + unit.
- **M2 — Flink streaming**: windowed aggs + alert rules → serving/alerts
  tables. GitLab CI integration stage passes on Floci.
- **M3 — Lambda L1+L2**: alert-notifier (DDB Streams → Lambda → SNS) and
  Firehose transform (normalize vitals → S3/Iceberg); Terraform infra in
  `infra/`, deployed in CI deploy stage.
- **M4 — Lambda L3+L4**: Patients REST API (API GW → Lambda → DDB) and
  scheduled Iceberg job (compaction + Parquet ML export).
- **M5 — Query + dashboards**: wire Trino → Superset; time-series, window
  function, CTE demo queries incl. vitals ✕ patient joins; live + historical
  dashboards.
- **M6 — ML data**: labeled Parquet dataset + training/val split; optional
  quick sklearn anomaly-baseline notebook.
- **M7 — CI/CD polish**: container registry, simulated deploy stage, docs.

## 11. Risks / Open Questions

- **Floci maturity/Kinesis depth**: new project (2026); verify record limits
  and EFO behavior in CI — fallback is Floci MSK (Redpanda) for the broker.
- **Flink↔Kinesis connector**: Managed Flink runs real Flink; verify Kinesis
  connector (EFO/polling) works against Floci (fallback: standalone Flink
  reading local Redpanda via MSK, Kinesis in prod).
- **Trino window/CTE support**: confirmed good; watch Iceberg partition
  pruning for time-range dashboards.
- **Nessie vs plain Iceberg**: reuse Nessie (as current project) or simplify.
- **Patient cohort source**: hand-rolled generator (reuse prior project's
  approach) vs actual Synthea (richer conditions/medications).
- **Vitals↔patients join cardinality**: ensure every streamed patient_id
  exists in the patient layer to avoid dirty joins.
- **Volume**: how much data is "enough for ML" — target ~5–10M events.
- **Lambda on Floci**: real Docker runtime (verified); watch warm-pool/cold-start
  behavior in CI and DDB Streams event-source mapping maturity — L1 is the
  earliest-risk item, spike it during M3.

## 12. Decisions

1. **Final stack: Kinesis+Flink+Iceberg (LOCKED)** — Floci-emulated Kinesis
   → Flink SQL → Iceberg (S3) → Trino → Superset, Parquet ML exports.
2. **Catalog**: Iceberg + **Glue Data Catalog (Floci)** — AWS-native; fallback
   Nessie if the Glue/Trino integration proves flaky in M1/M2.
3. **ML volume (LOCKED)** — 5M floor / 10M target events.
4. **Patient cohort (LOCKED)** — real **Synthea** (Java-based, FHIR-shaped
   patients/encounters/conditions/medications/procedures).

Decided (not open): Lambda path = L1–L4 (all four) via Terraform on Floci;
alerting = Flink rules → DynamoDB → Lambda → SNS (§5.5 L1).