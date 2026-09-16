# Reliability Design

Status: Draft v1
Date: 2026-09-15

## 1. Delivery Guarantees Per Hop

| Hop | Guarantee | Mechanism |
|---|---|---|
| Simulator → Kinesis | At-least-once (success = seq num) | Producer retries + dedupe key |
| Kinesis → Firehose | Best-effort (Firehose retries) | Delivery windows; no loss without config |
| Firehose → L2 → S3 | At-least-once | Firehose retry; `.failures/` backup prefix |
| Kinesis → Flink | At-least-once in, exactly-once out (to Iceberg) | Flink checkpoints + Iceberg/Glue sink (2PC/commit) |
| Flink → DynamoDB (alerts/latest) | Exactly-once via sink; at-least-once on retry | Idempotent PutItem by PK |
| DynamoDB Streams → L1 → SNS | At-least-once; exactly-once delivery avoided | `sent` guard + `alert_id` dedupe |
| API GW → L3 → DDB | N/A (reads) | — |
| Scheduler → L4 | At-least-once triggers | Lock + run-date idempotency |

## 2. Idempotency Strategy

- **Vitals events**: `(patient_id, device_id, event_time)` unique in the raw
  table; Iceberg accepts duplicate writes; downstream agg deduped by window
  recompute (keyed aggregates).
- **Alerts**: `alert_id = A-<event_ts>-<rule>`; L1 checks `sent` before
  SNS publish; duplicate stream events are no-ops.
- **latest_vitals**: last-write-wins by `event_time` (per patient).
- **L4 job**: run date as idempotency key; writes under partition `run_date=
  <date>`; manifest rename is atomic; advisory DDB lock (PK `maintenance-lock`,
  TTL ≥ job expected duration) prevents overlap.
- **Synthea loader**: staging partition + `INSERT OVERWRITE` semantics by
  generation batch id.

## 3. Retries / DLQ Behavior

| Component | Retry | DLQ/backup |
|---|---|---|
| L1 (DDB Streams) | Event-source retries (attempt window); `BisectBatchOnErrorEnabled` | SQS `lambda-alerts-dlq` |
| L2 (Firehose transform) | Firehose retries failed batches | S3 `raw/vitals/failures/` |
| L3 (API) | Client retries; no server retry | — (errors logged; metrics) |
| L4 (cron) | Scheduler retry policy (2 retries); DDB lock prevents overlap | CloudWatch alarm on failure |
| Flink | Checkpointed restart (Flink job restart strategy); sink commit retry | None (checkpoint-based) |
| Simulator | Producer backoff-jitter; records dropped after N attempts → dead-letter log | `s3://healthcare-lake/raw/vitals/unreached/` (debug only) |

## 4. Flink Reliability

- **Checkpoints**: enabled (default interval ~30–60s), RocksDB state backend
  with incremental snapshots; exactly-once via Iceberg sink committing
  to the Glue catalog.
- **Restart strategy**: `fixed-delay` (3 attempts) then failure → job restart.
- **Watermarks**: event-time arranged from `event_time` with generous allowed
  lateness (e.g. 30 s) and a bounded out-of-orderness; late data past window
  edge is emitted to a late-data side output table (reporting only).
- **Backpressure**: Kinesis at-least-once + CheckpointedFlink absorbs slow
  peers; alerting rule burst is bounded by window size.

## 5. Failure Scenarios & Mitigations

| Failure | Impact | Mitigation |
|---|---|---|
| Simulator down | No new vitals | Prometheus-style heartbeat; dashboards show staleness; alerts "data gap" rule on missing patient readings |
| Kinesis/Firehose outage (Floci restart) | Ingest gap | Data-replay from raw S3; simulator re-sends unacked via local DL log |
| Flink crash | Per-window aggregates lost only if post-checkpoint | Auto-restart from checkpoint; late-data table captures missed window |
| L1 Lambda stuck/error loop | Alerts not sent | `BisectBatchOnErrorEnabled` isolates poison records; DLQ; `sent` guard no double-send |
| L2 transform regression | Records dropped | Firehose `.failures/` backup; replay path heals once fixed |
| DDB capacity saturation (v1: no throttling on Floci) | Slow serving | pre-reserved RCU/WCU; exponential backoff in L3 |
| Trino crash | Dashboards stale | Superset reconnect + Trino auto-restart; Iceberg safe (immutable files) |
| Iceberg metadata bloat | Slow queries | L4 compaction: expire_snapshots / orphan cleanup weekly |

## 6. Data Retention

- Iceberg: raw vitals 90 days (snapshot expiry), agg serving 30 days,
  patient records 1 year (Synthea cohort snapshot). ML feature export kept
  indefinitely (small).
- DynamoDB: alerts TTL 90 days; latest_vitals no TTL (single row per
  patient, small).
- Implemented via L4 + S3 lifecycle rules where supported (Floci S3
  lifecycle is emulated; AWS real rules for later).

## 7. Availability/Qual of Service Targets

- Design targets (emulated env): ingest-to-query < 60 s p95;
  alert-delivery (rule matched → SNS) < 30 s p95; zero data loss for
  acknowledged simulator writes in a single-run session.
- These are validated in CI via integration suites (TESTING.md), not just
  asserted as aspirational.

## 8. Observability of Reliability

- Metric: `kinesis.put_record.failures`, `firehose.delivery.age`,
  `lambda.l1.dlq_depth`, `flink.checkpoint.failures`,
  `trino.query.failures` — surfaced in ops dashboard + CI assertions.