# Data Flow Design

Status: Draft v2
Date: 2026-09-16
Sequence diagrams for each end-to-end path. Mermaid `sequenceDiagram`.

## 1. Vitals Ingest — Firehose + L2 Transform Path

```mermaid
sequenceDiagram
    participant SIM as Vitals simulator
    participant KIN as Kinesis (Floci)
    participant FH as Firehose (Floci)
    participant L2 as Lambda L2 (transform)
    participant S3R as S3 raw bucket
    participant ICE as Iceberg lake

    SIM->>KIN: PutRecord(vitals JSON)
    KIN-->>SIM: sequenceNumber
    KIN-->>FH: stream pull (per delivery window)
    FH->>FH: buffer records (size/time)
    FH->>L2: invoke (batch, base64 payloads)
    L2->>L2: parse, normalize, flag abnormal buckets
    L2->>FH: response records {recordId, result: Ok/Dropped, data}
    FH->>S3R: GZIP/Parquet sink (partitioned)
    S3R-->>ICE: Iceberg add (raw-normalized table)
```

Retries: FH retries failed batches; `result: ProcessingFailed` moves to the
S3 `.failures/` prefix (Floci/PutRecordBackup). L2 idempotent on recordId.

## 2. Vitals Ingest — Flink Aggregation + Serving

```mermaid
sequenceDiagram
    participant SIM as Vitals simulator
    participant KIN as Kinesis (Floci)
    participant FLINK as Flink SQL job (3 sinks)
    participant ICE as Iceberg (Nessie) healthcare schema
    participant TRINO as Trino

    SIM->>KIN: PutRecord(vitals JSON)
    FLINK->>KIN: poll (FlinkKinesisConsumer, initpos TRIM_HORIZON)
    FLINK->>FLINK: WATERMARK(event_time, 10s), read Kinesis
    FLINK-->>ICE: vitals            (raw event rows + abnormal_flags[] + is_abnormal)
    FLINK-->>ICE: vitals_1m         (TUMBLE 1m aggregates per patient)
    FLINK-->>ICE: vitals_hop_1m     (HOP 30s/1m overlapping aggregates per patient)
    TRINO->>ICE:  SELECT ... window / LAG / CTE (Superset + workshop)
    TRINO-->>SUP: result set
```

One job (`job.sql`), three streaming INSERTs — each lands as its own Flink job
so TUMBLE/HOP/window dialects run side by side on the same stream. Sinks write
Parquet via Iceberg and only publish **on checkpoints** (`IcebergFilesCommitter`):
if the clusters tables stay empty, the checkpoint interval is missing
(`execution.checkpointing.interval`; see FLINK_OPS.md).

Read-time processing happens in the lake: window functions (LAG/LEAD, moving
averages) and CTEs run over `vitals` in Trino — see `sql/workshop/`.

Designed-but-not-wired: Firehose/L2 and the DynamoDB `latest_vitals` read-model
refresh from this path (see DATA_MODEL §5/§8).

## 3. Alert Path — L1 Notifier (DynamoDB Streams → Lambda → SNS)

```mermaid
sequenceDiagram
    participant FLINK as Flink (rule engine)
    participant DDB as DynamoDB alerts
    participant STREAM as DDB Streams
    participant L1 as Lambda L1
    participant SNS as SNS topic
    participant DLQ as L1 DLQ

    FLINK->>DDB: PutItem(alert)  # dedup via alert_id
    DDB-->>STREAM: UPDATE/INSERT event
    STREAM-->>L1: invoke (batch, retries per shard)
    L1->>L1: dedupe by alert_id, set sent
    L1->>SNS: publish (patient, rule, value, ts)
    SNS-->>L1: MessageId
    L1-->>DDB: update (sent=true, delivery_ts)
    L1-->>DLQ: async on failure (max retries exhausted)
```

Lambda semantics: synch trigger, batch window 1s or 100 incl, `BisectBatchOnErrorEnabled=true`, max retry attempts, DLQ after exhaustion. Idempotent via the `sent` guard.

## 4. Firehose transform detail (L2) — record contract

```mermaid
sequenceDiagram
    participant FH as Firehose
    participant L2 as Lambda L2
    FH->>L2: {"records": [{"recordId","data"(base64)}]}
    L2->>L2: decode → normalize (field map, rounding, flags)
    L2->>FH: {"records":[{"recordId","result":"Ok","data"(base64)}]}
    Note over L2,FH: Ok | Dropped | ProcessingFailed
    FH->>FH: On ProcessingFailed → retry/backup
```

Rules: exactly one response per input recordId; batch up to 10k records in
bounded time; don't buffer state across invocations; log recordIds that are
unmapped or failed.

## 5. Query Path

```mermaid
sequenceDiagram
    participant SUP as Superset
    participant TRINO as Trino
    participant GLUE as Glue Data Catalog
    participant ICE as Iceberg (S3)

    SUP->>TRINO: SELECT ... (window/CTE/join)
    TRINO->>GLUE: resolve schema (#tables)
    GLUE-->>TRINO: table metadata
    TRINO->>ICE: read Parquet/ORC (partition pruning)
    ICE-->>TRINO: rows
    TRINO-->>SUP: result set
```

## 6. Patients REST API — L3

```mermaid
sequenceDiagram
    participant CLI as curl / UI
    participant GW as API Gateway v2 (Floci)
    participant L3 as Lambda L3
    participant DDB as DynamoDB

    CLI->>GW: GET /patients/{id}/latest-vitals
    GW->>L3: event {path, query, headers, context}
    L3->>L3: validate id, choose table (latest_vitals | alerts)
    L3->>DDB: GetItem / Query (SK prefix)
    DDB-->>L3: item(s)
    L3-->>GW: {statusCode, body, headers}
    GW-->>CLI: 200 JSON (or 400/404)
```

Errors are handled in L3 and relayed as proper HTTP status codes (no
unhandled 5xx). Optional API-key authorizer in front (SECURITY.md).

## 7. Scheduled Maintenance / ML Export — L4

```mermaid
sequenceDiagram
    participant EB as EventBridge Scheduler
    participant L4 as Lambda L4
    participant ICE as Iceberg
    participant S3ML as S3 ml/ buckets

    EB->>L4: StepFunction/EventBridge cron event (daily 02:00)
    L4->>L4: acquire lock (DDB advisory lock, ttl)
    L4->>ICE: expire_snapshots, remove_orphan_files, (compact small files)
    L4->>ICE: rewrite vitals → vitals_features Parquet union
    L4->>S3ML: write features + train/val/test manifest (split by patient)
    L4->>L4: release lock
```

Idempotency: run date key; DDB lock prevents overlapping runs; manifest write
is atomic (write `.tmp` then rename-in-catalog).

## 8. Cross-cutting Notes

- Event timestamp is authoritative (`event_time`); ingestion stamping for
  latency measurement only.
- Every hop logged with `trace_id` (producer-minted UUID propagated in
  payloads) for debugging replay.
- Replay strategy: raw S3 payloads are the replay source — reprocessing a
  window = re-read raw partition → recompute (see RELIABILITY.md). The Flink
  source replays from the stream (initpos `TRIM_HORIZON`, 24 h Floci
  retention); drop the Iceberg tables before resubmitting to avoid duplicate
  window rows (see FLINK_OPS.md).