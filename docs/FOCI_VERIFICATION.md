# Floci Capability Verification — 2026-09-15

Status: verified against `floci/floci:2.1.0` docs (floci.io) + Docker Hub.
Purpose: record what is proven before each service is committed to in the
scaffold, and flag what must be spiked in M1/M2.

## Pinned artifacts

- Image: `floci/floci:2.1.0` (standard native image). CI/dev should PIN the
  `2.x.y` tag; `latest` drifts nightly (SECURITY.md §7).
- `-compat` variant adds AWS CLI/boto3 inside the container (not required).
- `floci-duck` sidecar (`floci/floci-duck:latest`) powers Athena +
  Firehose Parquet conversion; auto-managed by Floci (needs docker.sock).
- testcontainers-floci `0.1.1` on PyPI (import `from floci import FlociContainer`).

## Verified supported (per official docs)

| Service | Mode | Notes |
|---|---|---|
| Kinesis | in-process | Streams, shards, enhanced fan-out, split/merge (24 ops) |
| Managed Flink (Kinesis Analytics V2) | Real Docker | `StartApplication` pulls the job **JAR from local S3** and runs a real `apache/flink` JobManager+TaskManager; runtimes FLINK-1_15…FLINK-2_3; runtime props at `/etc/flink/application_properties.json`; real savepoints; SQL-1_0/Zeppelin NOT supported |
| Data Firehose | in-process | Buffered flush to S3; **Lambda transform via `ProcessingConfiguration`** (Ok/Dropped/ProcessedFailed + error-output prefix `.failures`) verified; optional Parquet conversion via floci-duck + Glue schema |
| Lambda | Real Docker | 46 ops; SQS/Kinesis/DDB Streams ESM triggers; real runtimes; warm pool |
| DynamoDB + Streams | in-process | streams → Lambda event source supported |
| SNS | in-process | topic + Lambda/HTTP subs |
| API Gateway v2 (HTTP+WS) | in-process | full data plane: routes → `AWS_PROXY` Lambda integrations, authorizers (incl. Lambda REQUEST), `floci:override-id` tag pins stable API id |
| EventBridge Scheduler | in-process | cron/rate/at; **targets: SQS, Lambda, SNS, EventBridge PutEvents**; 10 s tick |
| Glue Data Catalog | in-process | CreateDatabase/Table, GetTable(s), DeleteTable, GetPartitions, CreatePartition(Index)… |
| IAM | in-process | roles/policies emulated, not enforced (`FLOCI_SERVICES_IAM_ENFORCEMENT_ENABLED=false`) |
| S3 | in-process | path-style; per-account namespaces; presign |

Config vocabulary: all `FLOCI_*` env vars; storage modes
`memory|persistent|hybrid|wal`; set `FLOCI_HOSTNAME=<compose service>` so
internal containers (and Lambda sidecars) reach the emulator as
`http://floci:4566`; any non-empty creds; region `us-east-1`; account
`000000000000`.

## RISK: Glue as the Trino Iceberg catalog

The docs honestly list what Iceberg's Glue commit path needs and Floci lacks:

- **Missing from Floci Glue:** `UpdateTable`, `GetPartition` (by values),
  `BatchCreatePartition`, `BatchUpdatePartition`, `UpdateDatabase`,
  `SearchTables`.
- Trino's Iceberg `glue` catalog persists each commit by flipping
  `metadata_location`/`current_snapshot_id` through **`UpdateTable`**. With
  `UpdateTable` absent, `CREATE TABLE` may succeed but **INSERT/ALTER commits
  cannot — schema never advances**. Floci does not document passthrough of
  `Parameters`/`Location`, so the wire behavior must be probed before trusting it.

→ **Scaffold decision:** catalog is a variable, dev default `nessie`
(proven path from healthcare-lakehouse-trino-avro). `glue` is selectable and
is an explicit M1/M2 spike. If the spike proves the commit path is intact,
flip the default to `glue` (AWS-native) and delete the Nessie fallback.

## RISK: API GW v2 hostname routing in CI

v2 HTTP APIs are addressed as `{apiId}.execute-api.localhost.floci.io:4566`
(hostname-based data plane). CI runners are not on Floci's embedded DNS, so
integration tests must use `curl --resolve` / `/etc/hosts` mapping
`<apiId>.execute-api.localhost.floci.io -> 127.0.0.1`, or run via the compose
network. Mitigation: pin API id with `tags = { "floci:override-id" = "patients" }`.

## Operational notes

- Firehose flush: `IntervalInSeconds` honored (~10 s tick). For fast demos use
  a 30–60 s interval; conversion/flush on shutdown is guaranteed.
- Scheduler invocations are timer-driven (10 s granularity) — fine for the
  daily L4 cron; not for sub-minute schedules.
- CloudWatch Logs caps stored events (20 000/account default) — enough for CI.
- Floci provisioning of Terraform resources: compat suite runs OpenTofu v1.9+ /
  Terraform v1.10+ (16 / 22 tests green), so `tofu apply` against :4566 is a
  supported pathway.

## Decided integrations (locked)

| Service | Outcome |
|---|---|
| Kinesis (Floci) | commit — tested EFO surface in M2 integration |
| Flink (Managed via Floci) | commit — build JAR, upload to S3, `kinesisanalyticsv2` start; standalone `apache/flink` compose service kept as dev alternative |
| Firehose → Lambda L2 → S3 | commit — ProcessingConfiguration transform, `BufferingHints.IntervalInSeconds=30` |
| DynamoDB + Streams → L1 → SNS | commit — ESM eabled on `alerts`, `sent` guard dedupe |
| API GW v2 → L3 | commit — `floci:override-id` + curl --resolve pattern in tests |
| Scheduler → L4 | commit — daily cron; lock via DynamoDB ConditionalWrite |
| Glue catalog → Trino | SPIKE in M1 — Nessie default until `UpdateTable` wire behavior proven |

## Verified locally end-to-end (2026-09-16)

Beyond the official-docs capability table above, this session exercised Floci
as the backbone of the running stack:

- **Standalone Flink → Iceberg (Nessie) → Trino**: one Flink 1.19 job
  (`flink/sql/job.sql`) consumed the `vitals` Kinesis stream from Floci and
  wrote **three** Iceberg tables (`vitals`, `vitals_1m` TUMBLE, `vitals_hop_1m`
  HOP) into the Nessie catalog, with Iceberg's S3FileIO writing path-style to
  Floci S3 and Trino reading the same tables.
- **Kinesis replay**: TRIM_HORIZON re-read ~7 300 retained records after a
  fresh job start (Floci's 24 h retention). Row signatures: `vitals` 7 324,
  `vitals_1m` 32, `vitals_hop_1m` 60 (≈2× tumbling, as a HOP 30s/1m predicts).
- **In-network addressing**: console/CLI on `127.0.0.1:4566`, in-network
  consumers (Flink, Trino, Lambda) on `http://floci:4566` via `FLOCI_HOSTNAME`.
- **Storage**: `hybrid` mode kept emulated state (stream data) across several
  container restarts in the same session.
- **Limits observed to respect**: emulated services are single-process
  in-memory — resource exhaustion shows up as process death elsewhere in the
  stack (e.g. Flink TM OOMs), and Glue's catalog gaps stand (Nessie remains
  the dev catalog). See docs/FLINK_OPS.md + docs/FLOCI.md.