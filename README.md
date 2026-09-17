# Healthcare Realtime Vitals Lakehouse

Real-time vitals streaming + synthetic patient data lakehouse.
Kinesis → Flink SQL → Iceberg → Trino → Superset, all against **[Floci](https://floci.io/floci)**
(a free, tokenless AWS emulator on Docker), provisioned with **Terraform/OpenToFu**.

The stack (docs/):

- **Stream:** synthetic bedside vitals producer → Kinesis — **proven**
- **Reduce:** Flink SQL — `vitals` event sink + `vitals_1m` (TUMBLE 1m) + `vitals_hop_1m` (HOP 30s/1m) → Iceberg — **proven**
- **Serve:** Trino (Iceberg, Nessie catalog) + DynamoDB read-model → Lambda API → Superset dashboards — serving proven; the DDB read-model is designed but not wired to Flink
- **Operate (designed, not yet exercised):** EventBridge Scheduler → daily L4 maintenance/ML export

## Quickstart

```bash
make setup       # .env + uv sync (Python 3.12)
make up          # Floci + Trino + Superset + MySQL + Nessie
make tf-apply    # buckets, streams, Firehose, DynamoDB, Lambdas, API GW, scheduler
make tf-plan     # (review before apply)
make seed        # DDB read-model fixtures so L1/L3 work pre-Flink
make produce     # stream vitals to Kinesis (Ctrl-C to stop, or run in a 2nd shell)
make superset-setup  # provision Trino(Iceberg) DB in Superset (REST)
# Flink 3-sink job (details + troubleshooting: docs/FLINK_OPS.md, docs/GETTING_STARTED.md §5)
docker compose -f docker-compose.yaml -f docker/compose.flink.override.yml \
  up -d flink-jobmanager flink-taskmanager
make build-flink     # build the Flink JAR (host mvn; dockerized fallback in docs/FLINK_OPS.md)
docker cp flink/target/vitals-flink-job-1.0.0-SNAPSHOT.jar vitals-flink-jm:/opt/vitals-flink-job.jar
docker exec vitals-flink-jm flink run -d -c com.healthcare.vitals.VitalsFlinkJob /opt/vitals-flink-job.jar
make workshop-run    # time-series SQL workshop over the live lake (docs/WORKSHOP.md)
```

Then open http://127.0.0.1:8088 (Superset) and `trino --server http://127.0.0.1:8082`
for `sql/demo_queries.sql`. Both ports shift under the Flink override — see below.

> Port note: on Docker Desktop, host ports 8081/8088 are often taken by the
> desktop proxy. `docker/compose.flink.override.yml` republishes Superset on
> **18088**, Trino on **8083**, and the Flink UI on **18081** — the values the
> workshop and docs use.

## Learn the stack

- [docs/GETTING_STARTED.md](docs/GETTING_STARTED.md) — guided first-run walkthrough
  (prereqs, ordering, expected results, common traps) if the cheatsheet above
  isn't enough.
- [docs/FLOCI.md](docs/FLOCI.md) — how the Floci emulator works and how to
  build against it (endpoints, storage modes, per-service behaviour).
- [docs/WORKSHOP.md](docs/WORKSHOP.md) + `sql/workshop/` — runnable time-series
  SQL tour (LAG/LEAD, moving averages, running totals, ranking, CTEs,
  TUMBLE-vs-HOP) against the live lake.
- [docs/FLINK_OPS.md](docs/FLINK_OPS.md) — build/deploy/tune the 3-sink Flink
  job and the failure modes each knob fixes.
- Complementary design docs: [DATA_FLOW.md](docs/DATA_FLOW.md),
  [DATA_MODEL.md](docs/DATA_MODEL.md), [ARCHITECTURE.md](docs/ARCHITECTURE.md),
  [INFRASTRUCTURE.md](docs/INFRASTRUCTURE.md),
  [FOCI_VERIFICATION.md](docs/FOCI_VERIFICATION.md).

## Catalog note

`ICEBERG_CATALOG=nessie` (default) is the proven path. `glue` (Floci Glue Data Catalog)
is selectable but **not yet proven**: Floci lacks Glue `UpdateTable`/`GetPartition`/
`Batch*Partition`, so Trino Iceberg commits may not persist. See
`docs/FOCI_VERIFICATION.md`. Milestone M1/M2 = Glue spike.

## Layout

```
lambdas/  L1 alert SNS · L2 firehose transform · L3 patients API · L4 maintenance
producer/  vitals generator + Kinesis streamer
ml/        patient-based train/val/test split + manifest writer
flink/     Maven SQL job (VitalsFlinkJob + flink/sql/job.sql) — vitals, vitals_1m, vitals_hop_1m sinks
infra/     Terraform/OpenToFu -> Floci (9 modules)
docker/    compose + trino bootstrap + Flink override + vendored Superset dialect patch
scripts/   build_lambdas, smoke/deploy, superset provision, seed, diagnostics
sql/       demo + validation + workshop queries (sql/workshop/*)
tests/     unit (no infra) · integration (Floci) · e2e smoke · fixtures
```

`make help` lists every target.

## CI

Pipeline design (`.gitlab-ci.yml` + `docs/CI_CD.md`) was **removed** —
no CI host is in use yet. Quality is gated locally: `make lint/typecheck`,
`make test-unit`, `make test-floci/test-integration/test-e2e`,
`make build-lambdas/build-flink`, and `make smoke`. A CI host can be added
later (GitHub Actions fits the current remote); `ci/compose.ci.yml` remains
as the Floci-backed integration environment either way.