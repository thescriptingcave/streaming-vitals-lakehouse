# Getting Started

Status: 2026-09-16. Companion to the README (command cheatsheet) — this is the
guided first-run path with prerequisites, ordering, and expected results.

## TL;DR (nothing installed except Docker + git)

```bash
make setup && make up          # env + Emulators (Floci/Trino/Superset…)
make tf-apply                  # Lake infra: buckets, streams, lambdas, API
make seed                      # DDB fixtures (alerts/latest_vitals)
make produce                   # stream vitals -> Kinesis (leave running, 2nd shell)
# Flink (docs/FLINK_OPS.md top for the WG --, short version):
docker compose -f docker-compose.yaml -f docker/compose.flink.override.yml \
  up -d flink-jobmanager flink-taskmanager
make build-flink
docker cp flink/target/vitals-flink-job-1.0.0-SNAPSHOT.jar vitals-flink-jm:/opt/vitals-flink-job.jar
docker exec vitals-flink-jm flink run -d -c com.healthcare.vitals.VitalsFlinkJob /opt/vitals-flink-job.jar
make workshop-run              # verify + hands-on SQL
make superset-setup            # then http://127.0.0.1:18088 (admin/admin)
```

Fifteen minutes of producer time ⇒ `vitals` ≈ 7 000 rows, `vitals_1m` ≈ 32,
`vitals_hop_1m` ≈ 60 (≈2× the tumbling one — HOP overlap). If the tables stay
empty, the job is running but not checkpointing — see FLINK_OPS.md.

## 1. Prerequisites

| Thing | Why / requirement |
|---|---|
| Docker Desktop (or engine) | runs the whole stack; macOS arm64 expected (Rosetta OK) |
| git | clone |
| `uv` (Python 3.12) | Python env for scripts/tests (`make setup` creates `.venv`) |
| Maven | optional — with host `mvn` use `make build-flink`; otherwise use the dockerized build in docs/FLINK_OPS.md |
| Ports | 4566 (Floci), 8082/8081 (Trino/JM base), 8088 (Superset); if Docker Desktop squats on 8081/8088, use the Flink override which moves them (see §4) |

## 2. First boot

```bash
git clone <repo> && cd healthcare-realtime-vitals-lakehouse
make setup        # .env.example -> .env (edit ports/STREAM names if desired), uv sync
make up           # Floci + Trino + Superset + MySQL + Nessie (may take ~2 min first run)
make status       # container health per service
```

Expected: `make status` green for all core services; Trino querying works:

```bash
docker exec vitals-trino trino --catalog iceberg --schema healthcare \
  --execute "SHOW TABLES"
```

`SHOW SCHEMAS` on the `iceberg` catalog shows `healthcare` (Nessie is the
catalog — `ICEBERG_CATALOG=nessie` in `.env`; the Glue option is a not-yet-proven spike).

## 3. Provision the lake (Terraform/OpenToFu → Floci)

```bash
make tf-plan     # review the plan (buckets, streams, Firehose, DDB, Lambdas, API GW, scheduler)
make tf-apply    # idempotent; re-running is safe
make seed        # DDB read-model fixtures so the patients API (L3) works pre-Flink
```

Expected: resources appear in Floci (`aws --endpoint-url http://127.0.0.1:4566 kinesis list-streams`
shows `vitals`; S3 shows `healthcare-lake`). No real AWS accounts, creds, or billing involved —
see docs/FLOCI.md for why a URL swap is all that changes for real AWS.

## 4. Streaming path — Flink 3-sink job

```bash
docker compose -f docker-compose.yaml -f docker/compose.flink.override.yml \
  up -d flink-jobmanager flink-taskmanager     # standalone Flink 1.19 (override = fixed ports/resources)
make produce                                    # stream vitals -> Kinesis (leave running in a 2nd shell)

make build-flink                                # mvn package (host mvn; dockerized fallback in FLINK_OPS.md)
docker cp flink/target/vitals-flink-job-1.0.0-SNAPSHOT.jar vitals-flink-jm:/opt/vitals-flink-job.jar
docker exec vitals-flink-jm flink run -d \
  -c com.healthcare.vitals.VitalsFlinkJob /opt/vitals-flink-job.jar
```

Expected: three jobs RUNNING in the Flink UI (`http://127.0.0.1:18081`) —
`vitals`, `vitals_1m`, `vitals_hop_1m`. Wait ≥ ~1 min (checkpoint cadence 10 s),
then verify Iceberg got the data:

```bash
docker exec vitals-trino trino --catalog iceberg --schema healthcare --execute \
  "SELECT 'vitals' t, count(*) FROM vitals
   UNION ALL SELECT 'vitals_1m', count(*) FROM vitals_1m
   UNION ALL SELECT 'vitals_hop_1m', count(*) FROM vitals_hop_1m"
```

The windowed tables publish **only on checkpoints** — empty tables mean no
checkpoints, not no data. Redeploy/cancel/troubleshooting (including the
`java -jar` trap and the `flink cancel` CLI bug) in docs/FLINK_OPS.md.

## 5. Verify by doing — workshop + dashboard

```bash
make workshop-run    # time-series SQL tour over the live lake (docs/WORKSHOP.md)
make superset-setup  # idempotent: creates Trino-Iceberg DB, dataset, dashboard, charts
```

Then open `http://127.0.0.1:18088` (starting Super Superset — if handed 8088 by
Docker Desktop, the override moves it) with `admin/admin`: dashboard "Vitals
Live" (id 1) plots the windowed aggregates. If using the *base* compose instead
of the override, these URLs are 8088/8082 instead.

## 6. Day-to-day & teardown

```bash
make status          # health
make diag            # diagnostics dump (streams, buckets, tables, row counts)
make tf-apply        # re-apply infra after editing infra/ modules
make reset           # clean Floci state + re-provision (idempotent)
make down            # stop containers (keep volumes)
make down-clean      # stop + remove volumes (full state wipe)
```

## Common first-run traps

- `make tf-apply` before `make up` → 0 Lambdas / no stream: bring the stack up
  first (resources are provisioned *into* Floci).
- Jobs RUNNING but tables empty → checkpoints disabled (see §4); also confirm
  the producer is running (a healthy replay shows up from `TRIM_HORIZON` within
  ~30 s thanks to Floci's 24 h retention).
- Port conflicts on 8081/8088 → add the override (`docker/compose.flink.override.yml`)
  which republishes exactly what the docs/workshop assume (18081/18088/8083).
- `java -jar vitals-flink-job.jar` → `ClassNotFoundException: EnvironmentSettings`
  — submit through the Flink CLI (¶ above), never `java`.