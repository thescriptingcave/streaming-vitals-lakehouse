# Flink Ops — build, deploy, tune, troubleshoot

Status: Live (2026-09-16), field notes from running the 3-sink pipeline on a
macOS/Docker-Desktop laptop against Floci.

The job is one Maven project (`flink/`) whose entry point reads
`job.sql` from the classpath and submits each SQL statement:
`DDLs → vitals INSERT (event sink) → vitals_1m INSERT (TUMBLE) →
vitals_hop_1m INSERT (HOP)`. Running `flink run` with this JAR yields
**three independent streaming jobs** (one per streaming INSERT).

## Stack facts

- JM REST/UI: `http://127.0.0.1:18081` (container `vitals-flink-jm`, Flink 1.19).
- Profile-gated services in `docker-compose.yaml`; the working local config is
  `docker/compose.flink.override.yml`:

  ```bash
  docker compose -f docker-compose.yaml -f docker/compose.flink.override.yml \
    up -d flink-jobmanager flink-taskmanager
  ```

- Overrides that matter (each maps to a failure mode below): ports (18081,
  avoids Docker Desktop's own 8081), Hadoop shaded jar → `/opt/flink/lib`,
  `taskmanager.numberOfTaskSlots: 3`, `taskmanager.memory.jvm-metaspace.size: 512m`.
- **`java -jar` does NOT work** for submission: the fat JAR marks Flink API
  classes as `provided`, and plain `java` lacks `EnvironmentSettings`
  (`ClassNotFoundException`). Always submit via the CLI:

  ```bash
  docker exec vitals-flink-jm flink run -d \
    -c com.healthcare.vitals.VitalsFlinkJob /opt/vitals-flink-job.jar
  ```

- `flink cancel` is **broken** in this image (`NoSuchMethodError:
  org.apache.commons.cli.CommandLine.hasOption`). Cancel via REST:

  ```bash
  curl -X PATCH "http://127.0.0.1:18081/jobs/<jid>" -d ''      # empty body
  ```

  Watch states with `curl -s http://127.0.0.1:18081/jobs/overview`.

## Build & deploy loop

```bash
# 1. edit flink/sql/job.sql  →  sync to resources  →  package (host mvn optional)
cp flink/sql/job.sql flink/src/main/resources/job.sql
docker run --rm -v "$PWD":/work -v "$HOME/.m2":/root/.m2 -w /work \
  maven:3.9.9-eclipse-temurin-17 mvn -q -f flink/pom.xml package
#    (or `make build-flink` if mvn is on the host)

# 2. ship the JAR into the JM container
docker cp flink/target/vitals-flink-job-1.0.0-SNAPSHOT.jar \
  vitals-flink-jm:/opt/vitals-flink-job.jar

# 3. cancel the old jobs (all three, by jid) ...
curl -s http://127.0.0.1:18081/jobs/overview
curl -X PATCH http://127.0.0.1:18081/jobs/<jid> -d ''

# 4. ... then clear the tables so TRIM_HORIZON replay can't duplicate rows
docker exec vitals-trino trino --catalog iceberg --schema healthcare \
  --execute "DROP TABLE IF EXISTS vitals; \
             DROP TABLE IF EXISTS vitals_1m; \
             DROP TABLE IF EXISTS vitals_hop_1m;"

# 5. resubmit
docker exec vitals-flink-jm flink run -d \
  -c com.healthcare.vitals.VitalsFlinkJob /opt/vitals-flink-job.jar
```

The three jobs recreate their tables (DDL is `CREATE TABLE IF NOT EXISTS`).

## Verify

```bash
docker exec vitals-trino trino --catalog iceberg --schema healthcare --execute \
  "SELECT 'vitals' t, count(*) FROM vitals
   UNION ALL SELECT 'vitals_1m', count(*) FROM vitals_1m
   UNION ALL SELECT 'vitals_hop_1m', count(*) FROM vitals_hop_1m"
```

Expect `vitals` ≈ thousands of rows and `vitals_hop_1m` ≈ 2 × `vitals_1m`
(the HOP overlap; see sql/workshop/07). A healthy 20-minute replay gives
`32 / 60 / 7 324`-ish.

## Failure modes we hit (and the knob that fixes them)

| Symptom | Root cause | Fix |
|---|---|---|
| Third job stuck `RESTARTING`, root cause `NoResourceAvailableException` | 3 jobs × parallelism 2 need 6 slots, TM has 2 | parallelism → 1 (`table.exec.resource.default-parallelism` in `VitalsFlinkJob`) + slots → 3 |
| TM container exits, log ends `OutOfMemoryError: Metaspace` | every task reloads the fat JAR's (Iceberg + AWS SDK) classes; 256m default leaks across resubmissions | `taskmanager.memory.jvm-metaspace.size: 512m` |
| TM exit 137 (SIGKILL) under Rosetta | 4 slots too heavy for the emulation | keep 3 slots (`docker/compose.flink.override.yml`) |
| Tables exist but stay **empty** while jobs show RUNNING | Iceberg publishes via `IcebergFilesCommitter` **on checkpoints only**; default = checkpoints disabled | `execution.checkpointing.interval = 10000ms` in `VitalsFlinkJob` |
| Zero rows even after redeploy | producer stopped + reading nothing retained (or a position prop that isn't honored: `scan.startup.mode` silently falls back to LATEST) | keep `make produce` running; TRIM_HORIZON needs retained data (Floci retention 24 h); use `scan.stream.initpos` |
| SQL parse error on `->` lambda | Calcite didn't accept `x -> x IS NOT NULL` in this context | avoided lambdas: chained `ARRAY_REMOVE(..., '')` |

## Source schema rules (don't "fix" these)

- `vitals_source` lives in the **default catalog** — an Iceberg catalog table
  cannot declare a watermark (Iceberg rejects watermark specs).
- `event_time`/`ingestion_time` are `TIMESTAMP_LTZ(3)` — the Kinesis JSON is
  ISO-8601 UTC (`...Z`), which Flink's JSON parser only accepts for LTZ.
- The Kinesis connector reads its position prop as `flink.stream.initpos`
  (`TRIM_HORIZON | LATEST | AT_TIMESTAMP`). `scan.startup.mode` is **not**
  honored and silently falls back to LATEST.