# Healthcare Realtime Vitals Lakehouse

Real-time vitals streaming + synthetic patient data lakehouse.
Kinesis → Flink SQL → Iceberg → Trino → Superset, all against **[Floci](https://floci.io/floci)**
(a free, tokenless AWS emulator on Docker), provisioned with **Terraform/OpenToFu**.

Working with `naive-pdf-vitals`? The intended stack (docs/) is:

- **Stream:** synthetic bedside vitals producer → Kinesis
- **Batch:** Synthea (real patient cohort, FHIR) → Iceberg via Trino
- **Reduce:** Flink SQL tumbling-window aggregates + alert rules
- **Serve:** Trino (Iceberg, Nessie catalog) + read-model DynamoDB → Lambda API → Superset dashboards
- **Operate:** EventBridge Scheduler → daily L4 maintenance/ML export (advisory lock)

## Quickstart

```bash
make setup       # .env + uv sync (Python 3.12)
make up          # Floci + Trino + Superset + MySQL + Nessie
make tf-apply    # buckets, streams, Firehose, DynamoDB, Lambdas, API GW, scheduler
make tf-plan     # (review before apply)
make synth       # Synthea cohort (Java) -> synthea-output/fhir
make load        # FHIR -> Iceberg via Trino (patient_data loader)
make seed        # DDB read-model fixtures so L1/L3 work pre-Flink
make produce     # stream vitals to Kinesis (Ctrl-C to stop)
make superset-setup  # provision Trino(Iceberg) DB in Superset (REST)
```

Then open https://localhost:8443 (Superset) and `trino --server http://127.0.0.1:8082`
for `sql/demo_queries.sql`.

## Catalog note

`ICEBERG_CATALOG=nessie` (default) is the proven path. `glue` (Floci Glue Data Catalog)
is selectable but **not yet proven**: Floci lacks Glue `UpdateTable`/`GetPartition`/
`Batch*Partition`, so Trino Iceberg commits may not persist. See
`docs/FOCI_VERIFICATION.md`. Milestone M1/M2 = Glue spike.

## Layout

```
lambdas/  L1 alert SNS · L2 firehose transform · L3 patients API · L4 maintenance
producer/  vitals generator + Kinesis streamer
patient_data/ Synthea FHIR mapper + Trino loader
ml/        patient-based train/val/test split + manifest writer
flink/     Maven SQL job (VitalsFlinkJob + flink/sql/job.sql)
infra/     Terraform/OpenToFu -> Floci (9 modules)
docker/    compose + trino bootstrap + vendored Superset dialect patch
scripts/   build_lambdas, cohort, smoke/deploy, superset provision, seed, diagnostics
sql/       Trino demo + validation queries
tests/     unit (no infra) · integration (Floci) · e2e smoke · fixtures
```

`make help` lists every target.

## CI

GitLab pipeline (`lint → unit → integration(Floci) → build → deploy`) in
`.gitlab-ci.yml` — ephemeral Floci, pinned images, no secrets.