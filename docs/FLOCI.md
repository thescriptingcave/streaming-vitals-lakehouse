# Floci — how the emulator works and how to build against it

Status: Live (2026-09-16). Capability-by-capability verification notes live in
[FOCI_VERIFICATION.md](./FOCI_VERIFICATION.md); this doc is the "mental model
for building" guide.

## The one-paragraph mental model

Floci is a **single process that emulates the AWS control + data planes in
memory**, exposed as one HTTP endpoint (`:4566`), while **deliberately
re-using real Docker for the heavy runtimes** — Lambda and Flink run as actual
containers that Floci manages through your mounted `/var/run/docker.sock`.
"Building against Floci" is building against the **real AWS API surface**, so
the same code/terraform is portable to AWS later; what you give up in the
emulator is enforcement, durability and latency (never wire fidelity for the
services we use).

```
your code / terraform / boto3        (account 000000000000, region us-east-1)
        │  endpoint = http://<floci>:4566
        ▼
┌─────────────────── FLOCI ────────────────────┐
│  in-process emulations                        │
│   Kinesis · Firehose · S3 · DynamoDB+Streams │
│   SNS · API GW v2 · EventBridge · Glue · IAM │
│   CloudWatch Logs                            │
└──────────────┬────────────────────────────────┘
               │ docker.sock
               ▼
      real Docker containers
       Lambda (warm pool) · Managed Flink
       floci-duck (Firehose→Parquet sidecar)
```

## Addressing & configuration vocabulary

| Concept | Value in this repo |
|---|---|
| Endpoint | `http://127.0.0.1:4566` from the host, `http://floci:4566` in-network (`FLOCI_HOSTNAME: floci`, compose service name) |
| Credentials | any non-empty pair (`test`/`test`); enforcement disabled (`FLOCI_SERVICES_IAM_ENFORCEMENT_ENABLED=false`) |
| Region / account | `us-east-1` / `000000000000`; namespaces are per-account→per-region |
| Storage | `FLOCI_STORAGE_MODE`: `memory` (CI) / `hybrid` (~5 s async flush, local default) / `persistent` / `wal` |
| Data dir | `./data/floci` (compose volume) — the emulated AWS state survives restarts in `hybrid` |

All behaviour toggles are `FLOCI_*` env vars (see `docker-compose.yaml` service
`floci`: `FLOCI_SERVICES_UI_ENABLED=false`, Firehose flush tuning, etc.).
State inspect commands work with any AWS client — this repo standardises on
`lambdas/common/aws.py` (boto3 + `AWS_ENDPOINT_URL`), e.g. listing stream data:

```bash
aws --endpoint-url http://127.0.0.1:4566 kinesis list-streams
AWS_ENDPOINT_URL=http://127.0.0.1:4566 uv run python -m producer.producer   # make produce
```

## Service-by-service: how it behaves here

| Service | Emulation | Behaviour we rely on |
|---|---|---|
| Kinesis | in-process | shards + EFO; **24 h retention** (so `TRIM_HORIZON` replays work); producer `PutRecords` |
| Firehose | in-process | buffered flush (interval honored, ~10 s tick); **Lambda transform via `ProcessingConfiguration`**; optional Parquet via `floci-duck` sidecar |
| Lambda | real Docker | the function code runs in a real container (warm pool per runtime); Kinesis/DDB-Streams event-source mappings |
| DynamoDB + Streams | in-process | streams → Lambda (L1) event source |
| SNS | in-process | topic + Lambda/HTTP subs |
| API Gateway v2 | in-process | full data plane; **data plane addressed by hostname** `{apiId}.execute-api.localhost.floci.io:4566` — pin ids with `tags = { "floci:override-id" = "patients" }` |
| EventBridge Scheduler | in-process | cron/rate/at; **10 s tick** (fine for daily L4, not sub-minute) |
| Glue Data Catalog | in-process | **gap**: no `UpdateTable`/`GetPartition(batch)`… → Iceberg commits via Trino may not persist ⇒ dev catalog is **Nessie** (`ICEBERG_CATALOG=nessie`); Glue = M1/M2 spike |
| IAM | in-process | roles/policies recorded, **not enforced**; still defined so IaC is AWS-portable |
| CloudWatch Logs | in-process | caps stored events (~20 k/account) — enough for CI |

## How THIS repo builds against it (concrete)

1. **Produce** — boto3 → `http://127.0.0.1:4566` (`make produce`, `producer/producer.py`).
2. **Provision** — OpenToFu/Terraform with every `endpoints.{}` block overridden
   to Floci (`infra/providers.tf`), applied with `-auto-approve`
   (`make tf-apply`). Same IaC targets real AWS later — only the provider block changes.
3. **Stream/consume in-network** — containers address Floci by compose DNS:
   Trino (`FLOCI_ENDPOINT=http://floci:4566` for S3/Nessie), the Flink Kinesis
   connector (`'aws.endpoint' = 'http://floci:4566'` in `job.sql`, plus the
   `FLINK_AWS_ENDPOINT_URL` → `flink.stream.kinesis.endpoint` override in
   `VitalsFlinkJob`), Lambda sidecars same host.
4. **Verify** — integration tests spin up Floci via `testcontainers-floci`
   (`from floci import FlociContainer`); CI does the same ephemerally
   (`ci/compose.ci.yml`, `FLOCI_STORAGE_MODE=memory`). See `tests/integration/`
   (`test_stream`, `test_alerts`, `test_api`, `test_maintenance`) and
   `make test-integration` / `make test-floci`.

## What the "implementation" question means for your learning

There is no per-service business logic you author for Floci — you author normal
AWS clients. What Floci teaches is the **conventions**:

- SDK/CLI + `endpoint_url`; credentials passed but unenforced.
- In-network callers use the compose service name (`floci`), host callers use
  `127.0.0.1:4566`; both just change a URL.
- The emulator's durability/retention/latency differ from AWS (24 h Kinesis
  retention, 10 s scheduler tick, firehose flush tuning, `hybrid` storage), so
  pipelines you think of as "instant" need to be exercised with these
  cadences in mind.
- Where an emulation has a gap (Glue `UpdateTable`), the design compensates
  (Nessie fallback catalog) — flagging those gaps is part of the verification
  process recorded in FOCI_VERIFICATION.md.

## Using the emulator to debug

- `docker logs vitals-floci` — request-level logs for every emulated call.
- `make reset` — wipe Floci state, re-apply infra (`Makefile` target).
- Storage state lives under `./data/floci` in `hybrid`; deleting it resets the
  emulated "account".
- Real-AWS readiness: the whole stack is a URL swap away for services with no
  emulation gap (everything except the Glue-catalog spike).