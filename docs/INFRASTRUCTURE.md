# Infrastructure Design

Status: Draft v1
Date: 2026-09-15

## 1. Terraform/OpenToFu Layout

```
infra/
  providers.tf              # aws provider → endpoint override (Floci)
  backend.tf                # local state (v1); no remote backend
  variables.tf              # env/endpoint/region/account
  main.tf                   # root module composition
  modules/
    s3_lake/                # buckets + lifecycle config
    glue_catalog/           # db + tables (or managed from Trino DDL)
    kinesis/                # stream + shards + firehose
    firehose/               # delivery stream + processor (L2)
    dynamodb/               # alerts, latest_vitals + streams
    lambda/                 # generic Python-zip lambda module
    apigateway/             # API GW v2 + routes + integrations
    events/                 # EventBridge Scheduler rules
    sns/                    # alert topic + subscriptions
  lambda/                   # sources per function (L1–L4)
```

## 2. Environment & Endpoints

Single provider block targets Floci; real-AWS switch = update
`provider`/`endpoint` + creds (documented in the project README).

```tf
provider "aws" {
  region                      = var.region            # us-east-1
  access_key                  = var.access_key        # test
  secret_key                  = var.secret_key        # test
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  endpoints {
    kinesis        = "http://localhost:4566"
    firehose       = "http://localhost:4566"
    dynamodb       = "http://localhost:4566"
    dynamodbstreams= "http://localhost:4566"
    s3             = "http://localhost:4566"
    glue           = "http://localhost:4566"
    lambda         = "http://localhost:4566"
    apigatewayv2   = "http://localhost:4566"
    sns            = "http://localhost:4566"
    scheduler      = "http://localhost:4566"   # EventBridge Scheduler
    iam            = "http://localhost:4566"
  }
}
```

Docker-compose provides `floci` on `:4566`. Same config inside CI only the
host changes (runner-local container hostname).

## 3. Floci Storage Modes

| Mode | Behavior | Use |
|---|---|---|
| `memory` | Data lost on stop | CI ephemeral runs |
| `hybrid` | Async flush ~5s | Local dev default |
| `persistent` | Immediate flush | Debug/stateful sessions |
| `wal` | WAL durability | Future "prod-like" |

Enable via `FLOCI_STORAGE_MODE=<mode>`. Dev uses `hybrid`; CI uses `memory`.

## 4. Runtime Services (docker compose)

| Service | Image | Notes |
|---|---|---|
| `floci` | `floci/floci:latest` | :4566, docker.sock for Lambda |
| `synthea` | openjdk + synthea jar | one-shot generate → loader |
| `flink` | `apache/flink` (or Managed Flink via Floci) | jobmanager+taskmanager |
| `trino` | `trinodb/trino` | iceberg + glue catalogs |
| `superset` | `apache/superset` | UI + API |
| `mysql` | `mysql` | superset metadata |

Optional: `nessie` fallback catalog (only if Glue route fails: M1/M2 spike).

## 5. Mapping AWS services → Floci emulation

| AWS | Floci | Notes |
|---|---|---|
| Kinesis Data Streams | in-process | streams/shards/EFO verified |
| Kinesis Data Firehose | in-process | buffered flush to S3 |
| Lambda | Real Docker | warm pool per runtime |
| DynamoDB + Streams | in-process | streams invoke L1 as event source |
| S3 | in-process | versioning, multipart |
| Glue Data Catalog | in-process | used by Trino Iceberg catalog |
| API Gateway v2 | in-process | routes→Lambda integrations |
| EventBridge Scheduler | in-process | cron → L4 |
| SNS | in-process | topic + Lambda/HTTP subs |
| Secrets Manager | in-process | optional secret storage |
| Managed Flink | Real Docker | optional: skip standalone flink |

## 6. IAM Model (see SECURITY.md for details)

Least-privilege roles per resource:
- `producer-role`: `kinesis:PutRecord`, `kinesis:DescribeStream`
- `flink-role`: `kinesis:Describe/GetRecords`, `s3:*` (lake), `dynamodb:PutItem`,
  `glue:Get*`
- `l2-role`: `s3:PutObject` (raw bucket)
- `l1-role`: `dynamodb:GetRecords/DescribeStream`, `sns:Publish`
- `l3-role`: `dynamodb:GetItem/Query` (read tables only), `logs:CreateLogGroup`
- `l4-role`: `s3:*` (lake+ml), `glue:*`, `dynamodb:ConditionalWrite` (lock)
- `firehose-role`: `s3:PutObject`, KDS read

Floci permits any non-empty creds; roles still defined to keep IaC
AWS-portable and teach correct IAM shape. When running against Floci,
`aws_iam_role`/policy attachments are emulated; no enforcement expected.

## 7. Observability (v1)

- **Logs**: CloudWatch Logs (Floci emulated) per Lambda; JSON structured logs
  with `trace_id`.
- **Metrics**: CloudWatch custom metrics — Kinesis throughput (put count,
  size), Firehose delivery-age, Lambda duration/invocations/errors, DDB
  alerts written/sent, Trino query duration (Superset logs).
- **Dashboard**: CloudWatch dashboard (Floci) + Superset ops dashboard over
  the metrics tables in Iceberg (daily rollup).

## 8. Networking / Deployment Notes

- VPC-less; everything reachable on localhost/container network.
- No TLS in local emulation; real AWS would add TLS by default (endpoint
  swap only).
- CI does not run Superset UI; asserts happen via Trino REST + AWS SDK.
- Terraform state: `local` backend for v1; note for real AWS far later.
  CI applies with `-auto-approve` against a fresh Floci (`memory`).