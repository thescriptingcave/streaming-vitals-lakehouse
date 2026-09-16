# Security Design

Status: Draft v1
Date: 2026-09-15
Scope note: v1 targets Local/CI against Floci with synthetic data. This doc
defines the IAM/secret/API posture that transfers to real AWS when the
endpoint is swapped. **No real PHI is used or stored.**

## 1. Threat Model (v1)

| Asset | Risk | Tier |
|---|---|---|
| Synthetic patient dataset | Misuse/minor re-identification (fabricated data) | Low |
| Credentials/endpoints | Misspent secrets if leaked | Low→Med |
| Lambda handler code | Logic bugs → data corruption (no external attacker) | Low |
| REST API (L3) | Unauthenticated access to read-model | Med |
| S3 lake | Accidental delete/overwrite | Low→Med |
| Flink job JAR | Untrusted code exec (dev only) | Low |

Tier Low = local dev-only; no hardening beyond tidy IAM shape. Med = design
now, enforce when moving to real AWS.

## 2. IAM Model (Least Privilege)

Per-role statements in `infra/modules/*`. Floci emulates IAM without real
enforcement — the value is AWS-portability + correct shape.

| Role | Allows | Denies |
|---|---|---|
| `role.producer` | kinesis:PutRecord, DescribeStream | everything else |
| `role.firehose` | KDS Read, firehose:* own delivery stream, s3:PutObject (raw bucket) | — |
| `role.flink` | kinesis:DescribeStream/GetRecords, s3:* on lake prefix, dynamodb:PutItem (alerts/latest), glue:GetDatabase/GetTable/UpdateTable | iam:*, kms:* |
| `role.lambda-alerts` (L1) | dynamodb:GetRecords/GetShardIterator/DescribeStream (alerts table), sns:Publish (topic), sqs:SendMessage (DLQ) | others |
| `role.lambda-transform` (L2) | s3:PutObject (raw), logs:* | others |
| `role.lambda-api` (L3) | dynamodb:GetItem/Query (alerts, latest_vitals) | write ops |
| `role.lambda-maintenance` (L4) | s3:* (lake+ml), glue:* (own catalog), dynamodb:ConditionalWrite (lock), logs:* | iam:* |
| `role.trino` | s3:GetObject (lake), glue:GetTable/GetPartitions | others |
| `role.superset` | (transport only — uses Trino creds) | — |

Policy boundaries: all statements scoped to ARN-prefixed resources
(`arn:aws:s3:::healthcare-lake/iceberg/*`, etc.), never `*` on buckets.

## 3. Secrets Management

- **No secrets in repo**. `.env.example` documents var names; real `.env`
  gitignored (mirrors previous project conventions).
- Credentials: dummy (`test`/`test`) against Floci; region `us-east-1`.
- Runtime secret reads (future AWS): Secrets Manager via
  `boto3.client("secretsmanager")`; Lambda env vars only for non-secret
  config (table names, topic ARNs).
- Trino/Superset passwords: default dev creds documented; overridable via env.
- GitLab CI designs were removed; no CI host is in use (local `make` gates).
  If a CI system is added later: masked+protected secret variables, never in
  pipeline files. Floci needs no secrets anyway.

## 4. Data Handling

- **Synthetic only**. Synthea generator output is fully fabricated; names,
  dates, locations are not real people. Note this in README/demo materials.
- No PHI/de-identification step required for generated data; if real data is
  ever ingested, halt and re-architect (out of v1 scope by design).
- Lake buckets default-blocked (no public/default read). v1 has no external
  access to the lake at all (localhost).
- ML exports: same synthetic pipeline; manifest never includes raw notes/
  free text beyond schema columns.

## 5. API Security (L3)

- API Gateway v2 **lambda authorizer (API key)** protecting all routes —
  part of the Lambda learning path.
- Request validation: path/query types, length caps; reject
  non-`patient_id` patterns (400). Unknown ids → 404, no data leakage.
- Responses: JSON with `Cache-Control: no-store` (read model).
- Optional rate limiting at authorizer level (listed as later extension in
  Floci; gate in real AWS).

## 6. Service Security

- **VPC-less in v1** (localhost/containers). On real AWS: VPC + security
  groups + VPC endpoints for Lambda→S3/DDB (documented, not built).
- Trino: no-auth dev mode locally; credentials bound via env.
  Superset: admin defaults from env; flip in CI to ephemeral random password.
- TLS: none in local emulation; real AWS enables by default at endpoint swap.
- Fronting public internet: CI runners and dev machines only;
  no published ports beyond compose defaults.

## 7. CI/CD Supply Chain

- Images consumed from official registries (`floci/floci`, `trinodb/trino`,
  `apache/flink`, `apache/superset`, `mysql`); pinned tags (`latest` nightly
  for floci with documented drift risk — pin a version in CI for
  reproducibility: `floci/floci:2.x.y`).
- Lambda zip artifacts built in CI with hashes recorded in CI artifacts and
  Terraform `source_code_hash` → detect drift.
- `git-secrets`-style guard: banned patterns (`AKIA`, keywords) in a CI lint
  job (ruff + custom grep).
- Dependabot equivalents for Python deps (`pyproject.toml`/`uv.lock`).

## 8. Logging & Audit

- JSON structured logs everywhere (producer, Lambdas, Flink, Trino).
- CloudWatch Logs (Floci) retention 7 days CI/dev; real AWS 30–90 days.
- Audit trail for L4 maintenance actions (what ran, run_date, lock holder).
- No secrets or full payload bodies in logs (redact patient_id if ever real;
  synthetic ids are fine).

## 9. Open Items

- Authorizer enforcement on Floci API GW (verify); fallback: reverse-proxy
  API key in front (nginx) for L3 demo.
- S3 bucket policy denies — confirm Floci honors `aws_s3_bucket_policy`.
- Secrets rotation story when/if moved to real AWS.