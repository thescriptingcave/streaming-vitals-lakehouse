# CI/CD Design (GitLab)

Status: Draft v1
Date: 2026-09-15

## 1. Pipeline Overview

Stages:

```
lint → unit → integration → build → deploy
```

All jobs run on the shared GitLab runner with Docker executor
(`docker:24` image; runner shell usable for compose). No secrets required —
Floci is free and tokenless (§ of SECURITY).

## 2. Jobs

| Stage | Job | Command/notes |
|---|---|---|
| lint | `lint-python` | `uv run ruff check . && uv run mypy .` |
| lint | `lint-terraform` | `terraform fmt -check -recursive; tflint` (if available in CI) |
| lint | `lint-secrets` | grep banned patterns (AKIA…, passwords) |
| unit | `unit-producer` | pytest tests/unit (no infra) |
| unit | `unit-lambdas` | invoke handlers against mocks (no Floci) |
| unit | `unit-model` | schema/model builders, SQL string tests |
| integration | `it-stream` | Floci compose; producer→Kinesis→Firehose→L2→Trino asserts |
| integration | `it-alert` | seed DDB high-HR events; assert SNS messages via L1 |
| integration | `it-api` | API GW→L3 read flows (GET latest, alerts) |
| integration | `it-maintenance` | L4 lock+export smoke (write to ml/, verify manifest) |
| build | `build-lambdas` | zip L1–L4 (versioned artifact names w/ hash) |
| build | `build-flink` | docker image for Flink job (jars in image) |
| deploy | `deploy-tofu` | `tofu apply -auto-approve` infra against Floci |

Parallelization: lint/unit run in parallel; integration depends on unit;
build depends on integration; deploy depends on build.

## 3. Resources (GitLab)

- `.gitlab-ci.yml` at repo root; `ci/compose.ci.yml` for integration services.
- **Variables** (non-secret): `AWS_ENDPOINT_URL`, `AWS_REGION`,
  `SUPERSET_URL`, `STREAM_NAMES` etc. — group-level defaults, override per-pipeline.
- **Artifacts**: lambda zips + hashes (reused in deploy), pytest XML,
  coverage report. `artifacts:reports:coverage` aggregates.
- **Cache**: `uv` dependency cache keyed by `uv.lock` hash; Go/Terraform
  plugin caches under `$CI_PROJECT_DIR/.cache`.

## 4. Integration Stage (Floci in CI)

```yaml
integration:
  before_script:
    - docker compose -f ci/compose.ci.yml up -d --wait
  script:
    - export AWS_ENDPOINT_URL=http://localhost:4566
    - pytest tests/integration -m "floci" -x --junitxml=report.xml
  after_script:
    - docker compose -f ci/compose.ci.yml down -v
```

`docker-compose.ci.yml` services (memory storage, no Superset UI):

```
floci (/var/run/docker.sock), flink, trino, nessie(optional fallback)
```

Trino awaited via healthcheck; then seed fixtures
(`scripts/seed_fixtures.py`) → run assertions.

`testcontainers-floci` used for pesky-invisible unit/integration splits
(isolated S3/Kinesis per test when serial state is undesirable).

## 5. Deploy Stage

- `tofu apply` executes against a fresh Floci (memory) instance in the job.
  Applies Terraform only — Lambda definitions, DDB, streams, GW, rules.
- After apply: health-check route `GET /patients/…` returns 200; one alert
  fixture written → SNS message captured.
- Real-AWS deploy is intentionally **not** in this pipeline (v1 scope is
  local/CI emulation) — the same `infra/` and jobs can be re-pointed later.

## 6. Branch Strategy & Merging

- `main` is protected: MR (merge request) required, 1 approval; pipeline must
  pass all stages before merge.
- Feature branches from `main`; short-lived; MR title drives changelog
  (Conventional Commits).
- No tags in v1; releases via CI when needed (semantic-release optional
  later — defer).
- Nightlies: `schedule` pipeline runs integration on `main` at 02:00 to
  catch Floci image drift (pinned tags still drift upstream).

## 7. Pipeline Triggers

| Trigger | Runs |
|---|---|
| push to feature branch | lint, unit |
| MR to main | lint, unit, integration, build |
| push to main (post-merge) | all incl. deploy |
| schedule (nightly) | full on main |
| manual (`play`) | deploy only (re-run smoke) |

## 8. Observability of CI

- JUnit XML + coverage badges; failed-job Slack/GitLab notification
  (out-of-band for v1, optional).
- `ci/compose.ci.yml` health checks gate `--wait` so flaky-start races are
  caught as pipeline failures, not silent skips.
- Artifacts retained 7 days (zips 30 days for debugging).