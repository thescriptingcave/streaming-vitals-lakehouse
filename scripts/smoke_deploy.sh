#!/usr/bin/env bash
# Post-deploy smoke test against the Floci stack (CI deploy stage).
# Verifies Kinesis, DDB, SNS and the packaged Lambdas answered by tofu apply.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -f "$ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env"
  set +a
fi

ENDPOINT="${AWS_ENDPOINT_URL:-http://localhost:4566}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export AWS_ENDPOINT_URL="$ENDPOINT"

failures=0
pass() { printf 'PASS  %s\n' "$1"; }
fail() { printf 'FAIL  %s -> %s\n' "$1" "$2"; failures=$((failures + 1)); }

S=$(aws --output json kinesis list-streams 2>/dev/null || true)
if [[ "$S" == *"vitals"* ]]; then pass "kinesis stream vitals"; else fail "kinesis stream vitals" "$S"; fi

T=$(aws --output json dynamodb list-tables 2>/dev/null || true)
for table in alerts latest_vitals maintenance-lock; do
  if [[ "$T" == *"$table"* ]]; then pass "dynamodb table $table"; else fail "dynamodb table $table" "$T"; fi
done

N=$(aws --output json sns list-topics 2>/dev/null || true)
if [[ "$N" == *"vitals-alerts"* ]]; then pass "sns topic vitals-alerts"; else fail "sns topic vitals-alerts" "$N"; fi

# Invoke L2 with a Firehose event: expect a single {Ok, base64} response.
L2_EVENT='{"records":[{"recordId":"smoke-1","data":"eyJwYXRpZW50X2lkIjogIlBPT0xDMDExIiwgImRldmljZV9pZCI6ICJiZWQtMDAxIiwgImV2ZW50X3RpbWUiOiAiMjAyNi0wOS0xNVQwMDowMDowMFoiLCAiaGVhcnRfcmF0ZSI6IDgyLjAsICJzcG8yIjogOTguMCwgInN5c3RvbGljX2JwIjogMTE4LjAsICJkaWFzdG9saWNfYnAiOiA3Ni4wLCAidGVtcGVyYXR1cmUiOiAzNy4wLCAicmVzcF9yYXRlIjogMTYuMCwgInRyYWNlX2lkIjogInNtb2tlLXQifQ=="}]}'
L2_OUT=$(mktemp)
if aws lambda invoke --function-name l2_firehose_transform --cli-binary-format raw-in-base64-out --payload "$L2_EVENT" "$L2_OUT" >/dev/null 2>&1; then
  if grep -q '"Ok"' "$L2_OUT"; then pass "lambda l2_firehose_transform (Ok)"; else fail "lambda l2_firehose_transform" "$(cat "$L2_OUT")"; fi
else
  fail "lambda l2_firehose_transform" "invoke failed"
fi
rm -f "$L2_OUT"

# L4 export bootstrap must run and write a manifest.
L4_EVENT='{"detail":{"run_date":"2026-09-15"}}'
L4_OUT=$(mktemp)
if aws lambda invoke --function-name l4_scheduled_iceberg --cli-binary-format raw-in-base64-out --payload "$L4_EVENT" "$L4_OUT" >/dev/null 2>&1; then
  if grep -qE '"skipped"[[:space:]]*:[[:space:]]*false' "$L4_OUT"; then pass "lambda l4_scheduled_iceberg (export)"; else fail "lambda l4_scheduled_iceberg" "$(cat "$L4_OUT")"; fi
else
  fail "lambda l4_scheduled_iceberg" "invoke failed"
fi
rm -f "$L4_OUT"

if [[ "$failures" -gt 0 ]]; then
  echo ">>> $failures smoke check(s) FAILED (endpoint=$ENDPOINT)"
  exit 1
fi
echo ">>> all smoke checks passed (endpoint=$ENDPOINT)"