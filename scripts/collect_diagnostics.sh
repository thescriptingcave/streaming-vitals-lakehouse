#!/usr/bin/env bash
# Collect environment + service diagnostics into diag/ for issue triage.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STAMP="$(date +%Y%m%d-%H%M%S)"
OUT="$ROOT/diag/$STAMP"
mkdir -p "$OUT"

echo "=> diagnostics -> $OUT"
{
  echo "# toolchain ($(uname -srm))"
  docker version --format 'docker {{.Client.Version}} / engine {{.Server.Version}}' 2>&1 || true
  uv --version 2>&1 || true
  (terraform version || tofu version || true) 2>&1
  java -version 2>&1 | head -1 || true
  (mvn -version 2>&1 | head -1 || true)
} > "$OUT/toolchain.txt"

docker compose -f "$ROOT/docker-compose.yaml" ps 2>&1 > "$OUT/services.txt" || true
docker ps --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}' >> "$OUT/services.txt"

for svc in floci nessie trino superset mysql; do
  docker compose -f "$ROOT/docker-compose.yaml" logs --tail 200 "$svc" > "$OUT/logs-$svc.txt" 2>&1 || true
done

if command -v aws >/dev/null 2>&1; then
  export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
  export AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://localhost:4566}"
  aws --output json kinesis list-streams 2>&1 > "$OUT/aws-kinesis.json" || true
  aws --output json dynamodb list-tables 2>&1 > "$OUT/aws-dynamodb.json" || true
  aws --output json s3 ls 2>&1 > "$OUT/aws-s3.json" || true
  aws --output json lambda list-functions 2>&1 > "$OUT/aws-lambda.json" || true
fi

TRINO_URL="${TRINO_URL:-http://127.0.0.1:8082}"
curl -sf --max-time 5 "$TRINO_URL/v1/status" > "$OUT/trino-status.json" 2>&1 || echo "trino unreachable at $TRINO_URL" > "$OUT/trino-status.json"
curl -sf --max-time 5 "http://127.0.0.1:8088/api/v1/health" > "$OUT/superset-health.json" 2>&1 || echo "superset unreachable" > "$OUT/superset-health.json"

echo "done: $OUT"