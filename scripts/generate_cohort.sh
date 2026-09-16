#!/usr/bin/env bash
# Generate the real patient cohort via Synthea (Java) into synthea-output/,
# and emit patient_ids.csv for the producer + loader to reuse (real ids, not
# placeholders). Deterministic by default for reproducible runs.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA="$ROOT/data"
JAR="$DATA/synthea.jar"
OUT="$ROOT/synthea-output"
SYNTHEA_VERSION="${SYNTHEA_VERSION:-master}"
SEED="${SEED:-42}"
POPULATION="${POPULATION:-500}"

mkdir -p "$DATA"
if [[ ! -f "$JAR" ]]; then
  echo "=> downloading Synthea ($SYNTHEA_VERSION)"
  URL="https://github.com/synthetichealth/synthea/releases/download/${SYNTHEA_VERSION}/synthea.jar"
  if ! curl -fL --retry 3 -o "$JAR.tmp" "$URL"; then
    URL="https://raw.githubusercontent.com/synthetichealth/synthea/$SYNTHEA_VERSION/build/libs/synthea.jar"
    curl -fL --retry 3 -o "$JAR.tmp" "$URL"
  fi
  mv "$JAR.tmp" "$JAR"
fi

echo "=> generating cohort (n=$POPULATION, seed=$SEED)"
rm -rf "$OUT"
java -jar "$JAR" \
  -s "$SEED" \
  -p "$POPULATION" \
  --exporter.baseDirectory "$OUT" \
  --exporter.hospital.fhir.export false \
  --exporter.fhir.export true \
  --exporter.csv.export true

# Keep producer/loader on real ids: patient_id column from the CSV export.
awk -F, 'NR==1 {for(i=1;i<=NF;i++) if($i=="Id") c=i} NR>1 && $c!="" {print $1}' \
  "$OUT/csv/patients.csv" > "$OUT/patient_ids.csv" 2>/dev/null \
  || sed -n '2,$p' "$OUT/csv/patients.csv" | cut -d, -f1 > "$OUT/patient_ids.csv"

echo "=> cohort ready: $(wc -l < "$OUT/patient_ids.csv") patients"
echo "   FHIR   : $OUT/fhir"
echo "   ids    : $OUT/patient_ids.csv"
echo "   next   : make load && make produce"