"""Patient loader: Synthea FHIR -> Iceberg via staged Trino writes.

Milestone M1 wires the actual INSERT (staging partition + INSERT OVERWRITE by
batch id per RELIABILITY.md §2). For now this discovers + maps the cohort and
can run idempotently in --dry-run.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from collections import Counter
from pathlib import Path

from patient_data.mapper import rows_from_bundle

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TRINO_HOST = os.environ.get("TRINO_HOST", "127.0.0.1")
TRINO_PORT = os.environ.get("TRINO_PORT", "8082")
TRINO_CATALOG = os.environ.get("TRINO_ICEBERG_CATALOG", "iceberg")
TRINO_SCHEMA = os.environ.get("TRINO_SCHEMA", "healthcare")


def discover_bundles(fhir_dir: Path) -> list[Path]:
    if not fhir_dir.is_dir():
        return []
    return sorted(fhir_dir.glob("fhir/*.json")) or sorted(fhir_dir.glob("*.json"))


def map_all(bundles: list[Path]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for bundle_path in bundles:
        try:
            bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            logger.warning("skipping %s: %s", bundle_path, exc)
            continue
        for table, rows in rows_from_bundle(bundle).items():
            counts[table] += len(rows)
    return counts


def load_to_iceberg(counts: Counter[str], *, dry_run: bool) -> None:
    """INSERT rows into Iceberg (staged by batch). Wired end-to-end in M1."""
    total = sum(counts.values())
    if dry_run:
        logger.info(
            "dry-run: %s rows mapped (%s)", total, ", ".join(f"{k}={v}" for k, v in counts.items())
        )
        return
    # TODO(M1): generate INSERT..VALUES / staged COPY via Trino REST
    # (batch_id = Synthea run; INSERT OVERWRITE against staging partition).
    raise NotImplementedError("Trino loader lands in M1")


def main() -> None:
    parser = argparse.ArgumentParser(description="Load Synthea FHIR into the Iceberg lake.")
    parser.add_argument("--fhir-dir", default=os.environ.get("SYNTHEA_OUTPUT", "synthea-output"))
    parser.add_argument("--dry-run", action="store_true", help="only count rows, no writes")
    args = parser.parse_args()

    bundles = discover_bundles(Path(args.fhir_dir))
    logger.info("found %s FHIR bundles under %s", len(bundles), args.fhir_dir)
    if not bundles:
        raise SystemExit("no Synthea output; run `make synth` first")
    counts = map_all(bundles)
    logger.info("mapped %s tables: %s", len(counts), dict(counts))
    load_to_iceberg(counts, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
