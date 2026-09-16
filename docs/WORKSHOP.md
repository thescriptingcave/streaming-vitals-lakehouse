# Time-Series SQL Workshop

Status: Live (2026-09-16)
Goal: a runnable, hands-on tour of time-series SQL against the *real* lake —
window functions (LAG/LEAD, moving averages, running totals, ranking) and CTE
pipelines over the event-level `vitals` table, plus a TUMBLE-vs-HOP comparison
of the two precomputed window tables.

## Why this exists

The core pipeline (`flink/sql/job.sql`) lands three tables in Iceberg:

| Table | Sink | Why it exists for learners |
|---|---|---|
| `vitals` | raw event rows + `abnormal_flags[]` + `is_abnormal` | inputs for LAG/LEAD, moving averages, CTEs |
| `vitals_1m` | TUMBLE 1 min (non-overlapping) | fixed-bucket summaries |
| `vitals_hop_1m` | HOP 30 s slide / 1 min size (overlapping) | sliding-window summaries, ~2× count |

The workshop queries turn those tables into exercises.

## Run it

```bash
make workshop-run
```

Runs `sql/workshop/00_bootstrap_patients.sql` (creates the `patients`
dimension P0001–P0004 matching the stream) then every `sql/workshop/0[1-7]_*.sql`
against the live Trino (`docker exec vitals-trino trino --catalog iceberg
--schema healthcare`). Each file is self-contained and can be run alone:

```bash
docker exec -i vitals-trino trino --catalog iceberg --schema healthcare \
  < sql/workshop/04_running_totals.sql
```

Prerequisites: the Flink job running (docs/FLINK_OPS.md) and data in `vitals`
(~7k rows for a 20-minute producer session; patients/demos still work with less).

## Query-by-query map

| File | Concept | What to notice |
|---|---|---|
| `00_bootstrap_patients.sql` | dimension bootstrap | P0001–P0004 join keys for 06 |
| `01_time_bucketing.sql` | `DATE_TRUNC` + clock windows | irregular events → fixed grid; `hour()`/`day_of_week()` pivots |
| `02_lag_lead.sql` | `LAG` / `LEAD`, first derivative | per-device deltas and % change; filter `abs(delta) >= 5` |
| `03_moving_average.sql` | `WINDOW` clause, `ROWS BETWEEN` | trailing-3 vs centred-5 frames; name frames once, reuse |
| `04_running_totals.sql` | cumulative `SUM` (`UNBOUNDED PRECEDING`) | per-patient running count → % of session (0–100%) |
| `05_ranking.sql` | `RANK`/`DENSE_RANK`/`NTILE`/`ROW_NUMBER` | tie behaviour differs; deciles; latest-per-bucket |
| `06_chained_ctes.sql` | chained CTEs (4 stages) | flatten → aggregate → score → rank; joins `patients` |
| `07_tumbling_vs_hop.sql` | TUMBLE vs HOP | hop ≈ size/slide × tumbling rows (1.9–2.0×); one event in 2 buckets |

## Key rabbit holes on purpose

- `02` deliberately *doesn't* use `QUALIFY` (BigQuery/Spark syntax) — Trino
  filters the windowed result in a derived table. Note it when reading.
- `05` queries A/B/C each showcase a different ranking family — run them
  separately and diff the top-of-table ties (`RANK` gives 1,2,2,4;
  `DENSE_RANK` 1,2,2,3).
- `07`'s second half proves a single near-edge reading appears in **two** HOP
  buckets (`05:01:00` and `05:01:30` windows) while TUMBLE reports it once.

## Extending

All queries read `iceberg.healthcare.*` through Trino, so you can reuse the
patterns in Superset SQL Lab or `sql/demo_queries.sql`. The abnormal-flag CTE
in `06` (UNNEST over `abnormal_flags`) is a one-liner you'll want again.