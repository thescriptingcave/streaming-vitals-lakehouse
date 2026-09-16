-- 04 · Running totals — cumulative aggregates over ordered frames.
--
-- SUM(x) OVER (... ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) gives a
-- monotonically growing total per partition: the canonical "how far along are
-- we" for dashboards. Shown here on per-minute reading counts:
--
--   readings    : the per-bucket count (grouped first)
--   cumulative  : running total of readings within the patient's session
--   pct_of_session: same total re-based as a share of the patient's total,
--                 so the "coverage so far" curve for each patient runs 0→100.

SELECT
    patient_id,
    date_trunc('minute', event_time)          AS bucket_ts,
    count(*)                                  AS readings,

    sum(count(*)) OVER w_accum               AS cumulative,
    round(
        100.0 * sum(count(*)) OVER w_accum
              / NULLIF(sum(count(*)) OVER (PARTITION BY patient_id), 0), 1
    )                                         AS pct_of_session
FROM iceberg.healthcare.vitals
GROUP BY patient_id, date_trunc('minute', event_time)
WINDOW w_accum AS (
    PARTITION BY patient_id
    ORDER BY date_trunc('minute', event_time)
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)
ORDER BY patient_id, bucket_ts
LIMIT 25;