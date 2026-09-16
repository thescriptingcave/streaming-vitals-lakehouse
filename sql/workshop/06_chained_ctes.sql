-- 06 · Chained CTEs — a small read-model pipeline.
--
-- A CTE (WITH name AS (...)) is a named subquery the planner can resolve ONCE
-- and reuse. Chaining several models the way a streaming read-model stacks
-- enrichment → aggregation → scoring → ranking (mirrors L3's backend query).
--
-- Four stages:
--   scored   : flatten abnormal_flags → one row per flag occurrence.
--   minutes  : per patient-minute: readings, abnormal minutes, abnormal share.
--   flagged  : the same for the whole patient session.
--   ranked   : rank patients by abnormal share.

WITH scored AS (
    SELECT
        v.patient_id,
        v.event_time,
        f.flag
    FROM iceberg.healthcare.vitals AS v
    CROSS JOIN UNNEST(v.abnormal_flags) AS f(flag)
),
minutes AS (
    SELECT
        patient_id,
        date_trunc('minute', event_time) AS bucket_ts,
        count(*)                         AS flagged_readings
    FROM scored
    GROUP BY patient_id, date_trunc('minute', event_time)
),
flagged AS (
    SELECT
        m.patient_id,
        count(m.flagged_readings)                  AS abnormal_minutes,
        round(100.0 * count(m.flagged_readings)
                    / count(DISTINCT m.bucket_ts), 1) AS abnormal_share_pct,
        sum(m.flagged_readings)                    AS abnormal_readings
    FROM minutes m
    GROUP BY m.patient_id
),
ranked AS (
    SELECT
        f.patient_id,
        f.abnormal_minutes,
        f.abnormal_readings,
        f.abnormal_share_pct,
        DENSE_RANK() OVER (ORDER BY f.abnormal_share_pct DESC) AS rank_by_share,
        RANK()       OVER (ORDER BY f.abnormal_readings DESC)  AS rank_by_volume
    FROM flagged f
)
SELECT
    r.patient_id,
    p.first_name || ' ' || p.last_name AS patient_name,
    r.abnormal_minutes,
    r.abnormal_readings,
    r.abnormal_share_pct,
    r.rank_by_share,
    r.rank_by_volume
FROM ranked r
JOIN iceberg.healthcare.patients p ON r.patient_id = p.patient_id
ORDER BY r.rank_by_share;