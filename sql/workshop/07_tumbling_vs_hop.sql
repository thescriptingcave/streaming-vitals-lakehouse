-- 07 · TUMBLE vs HOP — comparing the two windowing dialects in the lake.
--
-- Flink runs BOTH of these against the same Kinesis stream (job.sql):
--   vitals_1m     : TUMBLE(event_time, INTERVAL '1' MINUTE)      — fixed, non-
--                   overlapping slices: every event belongs to exactly ONE
--                   window.  Exactly → rows per patient ≈ minutes covered.
--   vitals_hop_1m : HOP(event_time, INTERVAL '30' SECOND,        — sliding:
--                   INTERVAL '1' MINUTE)  size 1m, slide 30s. Two overlapping
--                   buckets live at any moment, one starting every 30s.
--                   Events near a bucket edge appear in BOTH buckets.
--
-- Overlap factor = window count(hop) / window count(tumble) ≈ size/slide = 2.
-- Sampling ratio = readings summed by hop / readings summed by tumble ≈ 2 too:
-- the SAME stream is double-counted because events near edges land twice.
-- This is the trade-off: hop gives finer response latency, tumble gives an
-- honest one-row-per-bucket summary.

WITH tumble AS (
    SELECT
        count(*)                                AS windows,
        count(DISTINCT patient_id)              AS patients,
        sum(reading_count)                      AS sampled_readings,
        min(window_start)                       AS lo,
        max(window_end)                         AS hi
    FROM iceberg.healthcare.vitals_1m
),
hop AS (
    SELECT
        count(*)                                AS windows,
        count(DISTINCT patient_id)              AS patients,
        sum(reading_count)                      AS sampled_readings,
        min(window_start)                       AS lo,
        max(window_end)                         AS hi
    FROM iceberg.healthcare.vitals_hop_1m
)
SELECT
    'tumbling 1m' AS windowing, windows, patients, sampled_readings, lo, hi, 1.0 AS overlap_factor
FROM tumble
UNION ALL
SELECT
    'hop 30s/1m' AS windowing, windows, patients, sampled_readings, lo, hi,
    round(1.0 * windows / (SELECT windows FROM tumble), 2) AS overlap_factor
FROM hop
ORDER BY windowing;

-- Prove a single near-edge event is reported TWICE by the hop windows.
SELECT
    'tumbling' AS win,
    window_start,
    window_end,
    count(DISTINCT patient_id) AS patients,
    round(avg(avg_heart_rate), 1) AS avg_hr
FROM iceberg.healthcare.vitals_1m AS v1
WHERE window_start = TIMESTAMP '2026-09-16 05:01:00 UTC'
GROUP BY window_start, window_end
UNION ALL
SELECT
    'hop' AS win,
    window_start,
    window_end,
    count(DISTINCT patient_id),
    round(avg(avg_heart_rate), 1)
FROM iceberg.healthcare.vitals_hop_1m AS vh
WHERE window_start BETWEEN TIMESTAMP '2026-09-16 05:00:30 UTC'
                       AND TIMESTAMP '2026-09-16 05:01:30 UTC'
GROUP BY window_start, window_end
ORDER BY win, window_start;