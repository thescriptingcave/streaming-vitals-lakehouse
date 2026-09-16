-- 01 · Time bucketing.
--
-- Goal: vitals arrive on an irregular cadence (one reading per device per
-- ~1.5 s). Comparing "the current reading" across devices is apples-to-oranges,
-- so we first PROJECT irregular timestamps onto a fixed grid with
-- DATE_TRUNC(<unit>, ts). Trends are then computed over buckets, not raw rows.
--
-- Also shows the companion EXTRACT-style windows: HOUR() / DAY_OF_WEEK() /
-- DAYOFWEEK() let you pivot by clock, not by magnitude.

SELECT
    patient_id,
    date_trunc('minute', event_time)          AS bucket_ts,
    hour(event_time)                          AS hour_of_day,
    day_of_week(event_time)                     AS day_of_week,
    count(*)                                  AS readings,
    round(avg(heart_rate), 1)                 AS avg_hr,
    min(spo2)                                 AS min_spo2,
    round(avg(systolic_bp), 1)                AS avg_sbp
FROM iceberg.healthcare.vitals
GROUP BY 1, 2, 3, 4
ORDER BY patient_id, bucket_ts
LIMIT 20;