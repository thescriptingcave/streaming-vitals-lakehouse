-- 02 · LAG / LEAD — "how fast is this patient changing?".
--
-- LAG(col, k) hands you the k-th PREVIOUS row within a partition/order; LEAD
-- the k-th NEXT. The cornerstone of delta/slope detection on time series:
-- a sudden drop in SpO2 or jump in HR is an *event*, not a level.
--
-- Here: per device, previous and next heart rate, the raw delta and the
-- percent change, keeping only swings >= 5 bpm to make the signal visible.
--
-- (BigQuery/Spark would write the final filter with QUALIFY ...; Trino
-- filters the windowed result in a derived table instead.)

SELECT
    patient_id,
    device_id,
    at_ts,
    hr_now,
    hr_prev,
    hr_next,
    delta,
    pct_change
FROM (
    SELECT
        patient_id,
        device_id,
        date_trunc('second', event_time)          AS at_ts,
        round(heart_rate, 0)                      AS hr_now,

        -- k-th previous / next value in the device's ordered stream
        round(lag(heart_rate)  OVER w, 0)         AS hr_prev,
        round(lead(heart_rate) OVER w, 0)         AS hr_next,

        -- first derivative (bpm per reading) + as a ratio
        round(heart_rate - lag(heart_rate) OVER w, 1)               AS delta,
        round(100.0 * (heart_rate - lag(heart_rate) OVER w)
                    / greatest(lag(heart_rate) OVER w, 1e-9), 2)    AS pct_change
    FROM iceberg.healthcare.vitals
    WINDOW w AS (PARTITION BY device_id ORDER BY event_time)
)
WHERE abs(delta) >= 5
ORDER BY patient_id, device_id, at_ts
LIMIT 30;