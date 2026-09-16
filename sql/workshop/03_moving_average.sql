-- 03 · Moving averages — smoothing the series.
--
-- A raw 1.5s cadence is noisy; a moving average replaces each point with the
-- mean of a sliding FRAME of neighbours. Frame = (PARTITION, ORDER, ROWS
-- BETWEEN `n PRECEDING AND m FOLLOWING | CURRENT ROW`) → the frame is the
-- "window of interest" over which the aggregate is computed.
--
-- Demonstrates TWO frame shapes on the same deltas you found in 02:
--   w_lead  : trailing 3-reading average (2 PRECEDING .. CURRENT ROW) — the
--             classic alert-check average.
--   w_center: centred 5-reading average  (2 PRECEDING .. 2 FOLLOWING) — lags
--             less than a trailing average, at the cost of future lookahead.
--
-- Trino's WINDOW clause names frames once so you can reuse them for several
-- aggregates instead of repeating the OVER(...) boilerplate.

SELECT
    patient_id,
    device_id,
    date_trunc('second', event_time)          AS at_ts,
    round(heart_rate, 0)                      AS hr,

    round(avg(heart_rate) OVER w_trailing, 1) AS hr_ma3_trailing,
    round(avg(heart_rate) OVER w_centred, 1)  AS hr_ma5_centred,
    round(min(spo2)      OVER w_trailing, 1)  AS spo2_trailing_min
FROM iceberg.healthcare.vitals
WINDOW
    w_trailing AS (PARTITION BY patient_id ORDER BY event_time
                   ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
    w_centred  AS (PARTITION BY patient_id ORDER BY event_time
                   ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)
ORDER BY patient_id, device_id, event_time
LIMIT 25;