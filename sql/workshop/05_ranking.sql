-- 05 · Ranking (RANK / DENSE_RANK / NTILE / ROW_NUMBER) — order within a group.
--
-- Four flavours, all computing an ordinal OVER (PARTITION, ORDER):
--   ROW_NUMBER : strict position — never ties.
--   RANK       : gaps after ties (1,2,2,4).
--   DENSE_RANK : no gaps after ties (1,2,2,3).
--   NTILE(k)   : splits the partition into k buckets (deciles in heuristics).
--
-- Query A — the highest-HR readings within each patient, ties kept as ties.
-- Query B — which HR decile does each patient's worst reading sit in? (NTILE)
-- Query C — "latest reading per 5-minute slot" via ROW_NUMBER.

-- A. top 3 HR values per patient (RANK → genuine ties survive)
SELECT patient_id, heart_rate AS hr, event_time, rk
FROM (
    SELECT
        patient_id, heart_rate, event_time,
        RANK() OVER (PARTITION BY patient_id ORDER BY heart_rate DESC) AS rk
    FROM iceberg.healthcare.vitals
)
WHERE rk <= 3
ORDER BY patient_id, rk;

-- B. decile of each patient's highest reading (NTILE(10) per patient)
WITH per_patient AS (
    SELECT
        patient_id, heart_rate, event_time,
        NTILE(10) OVER (PARTITION BY patient_id ORDER BY heart_rate) AS hr_decile
    FROM iceberg.healthcare.vitals
)
SELECT patient_id, max(heart_rate) AS peak_hr, max(hr_decile) AS peak_decile
FROM per_patient
GROUP BY patient_id
ORDER BY patient_id;

-- C. the most recent reading per patient per 5-minute bucket (ROW_NUMBER)
SELECT patient_id, bucket_ts, hourly_HR AS last_hr, event_time
FROM (
    SELECT
        patient_id,
        date_trunc('minute', event_time) AS bucket_ts,
        heart_rate AS hourly_HR,
        event_time,
        ROW_NUMBER() OVER (
            PARTITION BY patient_id, date_trunc('minute', event_time)
            ORDER BY event_time DESC
        ) AS rn
    FROM iceberg.healthcare.vitals
)
WHERE rn = 1
ORDER BY patient_id, bucket_ts
LIMIT 15;