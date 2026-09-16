-- Healthcare Realtime Vitals Lakehouse — demo queries (Trino over Iceberg).
-- Run against:  trino --server http://127.0.0.1:8082  (or Superset SQL Lab)
-- Catalog/schema follow .env.example: iceberg.healthcare

-- 1. Lake inventory: row counts grown since the batch load.
SELECT table_name, record_count, data_version_id
FROM iceberg.healthcare."$table_metadata"
ORDER BY table_name;

-- 2. Latest vitals per patient (window/CTE, mirrors L3's backend query).
WITH ranked AS (
    SELECT
        patient_id, event_time, heart_rate, spo2, systolic_bp,
        ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY event_time DESC) AS rn
    FROM iceberg.healthcare.vitals
)
SELECT patient_id, event_time, heart_rate, spo2, systolic_bp
FROM ranked
WHERE rn = 1;

-- 3. Tumbling 1-minute heart-rate stats per patient (Flink also computes these
-- into vitals_1m.min; showing the table's precomputed window result here).
SELECT
    patient_id,
    window_start,
    avg_heart_rate,
    max_heart_rate,
    alert_count
FROM iceberg.healthcare.vitals_1m
WHERE window_start >= CURRENT_TIMESTAMP - INTERVAL '60' MINUTE
ORDER BY window_start DESC;

-- 4. Abnormal flag distribution over the last 24h.
SELECT
    flag,
    COUNT(*) AS occurrences
FROM iceberg.healthcare.vitals
CROSS JOIN UNNEST(abnormal_flags) AS t(flag)
WHERE event_time >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
GROUP BY flag
ORDER BY occurrences DESC;

-- 5. Alert history joined to the read-model table the API serves.
SELECT
    a.patient_id,
    a.rule,
    a.severity,
    a.value,
    a.threshold,
    a.event_ts
FROM iceberg.healthcare.alerts AS a
WHERE a.sent = TRUE
  AND a.event_ts >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
ORDER BY a.event_ts DESC
LIMIT 50;