-- Validation queries (make validate). Success = row counts consistent with
-- the batch load + streaming windows. See docs/TESTING.md M1/M4/M5 commit gates.
SELECT 'patients'  AS table_name, COUNT(*) AS rows FROM iceberg.healthcare.patients
UNION ALL SELECT 'vitals', COUNT(*) FROM iceberg.healthcare.vitals
UNION ALL SELECT 'vitals_1m', COUNT(*) FROM iceberg.healthcare.vitals_1m;

-- Reads must round-trip: ordering stays intact for hedged window reads.
SELECT AVG(max_heart_rate) AS avg_max_hr
FROM iceberg.healthcare.vitals_1m;