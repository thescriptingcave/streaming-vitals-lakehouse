-- 00 · Boostrap the patients dimension (P0001–P0004) into the lake.
-- The live producer streams exactly these ids, so patient-level JOINs in the
-- workshop (06) resolve against a real dimension table.
-- Idempotent: DROP + CREATE keeps the workshop reproducible, dimension-state
-- lives in `vitals`, not here.

USE iceberg.healthcare;

DROP TABLE IF EXISTS patients;

CREATE TABLE patients (
    patient_id  varchar,
    first_name  varchar,
    last_name   varchar,
    gender      varchar,
    birth_date  date,
    race        varchar,
    ethnicity   varchar,
    city        varchar,
    state       varchar,
    county      varchar
) WITH (format = 'PARQUET');

INSERT INTO patients
VALUES
    ('P0001', 'Maria',  'Santos',    'F', DATE '1982-04-12', 'White',                        'Not Hispanic', 'Boston',      'MA', 'Suffolk'),
    ('P0002', 'James',  'Whitfield', 'M', DATE '1975-11-03', 'Black or African American',    'Not Hispanic', 'Springfield', 'MA', 'Hampden'),
    ('P0003', 'Aisha',  'Patel',     'F', DATE '1993-07-22', 'Asian',                        'Hispanic',     'Worcester',   'MA', 'Worcester'),
    ('P0004', 'Daniel', 'Okafor',    'M', DATE '1968-01-30', 'Black or African American',    'Not Hispanic', 'Lowell',      'MA', 'Middlesex');

SELECT patient_id, first_name, gender, birth_date, state
FROM iceberg.healthcare.patients
ORDER BY patient_id;