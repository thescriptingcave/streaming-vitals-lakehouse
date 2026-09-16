-- Vitals Flink SQL pipeline (source of truth; copied into the JAR resources).
-- Mirrors DATA_FLOW.md §2: Kinesis -> TUMBLE 1m -> Iceberg (Nessie) sinks.
-- Managed Flink (Floci) injects ApplicationProperties; the Kinesis connector
-- endpoint override comes from FLINK_AWS_ENDPOINT_URL via VitalsFlinkJob.
--
-- Catalog wiring (verified on Iceberg 1.8 + Flink 1.19): FlinkCatalogFactory
-- only special-cases hive/hadoop/rest catalog types, but CREATE CATALOG also
-- accepts 'catalog-impl'; this is the documented Flink-SQL Nessie pattern.
-- NessieCatalog resolves S3 through S3FileIO and needs no Hadoop FileSystem.

-- Raw stream source. Lives in the DEFAULT catalog: an Iceberg catalog table
-- cannot declare a watermark (Iceberg rejects watermark specs).
-- event_time/ingestion_time are TIMESTAMP_LTZ: the stream carries ISO-8601 UTC
-- instants ("...Z"), which Flink's JSON ISO-8601 parser only accepts for LTZ.
CREATE TABLE IF NOT EXISTS vitals_source (
  patient_id     STRING,
  device_id      STRING,
  event_time     TIMESTAMP_LTZ(3),
  ingestion_time TIMESTAMP_LTZ(3),
  heart_rate     DOUBLE,
  systolic_bp    DOUBLE,
  diastolic_bp   DOUBLE,
  spo2           DOUBLE,
  temperature    DOUBLE,
  resp_rate      DOUBLE,
  trace_id       STRING,
  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
  'connector' = 'kinesis',
  'stream' = 'vitals',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'aws.region' = 'us-east-1',
  'aws.endpoint' = 'http://floci:4566',
  'aws.trust.all.certificates' = 'true',
  -- Legacy kinesis consumer (FlinkKinesisConsumer under the SQL source) reads
  -- 'scan.'-prefixed keys as 'flink.' consumer props; the initial position is
  -- 'flink.stream.initpos' (TRIM_HORIZON|LATEST|AT_TIMESTAMP). 'scan.startup.mode'
  -- is NOT honored here and silently falls back to LATEST.
  'scan.stream.initpos' = 'TRIM_HORIZON',
  'aws.credentials.provider' = 'AUTO'
);

CREATE CATALOG iceberg WITH (
  'type' = 'iceberg',
  'catalog-impl' = 'org.apache.iceberg.nessie.NessieCatalog',
  'uri' = 'http://nessie:19120/api/v2',
  'warehouse' = 's3://healthcare-lake/warehouse',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  'client.region' = 'us-east-1',
  's3.endpoint' = 'http://floci:4566',
  's3.path-style-access' = 'true',
  's3.access-key-id' = 'test',
  's3.secret-access-key' = 'test',
  's3.region' = 'us-east-1'
);

USE CATALOG iceberg;
CREATE DATABASE IF NOT EXISTS healthcare;
USE healthcare;

-- Tumbling 1-minute aggregates: dashboards + latest-vitals read model.
-- M3 refinement: alert rules (HR/SpO2 thresholds) become a second sink.
-- write.flush.max-rows keeps the streaming writer spilling files at modest
-- volume (default 8000 rows would otherwise produce empty commits here).
CREATE TABLE IF NOT EXISTS vitals_1m (
  window_start    TIMESTAMP_LTZ(3),
  window_end      TIMESTAMP_LTZ(3),
  patient_id      STRING,
  avg_heart_rate  DOUBLE,
  max_heart_rate  DOUBLE,
  min_spo2        DOUBLE,
  max_systolic_bp DOUBLE,
  reading_count   BIGINT
) WITH (
  'write.format.default' = 'parquet',
  'write.flush.max-rows' = '100',
  'write.metadata.delete-after-commit.enabled' = 'true',
  'write.metadata.previous-versions-max' = '20'
);

INSERT INTO vitals_1m
SELECT
  TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
  TUMBLE_END(event_time, INTERVAL '1' MINUTE)   AS window_end,
  patient_id,
  AVG(heart_rate)     AS avg_heart_rate,
  MAX(heart_rate)     AS max_heart_rate,
  MIN(spo2)           AS min_spo2,
  MAX(systolic_bp)    AS max_systolic_bp,
  COUNT(*)            AS reading_count
FROM `default_catalog`.`default_database`.vitals_source
GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE), patient_id;