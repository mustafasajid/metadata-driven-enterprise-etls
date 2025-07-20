-- Databricks SQL DDL for Metadata Tables

-- 1. connection_metadata
CREATE TABLE IF NOT EXISTS connection_metadata (
  connection_id STRING,
  type STRING,
  host STRING,
  port INT,
  database STRING,
  schema STRING,
  username STRING,
  password STRING,
  options STRING
) USING DELTA;

-- 2. table_metadata
CREATE TABLE IF NOT EXISTS table_metadata (
  table_id STRING,
  connection_id STRING,
  table_name STRING,
  target_table_name STRING,
  table_type STRING,
  primary_key_columns STRING,
  watermark_column STRING,
  partition_column STRING,
  target_path STRING,
  load_frequency STRING,
  active_flag STRING,
  comments STRING,
  optimize_zorder_by STRING,
  repartition_columns STRING,
  num_output_files INT,
  write_mode STRING,
  cache_intermediate BOOLEAN,
  target_db STRING
) USING DELTA;

-- 3. column_metadata
CREATE TABLE IF NOT EXISTS column_metadata (
  table_id STRING,
  column_name STRING,
  data_type STRING,
  target_type STRING,
  target_column_name STRING, -- NEW: customizable target column name
  nullable BOOLEAN,
  is_primary_key STRING
) USING DELTA;

-- 4. datamart_metadata
CREATE TABLE IF NOT EXISTS datamart_metadata (
  datamart_id STRING,
  datamart_name STRING,
  description STRING
) USING DELTA;

-- 5. table_datamart_mapping
CREATE TABLE IF NOT EXISTS table_datamart_mapping (
  table_id STRING,
  datamart_id STRING
) USING DELTA;

-- 6. byod_table_catalog
CREATE TABLE IF NOT EXISTS byod_table_catalog (
  catalog_id STRING,
  connection_id STRING,
  schema_name STRING,
  table_name STRING,
  onboarded_flag STRING
) USING DELTA;

-- 7. ingestion_schedule
CREATE TABLE IF NOT EXISTS ingestion_schedule (
  schedule_id STRING,
  connection_id STRING,
  table_id STRING,
  schedule_type STRING, -- 'connection' or 'table'
  cron_expression STRING,
  active_flag STRING,
  last_run TIMESTAMP,
  next_run TIMESTAMP,
  comments STRING
) USING DELTA;