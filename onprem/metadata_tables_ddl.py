from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MetadataDDL").getOrCreate()

# DDL for metadata tables (on-premises version)
spark.sql('''
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
) USING DELTA
''')

spark.sql('''
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
) USING DELTA
''')

spark.sql('''
CREATE TABLE IF NOT EXISTS column_metadata (
  table_id STRING,
  column_name STRING,
  data_type STRING,
  target_type STRING,
  target_column_name STRING,
  nullable BOOLEAN,
  is_primary_key STRING
) USING DELTA
''')

spark.sql('''
CREATE TABLE IF NOT EXISTS ingestion_schedule (
  schedule_id STRING,
  connection_id STRING,
  table_id STRING,
  schedule_type STRING,
  cron_expression STRING,
  active_flag STRING,
  last_run TIMESTAMP,
  next_run TIMESTAMP,
  comments STRING
) USING DELTA
''')

print("All metadata tables created (on-premises version).")