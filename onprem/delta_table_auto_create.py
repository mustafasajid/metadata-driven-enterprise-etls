import argparse
from pyspark.sql import SparkSession
from src.metadata import MetadataManager

parser = argparse.ArgumentParser(description="Auto-create Delta tables from metadata (on-premises)")
parser.add_argument('--connection_id', required=True)
parser.add_argument('--schema_name', required=True)
args = parser.parse_args()

spark = SparkSession.builder.appName("DeltaTableAutoCreate").getOrCreate()
metadata = MetadataManager(spark)
conn_meta = metadata.load_connection_metadata()
col_meta = metadata.spark.table('column_metadata')

table_meta = metadata.load_table_metadata()
tables = table_meta.filter((table_meta.connection_id == args.connection_id) & (table_meta.table_name.isNotNull())).collect()

for tbl in tables:
    table_id = tbl.table_id
    target_db = tbl.target_db
    columns = col_meta.filter(col_meta.table_id == table_id).collect()
    col_defs = []
    for col in columns:
        col_name = col.target_column_name if hasattr(col, "target_column_name") and col.target_column_name else col.column_name
        spark_type = col.target_type if hasattr(col, "target_type") else "string"
        nullable = "NULL" if (col.nullable if isinstance(col.nullable, bool) else str(col.nullable).upper() in ["YES", "Y", "NULL", "1"]) else "NOT NULL"
        col_defs.append(f"`{col_name}` {spark_type} {nullable}")
    col_defs_str = ",\n  ".join(col_defs)
    partition_stmt = ""
    if tbl.partition_column:
        partition_stmt = f"\nPARTITIONED BY (`{tbl.partition_column}`)"
    pk_comment = f"-- Primary Key: {tbl.primary_key_columns}" if tbl.primary_key_columns else ""
    create_stmt = f"CREATE TABLE IF NOT EXISTS {target_db}.`{tbl.target_table_name}` (\n  {col_defs_str}\n) USING DELTA{partition_stmt}; {pk_comment}"
    try:
        print(f"Creating table: {target_db}.{tbl.target_table_name}")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {target_db}")
        spark.sql(create_stmt)
        print(f"Created table: {target_db}.{tbl.target_table_name}")
    except Exception as e:
        print(f"Error creating table {target_db}.{tbl.target_table_name}: {e}")

print("Delta table auto-creation complete (on-premises version).")