import argparse
from pyspark.sql import SparkSession
from src.metadata import MetadataManager

parser = argparse.ArgumentParser(description="Metadata-driven ingestion (on-premises)")
parser.add_argument('--table_ids', required=True, help="Comma-separated list of table_ids to ingest")
args = parser.parse_args()

table_ids = [tid.strip() for tid in args.table_ids.split(",") if tid.strip()]

spark = SparkSession.builder.appName("MetadataDrivenIngestion").getOrCreate()
metadata = MetadataManager(spark)

table_meta = metadata.load_table_metadata()
conn_meta = metadata.load_connection_metadata()
col_meta = metadata.spark.table('column_metadata')

selected_tables = table_meta.filter(table_meta.table_id.isin(table_ids)).collect()

for row in selected_tables:
    conn = conn_meta.filter(conn_meta.connection_id == row.connection_id).collect()[0]
    jdbc_url = f"jdbc:{conn.type}://{conn.host}:{conn.port}/{conn.database}"
    options = {
        "url": jdbc_url,
        "dbtable": row.table_name,
        "user": conn.username,
        "password": conn.password
    }
    target_table = f"{row.target_db}.{row.target_table_name}" if row.target_db else row.target_table_name
    columns = col_meta.filter(col_meta.table_id == row.table_id).collect()
    col_map = {col.column_name: col.target_column_name if hasattr(col, "target_column_name") and col.target_column_name else col.column_name for col in columns}
    def apply_col_mapping(df, col_map):
        for src, tgt in col_map.items():
            if src != tgt:
                df = df.withColumnRenamed(src, tgt)
        return df
    if row.table_type == "full":
        df = spark.read.format("jdbc").options(**options).load()
        df = apply_col_mapping(df, col_map)
        df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        print(f"Full load: {row.table_name} to {target_table}")
    elif row.table_type == "incremental":
        try:
            target_df = spark.table(target_table)
            max_watermark = target_df.agg({row.watermark_column: "max"}).collect()[0][0]
        except Exception:
            max_watermark = None
        predicate = f"{row.watermark_column} > '{max_watermark}'" if max_watermark else None
        if predicate:
            options["predicate"] = predicate
        df = spark.read.format("jdbc").options(**options).load()
        df = apply_col_mapping(df, col_map)
        df.write.format("delta").mode("append").saveAsTable(target_table)
        print(f"Incremental load: {row.table_name} to {target_table} (watermark: {max_watermark})")
    else:
        print(f"Unknown table_type for {row.table_name}")

print("Metadata-driven ingestion complete (on-premises version).")