# Databricks notebook: Metadata-Driven Ingestion (Simplified)
from pyspark.sql import SparkSession
from src.metadata import MetadataManager

# Databricks widget for table_ids (comma-separated)
dbutils.widgets.text("table_ids", "", "Table IDs (comma-separated)")
table_ids_param = dbutils.widgets.get("table_ids")
table_ids = [tid.strip() for tid in table_ids_param.split(",") if tid.strip()]

spark = SparkSession.builder.getOrCreate()
metadata = MetadataManager(spark)

# Load metadata tables
table_meta = metadata.load_table_metadata()
conn_meta = metadata.load_connection_metadata()
col_meta = metadata.spark.table('column_metadata')

# Filter for selected tables
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
    # Get column mapping for this table
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