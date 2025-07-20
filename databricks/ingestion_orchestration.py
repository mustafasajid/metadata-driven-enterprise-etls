# Databricks notebook: Ingestion Orchestration
from pyspark.sql import SparkSession
from src.metadata import MetadataManager

spark = SparkSession.builder.getOrCreate()
metadata = MetadataManager(spark)

# 1. List available connections and tables
conn_df = metadata.load_connection_metadata()
table_df = metadata.load_table_metadata()
connections = conn_df.collect()
print("Available connections:")
for idx, row in enumerate(connections):
    print(f"{idx+1}. {row.connection_id} ({row.type} @ {row.host})")

dbutils.widgets.text("connection_idx", "1", "Select connection (number)")
dbutils.widgets.text("table_id", "", "Table ID (optional, for single table)")
dbutils.widgets.dropdown("ingest_mode", "immediate", ["immediate", "schedule"], "Ingestion Mode")
dbutils.widgets.text("cron_expression", "0 2 * * *", "Cron Expression (if scheduled)")

connection_idx = int(dbutils.widgets.get("connection_idx")) - 1
table_id = dbutils.widgets.get("table_id")
ingest_mode = dbutils.widgets.get("ingest_mode")
cron_expression = dbutils.widgets.get("cron_expression")

if not (0 <= connection_idx < len(connections)):
    print("Invalid connection selection.")
    dbutils.notebook.exit("Invalid connection selection")

selected_conn = connections[connection_idx]

# 2. Immediate or scheduled ingestion
if ingest_mode == "immediate":
    # If table_id is provided, ingest just that table; else, all tables for the connection
    table_ids = [table_id] if table_id else [row.table_id for row in table_df.filter(table_df.connection_id == selected_conn.connection_id).collect()]
    table_ids_str = ",".join(table_ids)
    result = dbutils.notebook.run("metadata_driven_ingestion.py", 600, {
        "table_ids": table_ids_str
    })
    print(result)
    print("Ingestion complete.")
else:
    # Write to ingestion_schedule table
    from pyspark.sql import Row
    import uuid, datetime
    schedule_id = str(uuid.uuid4())
    schedule_type = "table" if table_id else "connection"
    now = datetime.datetime.now()
    schedule_row = Row(
        schedule_id=schedule_id,
        connection_id=selected_conn.connection_id,
        table_id=table_id if table_id else None,
        schedule_type=schedule_type,
        cron_expression=cron_expression,
        active_flag="Y",
        last_run=None,
        next_run=None,
        comments=f"Scheduled via orchestration notebook at {now}"
    )
    schedule_df = spark.createDataFrame([schedule_row])
    schedule_df.write.format("delta").mode("append").saveAsTable("ingestion_schedule")
    print(f"Scheduled {schedule_type} ingestion for {selected_conn.connection_id} {f'table {table_id}' if table_id else ''} with cron: {cron_expression}")
    print("You can now configure Databricks Jobs/Workflows to read from ingestion_schedule and trigger ingestion as per the schedule.")