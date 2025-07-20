import argparse
from pyspark.sql import SparkSession, Row
from src.metadata import MetadataManager
import uuid, datetime

parser = argparse.ArgumentParser(description="Ingestion orchestration (on-premises)")
parser.add_argument('--connection_id', required=True)
parser.add_argument('--table_id', default=None)
parser.add_argument('--ingest_mode', choices=['immediate', 'schedule'], default='immediate')
parser.add_argument('--cron_expression', default='0 2 * * *')
args = parser.parse_args()

spark = SparkSession.builder.appName("IngestionOrchestration").getOrCreate()
metadata = MetadataManager(spark)
conn_meta = metadata.load_connection_metadata()
table_df = metadata.load_table_metadata()

selected_conn = conn_meta.filter(conn_meta.connection_id == args.connection_id).collect()[0]

if args.ingest_mode == "immediate":
    if args.table_id:
        table_ids = [args.table_id]
    else:
        table_ids = [row.table_id for row in table_df.filter(table_df.connection_id == selected_conn.connection_id).collect()]
    table_ids_str = ",".join(table_ids)
    # Here you would call the ingestion script, e.g. via subprocess or function call
    print(f"Would ingest tables: {table_ids_str}")
else:
    schedule_id = str(uuid.uuid4())
    schedule_type = "table" if args.table_id else "connection"
    now = datetime.datetime.now()
    schedule_row = Row(
        schedule_id=schedule_id,
        connection_id=selected_conn.connection_id,
        table_id=args.table_id if args.table_id else None,
        schedule_type=schedule_type,
        cron_expression=args.cron_expression,
        active_flag="Y",
        last_run=None,
        next_run=None,
        comments=f"Scheduled via orchestration script at {now}"
    )
    schedule_df = spark.createDataFrame([schedule_row])
    schedule_df.write.format("delta").mode("append").saveAsTable("ingestion_schedule")
    print(f"Scheduled {schedule_type} ingestion for {selected_conn.connection_id} {f'table {args.table_id}' if args.table_id else ''} with cron: {args.cron_expression}")

print("Ingestion orchestration complete (on-premises version).")