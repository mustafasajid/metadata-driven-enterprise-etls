# Databricks notebook: Metadata Population Orchestration
from pyspark.sql import SparkSession
from src.metadata import MetadataManager

spark = SparkSession.builder.getOrCreate()
metadata = MetadataManager(spark)

# 1. Check if metadata tables exist
def table_exists(table_name):
    return table_name in [row.tableName for row in spark.catalog.listTables()]

required_tables = ["connection_metadata", "table_metadata"]
missing = [t for t in required_tables if not table_exists(t)]
if missing:
    print(f"Missing required tables: {missing}. Please run metadata_tables_ddl.sql first.")
    dbutils.notebook.exit("Missing tables")

# 2. List available connections
conn_df = metadata.load_connection_metadata()
connections = conn_df.collect()
print("Available connections:")
for idx, row in enumerate(connections):
    print(f"{idx+1}. {row.connection_id} ({row.type} @ {row.host})")

# 3. Prompt user for connection selection and schema name
dbutils.widgets.text("connection_idx", "1", "Select connection (number)")
dbutils.widgets.text("schema_name", "", "Schema Name")
connection_idx = int(dbutils.widgets.get("connection_idx")) - 1
schema_name = dbutils.widgets.get("schema_name")

if not (0 <= connection_idx < len(connections)):
    print("Invalid connection selection.")
    dbutils.notebook.exit("Invalid connection selection")

selected_conn = connections[connection_idx]
print(f"Selected connection: {selected_conn.connection_id} ({selected_conn.type})")
print(f"Schema: {schema_name}")

# 4. Trigger the auto-populate notebook
result = dbutils.notebook.run("metadata_auto_populate.py", 600, {
    "connection_id": selected_conn.connection_id,
    "schema_name": schema_name
})
print(result)

# 5. Show summary and next steps
print("\nMetadata population complete.")
print("You may now proceed to run metadata_driven_ingestion.py to ingest data from the populated tables.")