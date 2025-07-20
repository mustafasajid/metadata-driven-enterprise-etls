import argparse
from pyspark.sql import SparkSession
from src.metadata import MetadataManager
import subprocess

parser = argparse.ArgumentParser(description="Metadata population orchestration (on-premises)")
parser.add_argument('--connection_id', required=True)
parser.add_argument('--schema_name', required=True)
parser.add_argument('--target_db', required=True)
parser.add_argument('--target_prefix', default="")
parser.add_argument('--target_suffix', default="")
args = parser.parse_args()

spark = SparkSession.builder.appName("MetadataPopulationOrchestration").getOrCreate()
metadata = MetadataManager(spark)

# Check if metadata tables exist
required_tables = ["connection_metadata", "table_metadata"]
existing_tables = [row.tableName for row in spark.catalog.listTables()]
missing = [t for t in required_tables if t not in existing_tables]
if missing:
    print(f"Missing required tables: {missing}. Please run metadata_tables_ddl.py first.")
    exit(1)

# List available connections
conn_df = metadata.load_connection_metadata()
connections = conn_df.collect()
print("Available connections:")
for idx, row in enumerate(connections):
    print(f"{idx+1}. {row.connection_id} ({row.type} @ {row.host})")

# Trigger the auto-populate script (simulate subprocess call)
print(f"Running metadata_auto_populate.py for connection_id={args.connection_id}, schema_name={args.schema_name}, target_db={args.target_db}")
# In a real system, you might use subprocess to call the script:
# subprocess.run(["python", "metadata_auto_populate.py", "--connection_id", args.connection_id, "--schema_name", args.schema_name, "--target_db", args.target_db, "--target_prefix", args.target_prefix, "--target_suffix", args.target_suffix])

print("Metadata population orchestration complete (on-premises version).")