# Databricks notebook: Auto-create Delta Tables from Metadata
from pyspark.sql import SparkSession
from src.metadata import MetadataManager

# Widgets for connection_id and schema_name (target_db is now per-table)
dbutils.widgets.text("connection_id", "", "Connection ID")
dbutils.widgets.text("schema_name", "", "Schema Name")
connection_id = dbutils.widgets.get("connection_id")
schema_name = dbutils.widgets.get("schema_name")

spark = SparkSession.builder.getOrCreate()
metadata = MetadataManager(spark)
conn_meta = metadata.load_connection_metadata()
col_meta = metadata.spark.table('column_metadata')

conn = conn_meta.filter(conn_meta.connection_id == connection_id).collect()[0]

# def get_column_query(conn_type, schema_name, table_name):
#     if conn_type == "oracle":
#         return f"SELECT column_name, data_type, nullable FROM all_tab_columns WHERE owner = '{schema_name.upper()}' AND table_name = '{table_name.upper()}'"
#     elif conn_type == "sqlserver":
#         return f"SELECT COLUMN_NAME as column_name, DATA_TYPE as data_type, IS_NULLABLE as nullable FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'"
#     elif conn_type in ["postgresql", "redshift"]:
#         return f"SELECT column_name, data_type, is_nullable as nullable FROM information_schema.columns WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'"
#     elif conn_type in ["mysql", "mariadb"]:
#         return f"SELECT column_name, data_type, is_nullable as nullable FROM information_schema.columns WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'"
#     elif conn_type == "snowflake":
#         return f"SELECT column_name, data_type, is_nullable as nullable FROM information_schema.columns WHERE table_schema = '{schema_name.upper()}' AND table_name = '{table_name.upper()}'"
#     elif conn_type == "bigquery":
#         return f"SELECT column_name, data_type, is_nullable as nullable FROM `{schema_name}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{table_name}'"
#     elif conn_type == "db2":
#         return f"SELECT colname as column_name, typename as data_type, nulls as nullable FROM syscat.columns WHERE tabschema = '{schema_name.upper()}' AND tabname = '{table_name.upper()}'"
#     elif conn_type == "hana":
#         return f"SELECT COLUMN_NAME as column_name, DATA_TYPE_NAME as data_type, IS_NULLABLE as nullable FROM TABLE_COLUMNS WHERE SCHEMA_NAME = '{schema_name.upper()}' AND TABLE_NAME = '{table_name.upper()}'"
#     else:
#         raise Exception(f"Unsupported RDBMS type: {conn_type}")

# Get tables to create from table_metadata
table_meta = metadata.load_table_metadata()
tables = table_meta.filter((table_meta.connection_id == connection_id) & (table_meta.table_name.isNotNull())).collect()

for tbl in tables:
    table_id = tbl.table_id
    target_db = tbl.target_db
    # Use column_metadata for DDL
    columns = col_meta.filter(col_meta.table_id == table_id).collect()
    # Fallback code for source query is commented out for now
    # if not columns:
    #     print(f"No column metadata for {tbl.table_name}, querying source...")
    #     jdbc_url = None
    #     if conn.type == "oracle":
    #         jdbc_url = f"jdbc:oracle:thin:@{conn.host}:{conn.port}/{conn.database}"
    #     elif conn.type == "sqlserver":
    #         jdbc_url = f"jdbc:sqlserver://{conn.host}:{conn.port};databaseName={conn.database}"
    #     elif conn.type == "postgresql":
    #         jdbc_url = f"jdbc:postgresql://{conn.host}:{conn.port}/{conn.database}"
    #     elif conn.type == "mysql":
    #         jdbc_url = f"jdbc:mysql://{conn.host}:{conn.port}/{conn.database}"
    #     elif conn.type == "mariadb":
    #         jdbc_url = f"jdbc:mariadb://{conn.host}:{conn.port}/{conn.database}"
    #     elif conn.type == "snowflake":
    #         jdbc_url = f"jdbc:snowflake://{conn.host}/?db={conn.database}&schema={schema_name}"
    #     elif conn.type == "bigquery":
    #         jdbc_url = f"jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId={conn.database};"
    #     elif conn.type == "redshift":
    #         jdbc_url = f"jdbc:redshift://{conn.host}:{conn.port}/{conn.database}"
    #     elif conn.type == "db2":
    #         jdbc_url = f"jdbc:db2://{conn.host}:{conn.port}/{conn.database}"
    #     elif conn.type == "hana":
    #         jdbc_url = f"jdbc:sap://{conn.host}:{conn.port}"
    #     else:
    #         raise Exception(f"Unsupported RDBMS type: {conn.type}")
    #     col_query = get_column_query(conn.type, schema_name, tbl.table_name)
    #     options = {
    #         "url": jdbc_url,
    #         "user": conn.username,
    #         "password": conn.password,
    #         "query": col_query
    #     }
    #     cols_df = spark.read.format("jdbc").options(**options).load()
    #     columns = cols_df.collect()
    #     # Use source type as fallback
    #     col_defs = []
    #     for col in columns:
    #         spark_type = "string"  # fallback
    #         if hasattr(col, "target_type"):
    #             spark_type = col.target_type
    #         elif hasattr(col, "data_type"):
    #             spark_type = col.data_type
    #         nullable = "NULL" if (str(col.nullable).upper() in ["YES", "Y", "NULL", "1"]) else "NOT NULL"
    #         col_defs.append(f"`{col.column_name}` {spark_type} {nullable}")
    col_defs = []
    for col in columns:
        col_name = col.target_column_name if hasattr(col, "target_column_name") and col.target_column_name else col.column_name
        spark_type = col.target_type if hasattr(col, "target_type") else "string"
        nullable = "NULL" if (col.nullable if isinstance(col.nullable, bool) else str(col.nullable).upper() in ["YES", "Y", "NULL", "1"]) else "NOT NULL"
        col_defs.append(f"`{col_name}` {spark_type} {nullable}")
    col_defs_str = ",\n  ".join(col_defs)
    # Partitioning support
    partition_stmt = ""
    if tbl.partition_column:
        partition_stmt = f"\nPARTITIONED BY (`{tbl.partition_column}`)"
    # Primary key support (comment only, as Delta does not enforce PKs)
    pk_comment = f"-- Primary Key: {tbl.primary_key_columns}" if tbl.primary_key_columns else ""
    create_stmt = f"CREATE TABLE IF NOT EXISTS {target_db}.`{tbl.target_table_name}` (\n  {col_defs_str}\n) USING DELTA{partition_stmt}; {pk_comment}"
    try:
        print(f"Creating table: {target_db}.{tbl.target_table_name}")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {target_db}")
        spark.sql(create_stmt)
        print(f"Created table: {target_db}.{tbl.target_table_name}")
    except Exception as e:
        print(f"Error creating table {target_db}.{tbl.target_table_name}: {e}")