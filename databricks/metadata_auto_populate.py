# Databricks notebook: Auto-populate Metadata from RDBMS
from pyspark.sql import SparkSession, Row
from src.metadata import MetadataManager

# Widgets for connection_id, schema_name, target_db, prefix, and suffix
dbutils.widgets.text("connection_id", "", "Connection ID")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("target_db", "", "Target Databricks Database")
dbutils.widgets.text("target_prefix", "", "Target Table Prefix (optional)")
dbutils.widgets.text("target_suffix", "", "Target Table Suffix (optional)")
connection_id = dbutils.widgets.get("connection_id")
schema_name = dbutils.widgets.get("schema_name")
target_db = dbutils.widgets.get("target_db")
target_prefix = dbutils.widgets.get("target_prefix")
target_suffix = dbutils.widgets.get("target_suffix")

spark = SparkSession.builder.getOrCreate()
metadata = MetadataManager(spark)
conn_meta = metadata.load_connection_metadata()

conn = conn_meta.filter(conn_meta.connection_id == connection_id).collect()[0]

jdbc_url = None
table_query = None
if conn.type == "oracle":
    jdbc_url = f"jdbc:oracle:thin:@{conn.host}:{conn.port}/{conn.database}"
    table_query = f"SELECT table_name FROM all_tables WHERE owner = '{schema_name.upper()}'"
elif conn.type == "sqlserver":
    jdbc_url = f"jdbc:sqlserver://{conn.host}:{conn.port};databaseName={conn.database}"
    table_query = f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema_name}'"
elif conn.type == "postgresql":
    jdbc_url = f"jdbc:postgresql://{conn.host}:{conn.port}/{conn.database}"
    table_query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}'"
elif conn.type == "mysql":
    jdbc_url = f"jdbc:mysql://{conn.host}:{conn.port}/{conn.database}"
    table_query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}'"
elif conn.type == "mariadb":
    jdbc_url = f"jdbc:mariadb://{conn.host}:{conn.port}/{conn.database}"
    table_query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}'"
elif conn.type == "snowflake":
    jdbc_url = f"jdbc:snowflake://{conn.host}/?db={conn.database}&schema={schema_name}"
    table_query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name.upper()}'"
elif conn.type == "bigquery":
    jdbc_url = f"jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId={conn.database};"
    table_query = f"SELECT table_name FROM `{schema_name}.INFORMATION_SCHEMA.TABLES`"
elif conn.type == "redshift":
    jdbc_url = f"jdbc:redshift://{conn.host}:{conn.port}/{conn.database}"
    table_query = f"SELECT tablename AS table_name FROM pg_table_def WHERE schemaname = '{schema_name}'"
elif conn.type == "db2":
    jdbc_url = f"jdbc:db2://{conn.host}:{conn.port}/{conn.database}"
    table_query = f"SELECT tabname AS table_name FROM syscat.tables WHERE tabschema = '{schema_name.upper()}'"
elif conn.type == "hana":
    jdbc_url = f"jdbc:sap://{conn.host}:{conn.port}"
    table_query = f"SELECT TABLE_NAME FROM TABLES WHERE SCHEMA_NAME = '{schema_name.upper()}'"
else:
    raise Exception(f"Unsupported RDBMS type: {conn.type}")

options = {
    "url": jdbc_url,
    "user": conn.username,
    "password": conn.password,
    "query": table_query
}

# Type mapping: source -> Spark/Delta
type_map = {
    # Oracle
    "NUMBER": "long",
    "VARCHAR2": "string",
    "DATE": "timestamp",
    "CHAR": "string",
    "NVARCHAR2": "string",
    "FLOAT": "double",
    # SQL Server
    "int": "int",
    "bigint": "long",
    "nvarchar": "string",
    "varchar": "string",
    "datetime": "timestamp",
    "bit": "boolean",
    # PostgreSQL
    "integer": "int",
    "serial": "int",
    "bigserial": "long",
    "text": "string",
    "timestamp": "timestamp",
    # MySQL/MariaDB
    "tinyint": "byte",
    "smallint": "short",
    "mediumint": "int",
    "decimal": "double",
    # Snowflake/Redshift/BigQuery/Db2/HANA (add more as needed)
    "BOOLEAN": "boolean",
    "STRING": "string",
    "FLOAT64": "double",
    "TIMESTAMP": "timestamp",
    "DATE": "date"
}

def get_column_query(conn_type, schema_name, table_name):
    if conn_type == "oracle":
        return f"SELECT column_name, data_type, nullable FROM all_tab_columns WHERE owner = '{schema_name.upper()}' AND table_name = '{table_name.upper()}'"
    elif conn_type == "sqlserver":
        return f"SELECT COLUMN_NAME as column_name, DATA_TYPE as data_type, IS_NULLABLE as nullable FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'"
    elif conn_type in ["postgresql", "redshift"]:
        return f"SELECT column_name, data_type, is_nullable as nullable FROM information_schema.columns WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'"
    elif conn_type in ["mysql", "mariadb"]:
        return f"SELECT column_name, data_type, is_nullable as nullable FROM information_schema.columns WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'"
    elif conn_type == "snowflake":
        return f"SELECT column_name, data_type, is_nullable as nullable FROM information_schema.columns WHERE table_schema = '{schema_name.upper()}' AND table_name = '{table_name.upper()}'"
    elif conn_type == "bigquery":
        return f"SELECT column_name, data_type, is_nullable as nullable FROM `{schema_name}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{table_name}'"
    elif conn_type == "db2":
        return f"SELECT colname as column_name, typename as data_type, nulls as nullable FROM syscat.columns WHERE tabschema = '{schema_name.upper()}' AND tabname = '{table_name.upper()}'"
    elif conn_type == "hana":
        return f"SELECT COLUMN_NAME as column_name, DATA_TYPE_NAME as data_type, IS_NULLABLE as nullable FROM TABLE_COLUMNS WHERE SCHEMA_NAME = '{schema_name.upper()}' AND TABLE_NAME = '{table_name.upper()}'"
    else:
        raise Exception(f"Unsupported RDBMS type: {conn_type}")

# Fetch tables from source schema
tables_df = spark.read.format("jdbc").options(**options).load()
table_names = [row.table_name for row in tables_df.collect()]

# Prepare rows for table_metadata and column_metadata (with sensible defaults)
table_metadata_rows = []
column_metadata_rows = []
for tbl in table_names:
    table_id = f"{connection_id}_{tbl}"
    tgt_tbl_name = f"{target_prefix}{tbl}{target_suffix}" if target_prefix or target_suffix else tbl
    table_metadata_rows.append(Row(
        table_id=table_id,
        connection_id=connection_id,
        table_name=tbl,
        target_table_name=tgt_tbl_name,
        table_type="full",
        primary_key_columns=None,
        watermark_column=None,
        partition_column=None,
        target_path=f"/mnt/datalake/{tgt_tbl_name}",
        load_frequency="daily",
        active_flag="Y",
        comments="Auto-populated",
        optimize_zorder_by=None,
        repartition_columns=None,
        num_output_files=None,
        write_mode="overwrite",
        cache_intermediate=False,
        target_db=target_db
    ))
    # Fetch columns for this table
    col_query = get_column_query(conn.type, schema_name, tbl)
    col_options = {
        "url": jdbc_url,
        "user": conn.username,
        "password": conn.password,
        "query": col_query
    }
    cols_df = spark.read.format("jdbc").options(**col_options).load()
    for col in cols_df.collect():
        column_metadata_rows.append(Row(
            table_id=table_id,
            column_name=col.column_name,
            data_type=col.data_type,
            target_type=type_map.get(col.data_type.upper(), "string"),
            target_column_name=col.column_name,  # Default to source column name
            nullable=(str(col.nullable).upper() in ["YES", "Y", "NULL", "1"]),
            is_primary_key="N"  # Could be improved with PK detection logic
        ))

# Convert to DataFrame and write to table_metadata and column_metadata
auto_df = spark.createDataFrame(table_metadata_rows)
auto_df.write.format("delta").mode("append").saveAsTable("table_metadata")

col_auto_df = spark.createDataFrame(column_metadata_rows)
col_auto_df.write.format("delta").mode("append").saveAsTable("column_metadata")

print(f"Populated table_metadata for {len(table_names)} tables and column_metadata for {len(column_metadata_rows)} columns from schema {schema_name}.")