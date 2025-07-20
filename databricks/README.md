# Notebooks

## Workflow Overview

- **metadata_tables_ddl.sql**
   - Create the required Delta metadata tables (including ingestion_schedule).
- **metadata_auto_populate.py**
   - Auto-populate `table_metadata` from a source RDBMS (Oracle, SQL Server, PostgreSQL, MySQL).
- **metadata_population_orchestration.py**
   - Orchestrate and guide the metadata population process.
- **metadata_tables_sample_data.sql** (optional/manual)
   - Insert sample data into metadata tables for testing or demonstration.
- **ingestion_orchestration.py**
   - Orchestrate ingestion, allowing immediate or scheduled ingestion for a connection or table.
- **metadata_driven_ingestion.py**
   - Ingest data from source tables into the data lake, driven by metadata.