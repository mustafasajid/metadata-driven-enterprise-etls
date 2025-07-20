# Features: BYOD Enterprise Solution Metadata-Driven Data Warehouse Utility

This document provides a comprehensive overview of all features, integration points, and validation steps for the metadata-driven data warehouse utility.

---

## 1. Metadata Table Creation
- `metadata_tables_ddl.sql` creates all required Delta tables:
  - `connection_metadata`: RDBMS connection info
  - `table_metadata`: Table-level mapping, naming, partitioning, and scheduling info
  - `column_metadata`: Column-level schema, types, nullability, and customizable `target_column_name`
  - `ingestion_schedule`: Scheduling info for orchestrated ingestion jobs
  - (Others for extensibility)
- **All required fields for mapping, naming, partitioning, and scheduling are present.**

---

## 2. Automated Metadata Population
- `metadata_auto_populate.py`:
  - User provides: `connection_id`, `schema_name`, `target_db`, and (optionally) `target_prefix`/`target_suffix`.
  - Discovers all tables and columns in the source schema.
  - Populates:
    - `table_metadata`:
      - `table_name` = source table name
      - `target_table_name` = prefix + source table name + suffix (or just source table name)
      - `target_db` = user-supplied Databricks database
    - `column_metadata`:
      - `column_name` = source column name
      - `target_column_name` = source column name (default, but can be customized)
      - `target_type` = mapped Spark/Delta type
      - `nullable`, `is_primary_key`, etc.
- **All mappings for both tables and columns are present and customizable.**

---

## 3. Orchestration
- `metadata_population_orchestration.py` and `ingestion_orchestration.py`:
  - Guide the user through the process, ensuring correct parameters and metadata population.
  - Allow for immediate or scheduled ingestion, writing to `ingestion_schedule`.

---

## 4. Delta Table Auto-Creation
- `delta_table_auto_create.py`:
  - Reads `table_metadata` and `column_metadata`.
  - For each table:
    - Uses `target_db` and `target_table_name` for the Databricks table.
    - Uses `target_column_name` for each column (falls back to `column_name` if not set).
    - Supports partitioning and PK documentation.
  - **No source connection needed if metadata is present.**
  - **Target table schema matches metadata, including custom column names.**

---

## 5. Ingestion
- `metadata_driven_ingestion.py`:
  - User specifies which tables to ingest (by `table_id`).
  - For each table:
    - Connects to the correct source using `connection_id`.
    - Reads from the correct source table (`table_name`).
    - **Renames DataFrame columns according to the `target_column_name` mapping from `column_metadata`.**
    - Writes to the correct Databricks table (`target_db.target_table_name`).
    - Supports both full and incremental loads, as specified in metadata.
- **Target table and column names are always correct and metadata-driven.**

---

## 6. Scheduling
- `ingestion_schedule` table and orchestration:
  - Users can schedule ingestion jobs for connections or tables.
  - Stores cron expressions and job metadata for integration with Databricks Jobs/Workflows or external orchestrators.

---

## 7. Validation & Error Handling
- Validation steps are documented for each stage.
- Error handling is present in table creation and ingestion.
- Users are guided to check that target tables and columns match metadata.

---

## 8. Extensibility
- All logic is modular and metadata-driven.
- Adding new RDBMS, new fields, or new orchestration patterns is straightforward.
- Easily integrate with Databricks Jobs/Workflows or external orchestrators for production scheduling.

---

## 9. End-to-End Integration
- **Every step (from source discovery to Delta table creation to ingestion) is metadata-driven.**
- **Both table and column mappings are respected at every stage.**
- **Custom naming, partitioning, and scheduling are all supported and integrated.**
- **No step is out of sync with the metadata.**

---

## 10. Validation Checklist

1. **After metadata auto-population:**
   - Check `table_metadata` for correct `table_name`, `target_table_name`, and `target_db` values.
   - Check `column_metadata` for all expected columns, types, and `target_column_name` values.
2. **After Delta table creation:**
   - Use `SHOW TABLES IN <target_db>` in Databricks SQL to verify tables exist.
   - Use `DESCRIBE TABLE <target_db>.<target_table_name>` to verify schema matches metadata (including column names).
3. **After ingestion:**
   - Query the Delta tables to ensure data is present and correct.
   - For incremental loads, verify only new/changed data is ingested.
   - Verify that column names in the target match `target_column_name` in metadata.
4. **For scheduling:**
   - Check `ingestion_schedule` for correct entries and cron expressions.
   - Ensure Databricks Jobs/Workflows are configured to use this schedule if needed.

---

For questions or enhancements, please contact the project maintainer.