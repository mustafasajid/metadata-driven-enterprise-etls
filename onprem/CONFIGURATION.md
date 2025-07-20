# Configuration Guide: On-Premises (Linux/Spark) Environment

This document provides guidance for configuring the metadata-driven data warehouse utility in an on-premises (Linux/Spark) environment.

---

## 1. Connection Configuration
- All RDBMS and target connections are defined in the `connection_metadata` Delta table (or Parquet/local RDBMS if preferred).
- Required fields:
  - `connection_id`, `type`, `host`, `port`, `database`, `schema`, `username`, `password`, `options`
- **How to configure:**
  - Use a Python script, SQL client, or direct file edit (if using Parquet/CSV for metadata).
  - Store credentials securely (e.g., environment variables, .env files, or OS-level secrets).

---

## 2. Table & Column Metadata
- `table_metadata` and `column_metadata` drive all ingestion, mapping, and table creation logic.
- Key fields:
  - `table_metadata`: `table_id`, `connection_id`, `table_name`, `target_table_name`, `target_db`, `partition_column`, etc.
  - `column_metadata`: `table_id`, `column_name`, `target_column_name`, `target_type`, `nullable`, etc.
- **How to configure:**
  - Use the provided on-premises scripts for initial auto-population.
  - Edit metadata tables directly in Spark SQL, via scripts, or in Parquet/CSV to customize mappings, naming, partitioning, etc.

---

## 3. Environment-Specific Settings
- Use argparse, config files, or environment variables for script parameters.
- Store metadata tables as Delta, Parquet, or in a local RDBMS.
- Use Airflow, cron, or shell scripts for orchestration and scheduling.
- Use OS-level secrets or .env files for sensitive information.

---

## 4. Advanced Configuration
- **Partitioning:** Set `partition_column` in `table_metadata` for partitioned Delta tables.
- **Custom Naming:** Set `target_table_name` and `target_column_name` for custom table/column names.
- **Incremental Loads:** Set `table_type` and `watermark_column` for incremental ingestion.
- **Scheduling:** Use `ingestion_schedule` to define cron expressions and job metadata.

---

## 5. Validation
- Always validate that metadata tables are populated and correct before running ingestion or table creation scripts.
- Use the validation checklist in [FEATURES.md](../FEATURES.md) for step-by-step verification.

---

For more details, see the main [README](../README.md) and [FEATURES.md](../FEATURES.md).