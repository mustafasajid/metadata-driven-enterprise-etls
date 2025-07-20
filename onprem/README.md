# On-Premises (Linux/Spark) Environment: BYOD Enterprise Solution

This directory contains all on-premises (Linux/Spark) scripts, configuration, and documentation for the metadata-driven data warehouse utility.

---

## Contents
- Python scripts for metadata table creation, population, Delta table creation, ingestion, and orchestration
- Example Airflow DAGs, cron jobs, or shell scripts for orchestration
- Environment-specific configuration and documentation

---

## Setup & Configuration
1. **Install Apache Spark** (with Delta Lake support) on your Linux environment.
2. **Install required Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
3. **Configure connections** in `connection_metadata` (can be done via script or SQL client).
4. **Set up orchestration** (e.g., Airflow, cron) as needed for your environment.
5. **Follow the happy flow** as described in the main [README](../README.md) and [FEATURES.md](../FEATURES.md).

---

## Usage
- Use the provided scripts in the recommended order:
  1. `metadata_tables_ddl.py` (or equivalent SQL)
  2. `metadata_auto_populate.py`
  3. (Optional) orchestration scripts
  4. `delta_table_auto_create.py`
  5. `metadata_driven_ingestion.py`
- All configuration is metadata-driven. Update the metadata tables to control ingestion, mapping, and scheduling.

---

## Notes
- All shared logic is in the `src/` directory and can be reused for Databricks or other environments.
- For advanced configuration, see the [FEATURES.md](../FEATURES.md) and `docs/`.
- Adapt orchestration to your environment (e.g., Airflow DAGs, cron jobs, shell scripts).

---

For on-premises-specific questions, contact the project maintainer.