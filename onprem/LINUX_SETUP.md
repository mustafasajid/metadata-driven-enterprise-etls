# Linux Setup Guide: On-Premises Metadata-Driven Data Warehouse Utility

This guide will help you set up and run the on-premises (Linux/Spark) version of the metadata-driven data warehouse utility.

---

## 1. Install Prerequisites

### a. Python 3.8+
Most modern Linux distributions come with Python 3. Check with:
```bash
python3 --version
```
If not installed, use your package manager (e.g., `sudo apt install python3 python3-pip` for Ubuntu/Debian).

### b. Java (OpenJDK 8 or 11)
Apache Spark requires Java. Check with:
```bash
java -version
```
If not installed:
```bash
sudo apt install openjdk-11-jdk
```

### c. Apache Spark (with Delta Lake support)
- Download Spark from https://spark.apache.org/downloads.html (choose a version compatible with Delta Lake, e.g., 3.2+).
- Extract and set environment variables:
```bash
export SPARK_HOME=~/spark-3.x.x-bin-hadoop3.x
export PATH=$SPARK_HOME/bin:$PATH
```
- (Optional) Add these lines to your `~/.bashrc` or `~/.zshrc` for persistence.

### d. Delta Lake JARs
- Download the Delta Lake JARs from https://docs.delta.io/latest/quick-start.html#download
- Place them in `$SPARK_HOME/jars/` or specify with `--packages io.delta:delta-core_2.12:2.2.0` when running Spark jobs.

---

## 2. Install Python Dependencies
Navigate to the `onprem/` directory and run:
```bash
pip3 install -r requirements.txt
```

---

## 3. Configure Metadata
- Use the provided scripts to create metadata tables:
```bash
python3 metadata_tables_ddl.py
```
- Populate `connection_metadata` with your RDBMS connection info (can use a script, SQL client, or edit directly if using Parquet/CSV).

---

## 4. Run the Solution
- **Auto-populate metadata:**
```bash
python3 metadata_auto_populate.py --connection_id <id> --schema_name <schema> --target_db <db> [--target_prefix <prefix>] [--target_suffix <suffix>]
```
- **Create Delta tables:**
```bash
python3 delta_table_auto_create.py --connection_id <id> --schema_name <schema>
```
- **Ingest data:**
```bash
python3 metadata_driven_ingestion.py --table_ids <comma-separated-table-ids>
```
- **Orchestrate ingestion:**
```bash
python3 ingestion_orchestration.py --connection_id <id> [--table_id <table_id>] [--ingest_mode immediate|schedule] [--cron_expression <cron>]
```

---

## 5. Orchestration (Optional)
- Use Airflow, cron, or shell scripts to schedule and automate the above commands.
- Example cron job (run every night at 2am):
```
0 2 * * * cd /path/to/onprem && python3 ingestion_orchestration.py --connection_id my_conn
```

---

## 6. Troubleshooting
- Ensure all environment variables (`SPARK_HOME`, `PATH`) are set correctly.
- Check that all dependencies are installed (`pip3 list`, `java -version`, `spark-submit --version`).
- Review logs/output for errors and consult the main documentation for help.

---

For more details, see the main [README](../README.md), [FEATURES.md](../FEATURES.md), and [CONFIGURATION.md](./CONFIGURATION.md).