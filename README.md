# BYOD Enterprise Solution: Metadata-Driven Data Warehouse Utility

This repository provides a robust, metadata-driven utility for onboarding, managing, and ingesting data from multiple RDBMS sources into a Databricks Lakehouse or on-premises Spark environment. It is designed for extensibility, automation, and platform-agnostic operation.

---

## Repository Structure

```
/ (repo root)
│
├── src/                # Shared core Python modules (metadata, ingestion, etc.)
├── databricks/         # Databricks-specific notebooks, configs, and documentation
├── onprem/             # On-premises (Linux/Spark) scripts, configs, and documentation
├── docs/               # Shared documentation (features, architecture, etc.)
├── requirements.txt    # Top-level requirements (if needed)
├── .gitignore
└── README.md           # Main project overview (this file)
```

---

## Documentation
- **Feature Overview:** See [FEATURES.md](./FEATURES.md) for a comprehensive list of features and validation steps.
- **Databricks Configuration & Usage:** See [`databricks/README.md`](./databricks/README.md)
- **On-Premises Configuration & Usage:** See [`onprem/README.md`](./onprem/README.md)
- **Shared Architecture & Metadata Model:** See [`docs/`](./docs/)

---

## Quickstart
1. Review the [FEATURES.md](./FEATURES.md) to understand capabilities.
2. Choose your environment (Databricks or On-Premises) and follow the respective README for setup and configuration.
3. Use the shared `src/` modules for any custom development or extension.

---

For questions or enhancements, please contact the project maintainer.
