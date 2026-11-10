

<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

[![Validate and Test DAGs](https://github.com/omazapa/impactu_airflow/actions/workflows/deploy.yml/badge.svg)](https://github.com/omazapa/impactu_airflow/actions/workflows/deploy.yml)
# ImpactU Airflow ETL

Central repository for Apache Airflow DAGs and ETL (Capture, Transformation, and Loading) processes for the ImpactU project. This package includes the full logic for data orchestration and data processing.

## 🚀 Description
This project orchestrates data collection from various scientific and academic sources, its processing using the [Kahi](https://github.com/colav/Kahi) tool, and its subsequent loading into query systems such as MongoDB and Elasticsearch.

## � Installation
You can install the package directly from PyPI:

```bash
pip install impactu_airflow
```

Or for development:

```bash
git clone https://github.com/colav/impactu_airflow.git
cd impactu_airflow
pip install -e .
```

## �📂 Project Structure
The repository is organized by data lifecycle stages and Airflow components:

*   `dags/`: Apache Airflow DAG definitions.
*   `extract/`: Shared extraction base classes and source-specific modules that still run outside capture DAGs.
*   `transform/`: Transformation and normalization processes (Kahi).
*   `load/`: Loading scripts to final destinations (MongoDB, Elasticsearch).
*   `deploys/`: Deployment logic for external services (APIs, databases) via DAGs.
*   `backups/`: Database backup automation via DAGs.
*   `tests/`: Integration, unit, and data quality tests.

## 📋 Requirements and Architecture
For details on design principles (Checkpoints, Idempotency, Parallelism), see the [System Requirements](REQUIREMENTS.md) document.

## 🛠 DAG Naming Standard
To maintain consistency in the Airflow interface, we follow this convention:

| Type | Format | Example |
| :--- | :--- | :--- |
| **Capture** | `{source}_capture` | `doaj_capture` |
| **Transformation** | `transform_{entity}` | `transform_sources` |
| **Loading** | `load_{db}_{env}` | `load_mongodb_production` |
| **Deployment** | `deploy_{service}_{env}` | `deploy_mongodb_production` |
| **Backup** | `backup_{db}_{name}` | `backup_mongodb_kahi` |
| **Tests** | `tests_{service}` | `tests_kahi` |

## ⚙️ Development and Deployment

### ROR capture

`ror_capture` resolves the current ROR release **on every run**, using Zenodo's
concept record `6347574`. No version-specific record ID or release ZIP is pinned
in the DAG. The sequence is `resolve_ror` → `download_ror` → `load_ror`.

If Zenodo is unavailable, the resolver consults the concept DOI in DataCite and
verifies the version-specific DOI and its parent. It then selects **that exact
version** from the official `ror-community/ror-data` repository, pins the GitHub
commit, and verifies the Git blob checksum. A download fallback from Zenodo to
GitHub also retains the original Zenodo checksum. It never substitutes another
release or disables HTTPS certificate verification.

Every run downloads the resolved ZIP again, verifies its size and checksum, and
replaces the cached file only after validation. A SHA-256 binds the downloaded
archive to the load task. Files reside in
`$AIRFLOW_HOME/cache/ror_import/<resolved_record_id>/`, in the Docker deployment's
shared cache mount so the download and load tasks can use different workers.

The loader validates the JSON and unique ROR IDs, inserts into a temporary
collection, archives the previous database under a unique timestamped name, and
renames the completed collection to `ror.ror_stage`. It reloads even when the
release has not changed. Runs are serialized (`max_active_runs=1`). Other
collections remain available. The resolved version, Zenodo record, download URL,
SHA-256, loaded count and archive name are saved in
`ror.ror_import_metadata`, document `_id: "current"`.

Reference: [Retrieve ROR data from Zenodo](https://ror.readme.io/docs/zenodo).

Run the focused tests in an Airflow environment with:

```bash
python -m unittest discover -s tests/etl -p test_ror.py -v
```

This repository focuses exclusively on DAG logic and ETL processes. The base infrastructure is provided by the **Chia** repository.

For details on the CI/CD strategy, image building, and environment management, see the document:
👉 **[README_DEVOPS.md](README_DEVOPS.md)**

### Local Workflow
1. Clone the repository.
2. Install dependencies: `pip install -r requirements.txt`.
3. Develop DAGs in the `dags/` folder.
4. Validate integrity: `pytest tests/etl/test_dag_integrity.py`.

---
**Colav - ImpactU**
