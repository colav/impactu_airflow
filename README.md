# ImpactU Airflow ETL

Central repository for Apache Airflow DAGs for the Extraction, Transformation, and Loading (ETL) processes of the ImpactU project.

## 🚀 Description
This project orchestrates data collection from various scientific and academic sources, its processing using the [Kahi](https://github.com/colav/Kahi) tool, and its subsequent loading into query systems such as MongoDB and Elasticsearch.

## 📂 Project Structure
The repository is organized by data lifecycle stages:

*   `extract/`: Extraction logic for sources like OpenAlex, ORCID, ROR, etc.
*   `transform/`: Transformation and normalization processes (Kahi).
*   `load/`: Loading scripts to final destinations.
*   `deploys/`: Lógica de despliegue de servicios externos (APIs, bases de datos) mediante DAGs.
*   `backups/`: Automatización de respaldos de bases de datos mediante DAGs.
*   `tests/`: Integration and data quality tests.

## 📋 Requirements and Architecture
For details on design principles (Checkpoints, Idempotency, Parallelism), see the [System Requirements](REQUIREMENTS.md) document.

## 🛠 DAG Naming Standard
To maintain consistency in the Airflow interface, we follow this convention:

| Type | Format | Example |
| :--- | :--- | :--- |
| **Extraction** | `extract_{source}` | `extract_openalex` |
| **Transformation** | `transform_{entity}` | `transform_sources` |
| **Loading** | `load_{db}_{env}` | `load_mongodb_production` |
| **Deployment** | `deploy_{service}_{env}` | `deploy_mongodb_production` |
| **Backup** | `backup_{db}_{name}` | `backup_mongodb_kahi` |
| **Tests** | `tests_{service}` | `tests_kahi` |

## ⚙️ Desarrollo y Despliegue

Este repositorio se enfoca exclusivamente en la lógica de los DAGs y procesos ETL. La infraestructura base es provista por el repositorio **Chia**.

Para detalles sobre la estrategia de CI/CD, construcción de imágenes y gestión de entornos, consulta el documento:
👉 **[README_DEVOPS.md](README_DEVOPS.md)**

### Flujo de Trabajo Local
1. Clonar el repositorio.
2. Instalar dependencias: `pip install -r requirements.txt`.
3. Desarrollar DAGs en la carpeta `dags/`.
4. Validar integridad: `pytest tests/etl/test_dag_integrity.py`.

---
**Colav - ImpactU**
