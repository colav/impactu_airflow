# Estrategia de DevOps para ImpactU Airflow

Este documento detalla la estrategia profesional de CI/CD, gestión de entornos y despliegue para el monorepositorio `impactu_airflow`.

## 1. Arquitectura de Imágenes (Decoupled Strategy)

Para optimizar los tiempos de construcción y garantizar la estabilidad, utilizamos una estrategia de imágenes desacoplada:

- **Imagen Base (Infraestructura - Repositorio `Chia`)**: Contiene Airflow, dependencias del sistema (build-essential, git, etc.) y librerías de Python pesadas.
  - `colav/impactu_airflow:base-latest` (para Desarrollo)
  - `colav/impactu_airflow:base-3.1.0` (para Producción)
- **Imagen de Lógica (Este Repositorio)**: Construye sobre la imagen base e inyecta únicamente el código de los DAGs, extractores, transformadores y cargadores.
  - `colav/impactu_airflow:latest` (Desarrollo)
  - `colav/impactu_airflow:v*` (Producción)

## 2. Estrategia de Ramas y Entornos

| Entorno | Rama/Trigger | URL | Puerto | Docker Project |
| :--- | :--- | :--- | :--- | :--- |
| **Desarrollo (Dev)** | Push a `main` | `dev.airflow.colav.co` | 8081 | `airflow-dev` |
| **Producción (Prod)** | Creación de Tag `v*` | `airflow.colav.co` | 8080 | `airflow-prod` |

### Flujo de Trabajo (GitFlow Simplificado)
1. Los desarrolladores trabajan en ramas de características (`feat/`, `fix/`).
2. Se abre un Pull Request (PR) hacia `main`.
3. El CI ejecuta pruebas de integridad y unitarias.
4. Al hacer merge a `main`, el CD despliega automáticamente a **Dev**.
5. Cuando el código es estable, se crea un tag (ej. `v1.0.0`) para desplegar a **Prod**.

## 3. Pipeline de CI/CD (GitHub Actions)

El archivo `.github/workflows/deploy.yml` gestiona el ciclo de vida:

### Fase de Test
- **Integridad de DAGs**: Verifica que todos los archivos en `dags/` sean parseables por Airflow.
- **Pruebas Unitarias**: Ejecuta `pytest` sobre los extractores y lógica de transformación usando `mongomock`.
- **Validación Remota (PRs)**: Cuando se abre un PR, el CI notifica al API de Airflow en Desarrollo para ejecutar el DAG `pr_validator`. Este DAG descarga los cambios del PR y los valida en el entorno real de Dev sin necesidad de desplegar la imagen completa.

### Fase de Build & Push
- Construye la imagen de Docker usando `build-args` para seleccionar la imagen base correcta.
- Publica en Docker Hub.
- **Nota**: El despliegue final en el servidor se gestiona de forma independiente desde el repositorio de infraestructura (`Chia`). Este repositorio solo se encarga de la entrega continua (Continuous Delivery) de las imágenes.

## 4. Gestión de Secretos y Configuración

### GitHub Secrets
- `DOCKER_HUB_USERNAME` / `DOCKER_HUB_TOKEN`: Para publicar imágenes en el registro.
- `AIRFLOW_API_USER` / `AIRFLOW_API_PASSWORD`: Credenciales para que el CI pueda disparar validaciones en el servidor de Desarrollo.
- `GH_TOKEN_PR_VALIDATOR`: Token de GitHub (Personal Access Token) con permisos de lectura para que el DAG validador pueda descargar los archivos del PR.

### Airflow (Runtime)
- **Variables**: Configuraciones no sensibles (ej. `scimagojr_cache_dir`).
- **Connections**: Credenciales de bases de datos (MongoDB, Postgres) y APIs. **Nunca** hardcodear credenciales en el código.

## 5. Monitoreo y Mantenimiento

- **Logs**: Centralizados en `/storage/airflow/data/dev/logs` y `/storage/airflow/data/prod/logs`.
- **Backups**: Los volúmenes de MongoDB y Postgres deben ser respaldados periódicamente (ver carpeta `backups/`).
- **Alertas**: Se recomienda configurar un `Sentry` o un Notificador de Slack en Airflow para fallos críticos en los DAGs.
