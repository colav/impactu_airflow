ARG BASE_IMAGE=colav/impactu_airflow:base-3.1.0
FROM ${BASE_IMAGE}

USER root

# Install any additional system dependencies if needed
# RUN apt-get update && apt-get install -y ... && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements and install them
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy the project structure into the airflow home
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root extract/ /opt/airflow/extract/
COPY --chown=airflow:root transform/ /opt/airflow/transform/
COPY --chown=airflow:root load/ /opt/airflow/load/
COPY --chown=airflow:root impactu/ /opt/airflow/impactu/
COPY --chown=airflow:root config/ /opt/airflow/config/

# Set PYTHONPATH to include the root so imports work correctly
ENV PYTHONPATH="/opt/airflow/impactu:/opt/airflow"
