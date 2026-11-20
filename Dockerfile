# Preserve the production runtime, including the Oxomoc invalid-OAI fix and
# the AWS tooling layered into this image.
ARG IMPACTU_AIRFLOW_BASE=colav/impactu_airflow:base-3.1.0-aws-oxofix
FROM ${IMPACTU_AIRFLOW_BASE}

# Add only the runtime dependencies missing from the reconciled base image.
USER airflow
RUN python -m pip install --no-cache-dir --ignore-installed --no-deps \
    "apache-airflow-providers-mongo==5.3.0" \
    "pymongo==4.15.5"

# Copy and install repository code without replacing the preserved runtime.
COPY --chown=airflow:root . /opt/airflow/
RUN python -m pip install --no-cache-dir --no-deps --force-reinstall . && \
    test -d /tmp/oxomoc-fixed && \
    python -m pip install --no-cache-dir --no-deps --force-reinstall /tmp/oxomoc-fixed && \
    python -m pip check

WORKDIR /opt/airflow
