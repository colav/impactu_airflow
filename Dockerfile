# Use the base image provided by the infrastructure
FROM colav/impactu_airflow:base-latest

# Install system dependencies
# - build-essential / g++ : compile C/C++ extensions (pycld2, hunspell)
# - libhunspell-dev : hunspell headers + shared lib (needed by fastspell dep of kahi_impactu_utils)
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    g++ \
    git \
    libhunspell-dev \
    pybind11-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Build wheels as root.
# The Airflow base image wraps `pip` to block root calls, but `python3 -m pip`
# bypasses that wrapper.
#
# 1. hunspell==0.5.5  — C extension that needs libhunspell-dev headers.
# 2. pybind11         — Python package required by fasttext's setup.py at import time.
# 3. fasttext==0.9.2  — fasttext's setup.py does `import pybind11`; we build it with
#                       --no-build-isolation so it finds the already-installed pybind11
#                       instead of trying to re-install it via /usr/python/bin/python3.12
#                       (which has no pip).
RUN python3 -m pip install --no-cache-dir "pybind11" && \
    python3 -m pip wheel --no-cache-dir \
        "hunspell==0.5.5" \
        "fasttext==0.9.2" \
        --no-build-isolation \
        -w /tmp/wheels/ && \
    chmod -R a+r /tmp/wheels/

# Switch back to airflow user for all pip installs
USER airflow

# Install pre-built wheels so downstream deps (fastspell, kahi_impactu_utils)
# find them already present and skip source builds.
# Exclude setuptools/pybind11 wheels (saved by --no-build-isolation) to avoid
# permission conflicts with the pre-installed system setuptools.
RUN pip install --no-cache-dir \
    /tmp/wheels/hunspell-*.whl \
    /tmp/wheels/fasttext-*.whl \
    /tmp/wheels/numpy-*.whl

# Install all remaining Python dependencies
COPY --chown=airflow:root requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the project structure
COPY --chown=airflow:root . /opt/airflow/

# Install the project in the image to make all modules (extract, transform, etc.)
# available globally in the Python environment.
RUN pip install --no-cache-dir .

# Set the working directory
WORKDIR /opt/airflow
