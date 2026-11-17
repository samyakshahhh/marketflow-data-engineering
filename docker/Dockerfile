# MarketFlow Airflow image.
#
# Extends the official Airflow image with the Snowflake provider and the
# libraries the DAGs need, so nothing is installed at container start-up and no
# local virtualenv is required to run the pipeline.
#
# Versions are resolved through the official Airflow constraints file for this
# exact Airflow + Python combination, which is the supported way to add
# providers to the image.
ARG AIRFLOW_VERSION=3.4.0
FROM apache/airflow:${AIRFLOW_VERSION}
ARG AIRFLOW_VERSION

USER airflow

COPY requirements.txt /tmp/marketflow-requirements.txt

RUN PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')" && \
    pip install --no-cache-dir \
        -r /tmp/marketflow-requirements.txt \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" && \
    pip check

# Project code is bind-mounted at /opt/airflow/project by docker-compose.
ENV PYTHONPATH=/opt/airflow/project \
    MARKETFLOW_PROJECT_ROOT=/opt/airflow/project
