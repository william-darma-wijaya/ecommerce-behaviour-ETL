FROM apache/airflow:3.3.2-python3.11

COPY requirements.txt requirements-dbt.txt /tmp/

# Airflow environment: installed against Airflow's constraints so Airflow's own deps stay intact
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt"

# dbt in its own virtualenv (dbt and Airflow pin conflicting deps)
USER root
RUN python -m venv /opt/dbt-venv && chown -R airflow:0 /opt/dbt-venv
USER airflow
RUN PIP_USER=false /opt/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt
