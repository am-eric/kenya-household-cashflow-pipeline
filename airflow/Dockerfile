FROM apache/airflow:2.10.5-python3.11

# dbt and the loader live in their own venv so their dependencies never clash with Airflow's
RUN python -m venv /home/airflow/dbt_venv \
 && /home/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-core==1.12.6 dbt-postgres==1.11.0 python-dotenv==1.2.4
