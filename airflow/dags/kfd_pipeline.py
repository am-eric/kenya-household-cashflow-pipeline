"""Kenya Financial Diaries pipeline: raw csv -> raw.kfd_raw -> dbt (staging, clean, marts) -> dbt tests.

Weekly, and can be triggered on demand from the UI. Each task is one command, so a failed step
can be re-run on its own from the UI (clear the task) without redoing the earlier ones.
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = os.environ.get('KFD_PROJECT_DIR', '/opt/airflow/project')
VENV = '/home/airflow/dbt_venv/bin'

with DAG(
    dag_id='kfd_pipeline',
    description='Load the raw KFD csv into Postgres, build dbt models, run dbt tests',
    start_date=datetime(2026, 1, 1),
    schedule='@weekly',
    catchup=False,
    max_active_runs=1,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=2)},
    tags=['kfd'],
) as dag:
    load_raw = BashOperator(
        task_id='load_raw',
        bash_command=f'cd {PROJECT} && {VENV}/python load_raw.py',
    )
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {PROJECT}/kfd_dbt && {VENV}/dbt run --no-use-colors',
    )
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {PROJECT}/kfd_dbt && {VENV}/dbt test --no-use-colors',
        retries=0,   # a failing data test is a finding, not a flaky step
    )

    load_raw >> dbt_run >> dbt_test
