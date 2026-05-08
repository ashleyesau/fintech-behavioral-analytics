from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

REPO = "/opt/airflow/fintech"
PYTHON = "/home/airflow/.local/bin/python"
DBT = "/home/airflow/.local/bin/dbt"
DBT_FLAGS = f"--project-dir {REPO}/dbt/fintech_analytics --profiles-dir /home/airflow/.dbt"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="fintech_pipeline",
    description="Extract from Plaid, load to BigQuery, run dbt models end to end",
    schedule_interval="@daily",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    default_args=default_args,
    tags=["fintech", "plaid", "dbt"],
) as dag:

    extract_plaid_data = BashOperator(
        task_id="extract_plaid_data",
        bash_command=f"cd {REPO} && /home/airflow/.local/bin/python3 ingestion/extract_transactions.py",
    )

    load_to_bigquery = BashOperator(
        task_id="load_to_bigquery",
        bash_command=f"cd {REPO} && /home/airflow/.local/bin/python3 ingestion/load_to_bigquery.py",
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"{DBT} run --select staging.* {DBT_FLAGS}",
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"{DBT} test --select staging.* {DBT_FLAGS}",
    )

    dbt_run_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=f"{DBT} run --select intermediate.* {DBT_FLAGS}",
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"{DBT} run --select marts.* {DBT_FLAGS}",
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=f"{DBT} test --select marts.* {DBT_FLAGS}",
    )

    extract_plaid_data >> load_to_bigquery >> dbt_run_staging >> dbt_test_staging >> dbt_run_intermediate >> dbt_run_marts >> dbt_test_marts
