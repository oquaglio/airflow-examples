from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from os import environ as env


def print_env_vars():
    for key, value in env.items():
        print(f"{key}: {value}")


with DAG(
    "print_env_vars",
    description="DAG to print environment variables",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=[
        "variables",
    ],
) as dag:

    task_print_env_vars = PythonOperator(task_id="print_env_vars", python_callable=print_env_vars)

    task_print_env_vars
