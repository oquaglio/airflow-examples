from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from os import environ as env


def print_database_name():
    database_name = env.get("DATABASE_NAME")
    print(f"The database name is: {database_name}")
    database_name = env["DATABASE_NAME"]
    print(f"The database name is: {database_name}")


with DAG("using_env_vars", start_date=datetime(2022, 1, 1), schedule_interval="@once") as dag:
    task = PythonOperator(task_id="print_database_name", python_callable=print_database_name)
