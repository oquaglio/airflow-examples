from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pkg_resources
from airflow.utils.dates import days_ago
import os
import subprocess  # Import subprocess module to run shell commands

DAG_ID = os.path.basename(__file__).replace(".py", "")

default_args = {
    "owner": "niwdu",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "trigger_rule": "all_done",
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="A DAG to print all installed Python packages",
    schedule_interval="@once",
    start_date=days_ago(1),
    tags=["example", "airflow", "python", "test"],
)


def print_python_packages():
    # Using subprocess to execute pip freeze
    result = subprocess.run(["pip", "freeze"], capture_output=True, text=True)
    print(result.stdout)


# Task to print all python packages
print_packages_task = PythonOperator(
    task_id="print_python_packages",
    python_callable=print_python_packages,
    dag=dag,
)

print_packages_task
