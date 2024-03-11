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
    description="A test DAG to check all dependencies are installed.",
    schedule_interval="@once",
    start_date=days_ago(1),
    tags=["example", "airflow", "python"],
)


def check_package(package_name):
    try:
        pkg_resources.require(package_name)
        print(f"{package_name} is installed.")
    except pkg_resources.DistributionNotFound:
        print(f"{package_name} is NOT installed.")
        raise


def pip_freeze():
    # Using subprocess to execute pip freeze
    result = subprocess.run(["pip", "freeze"], capture_output=True, text=True)
    print(result.stdout)


packages = [
    "watchtower==2.0.1",
    "apache-airflow-providers-snowflake==5.0.1",
    "airflow-dbt==0.4.0",
    "dbt-snowflake==1.7.1",
    "jq",
    "psycopg2-binary==2.9.9",
    "boto3==1.28.17",
    "botocore==1.31.17",
]

# Tasks to check if specific packages are installed
for package in packages:
    check_package_task = PythonOperator(
        task_id=f"check_{package.replace('==', '_').replace('-', '_').replace('.', '_')}_installation",
        python_callable=check_package,
        op_args=[package],
        dag=dag,
    )

# Task to run pip freeze
pip_freeze_task = PythonOperator(
    task_id="pip_freeze",
    python_callable=pip_freeze,
    dag=dag,
)

for package in packages:
    check_package_task >> pip_freeze_task
