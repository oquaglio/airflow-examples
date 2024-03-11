from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os

DAG_ID = os.path.basename(__file__).replace(".py", "")

default_args = {
    "owner": "niwdu",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "trigger_rule": "all_done",
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="A DAG to print the Python version",
    schedule_interval="@once",
    start_date=days_ago(1),
    tags=["example", "airflow", "python"],
) as dag:

    print_python_version = BashOperator(
        task_id="print_python_version",
        bash_command="python --version || python3 --version",
    )
