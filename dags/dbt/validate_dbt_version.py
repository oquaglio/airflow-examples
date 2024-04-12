from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os

DAG_ID = os.path.basename(__file__).replace(".py", "")

default_args = {
    "owner": "otto",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "trigger_rule": "all_done",
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="A DAG to check the DBT version.",
    schedule_interval="@once",
    start_date=days_ago(1),
    tags=["example", "test"],
) as dag:

    cli_command = BashOperator(task_id="bash_command", bash_command="$AIRFLOW_HOME/.local/bin/dbt --version")
