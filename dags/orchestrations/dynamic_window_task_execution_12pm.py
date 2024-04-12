from airflow import DAG
from datetime import datetime, timedelta
from orchestrations.common_tasks import create_common_tasks
import os

DAG_ID = os.path.basename(__file__).replace(".py", "")

default_args = {
    "owner": "otto",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "trigger_rule": "all_done",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    DAG_ID,
    default_args=default_args,
    start_date=datetime(2022, 1, 1),
    schedule_interval="*/5 12 * * *",  # Every 5 minutes from 12pm
    catchup=False,
    max_active_runs=1,
    tags=["12pm", "example", "test"],
) as dag:

    create_common_tasks(dag)
