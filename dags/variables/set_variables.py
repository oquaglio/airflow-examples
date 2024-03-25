from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python import PythonOperator
import json
import os

DAG_ID = os.path.basename(__file__).replace(".py", "")
STACK_NAME = "stack_common_niw_snowflake"
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
DBT_PROFILES_PATH = f"{AIRFLOW_HOME}/dags/{STACK_NAME}/dbt/profiles.yml"
DBT_PROFILES_DIR = f"{AIRFLOW_HOME}/dags/{STACK_NAME}/dbt"
ENVIRONMENT_NAME = "DEV"
SNOWFLAKE_DB_NAME = "NIW_DEV"
STACKS_DATA_MONITORING_CONFIG = '["stack_1","stack_2","stack_3"]'

default_args = {
    "owner": "niwdu",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "trigger_rule": "all_done",
}


def fetch_and_set_variables():
    from airflow.models import Variable

    Variable.set("ENVIRONMENT_NAME", ENVIRONMENT_NAME)
    Variable.set("SNOWFLAKE_DB_NAME", SNOWFLAKE_DB_NAME)
    Variable.set("DBT_PROFILES_PATH", DBT_PROFILES_PATH)
    Variable.set("DBT_PROFILES_DIR", DBT_PROFILES_DIR)
    Variable.set("STACKS_DATA_MONITORING_CONFIG", STACKS_DATA_MONITORING_CONFIG)


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Set the Airflow Variables.",
    schedule="@once",
    start_date=days_ago(0),
    tags=["admin", "variables"],
) as dag:

    dag.doc_md = f"""
    ### {DAG_ID} DAG Documentation
    This DAG runs on demand to copy Snowflake configuration from AWS Secrets Manager into Airflow variables for use in DAGs
    """

    set_variables = PythonOperator(task_id="set_variables", python_callable=fetch_and_set_variables)
