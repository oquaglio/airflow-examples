# Dag documentation
dag_docs = """
### This DAGS demos bash script var substitution
"""

import datetime
import json
import os
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

stacks_config = Variable.get("STACKS_DATA_MONITORING_CONFIG", deserialize_json=True)

# DAG Configuration
STACK_NAME = "stack_common_waio_data_quality"
DB_NAME = Variable.get("SNOWFLAKE_DB_NAME")
DB_SCHEMA_NAME = STACK_NAME.replace("stack_", "").upper()
TARGET = "HOUSEKEEPING_" + Variable.get("ENVIRONMENT_NAME")
DBT_PROFILES_DIR = Variable.get("DBT_PROFILES_DIR")


default_args = {
    "owner": "mawdu",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "trigger_rule": "all_done",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id=STACK_NAME,
    default_args=default_args,
    schedule_interval="0 */12 * * *",
    start_date=days_ago(0),
    tags=["data_quality", "monitoring", "admin"],
) as dag:

    dag.doc_md = dag_docs

    load_script = """
    echo "Loading source freshness and dbt test results into into Snowflake... "
    ${{AIRFLOW_HOME}}/.local/bin/dbt run-operation local_load_data_to_sf --args "{{db_name: {DB_NAME}, schema_name: {SCHEMA_NAME}}}" --project-dir ${{AIRFLOW_HOME}}/dags/{STACK_NAME}/dbt/ --profiles-dir {DBT_PROFILES_DIR} --target {TARGET}
    echo "Data Load Completed"
    """

    load_results = BashOperator(task_id="load_results_to_snowflake", bash_command=load_script, trigger_rule="all_done")

    load_results
