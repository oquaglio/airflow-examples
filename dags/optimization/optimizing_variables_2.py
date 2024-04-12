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
    dag_id=STACK_NAME,
    default_args=default_args,
    schedule_interval="0 */12 * * *",
    start_date=days_ago(0),
    tags=["example", "test"],
) as dag:

    dag.doc_md = dag_docs

    test_script = f"""
    echo "Starting test script to echo parameters..."
    echo "AIRFLOW_HOME: $AIRFLOW_HOME"
    echo "DB_NAME: {DB_NAME}"
    echo "SCHEMA_NAME: {DB_SCHEMA_NAME}"
    echo "STACK_NAME: {STACK_NAME}"
    echo "DBT_PROFILES_DIR: {DBT_PROFILES_DIR}"
    echo "TARGET: {TARGET}"
    echo --args '{{"db_name": "{DB_NAME}", "schema_name": "{DB_SCHEMA_NAME}"}}'
    echo "All parameters have been echoed."
    """

    load_script = f"""
    echo "Loading source freshness and dbt test results into Snowflake... "
    dbt run-operation local_load_data_to_sf --args '{{"db_name": "{DB_NAME}", "schema_name": "{DB_SCHEMA_NAME}"}}' --project-dir $AIRFLOW_HOME/dags/{STACK_NAME}/dbt/ --profiles-dir {DBT_PROFILES_DIR} --target {TARGET}
    echo "Data Load Completed"
    """

    bash_test = BashOperator(task_id="bash_test", bash_command=test_script, trigger_rule="all_done")

    load_results = BashOperator(task_id="load_results_to_snowflake", bash_command=load_script, trigger_rule="all_done")

    bash_test >> load_results
