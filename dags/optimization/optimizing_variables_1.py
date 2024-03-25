from airflow import DAG
from airflow.operators.bash import BashOperator

# from airflow.models import Variable

# from common_operators.invoke_lambda import InvokeLambda
from datetime import datetime, timedelta
import os

# import boto3
# import botocore


# Dag documentation
dag_docs = """
### stack_raw_niw_wmg_complyflow DAG Documentation
This DAG is a data pipeline that retrieves data from complyflow API source and stores it in S3 and then Snowflake
"""

# Tasks documentation
task1_doc = """
### Task Documentation
This task retrieves data from complyflow API and saves it to the S3 output location. The data is saved to Snowflake.
"""

# Tasks documentation
task2_doc = """
### Task Documentation
This task retrieves data from complyflow API and saves it to the S3 output location. The data is saved to Snowflake.
"""

# Default settings applied to all tasks.
# More details on the default arguments that can be used https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html#default-arguments
DEFAULT_ARGS = {
    "owner": "mawdu",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


# Environment variables
# env = Variable.get("ENVIRONMENT_NAME")
DAG_ID = os.path.basename(__file__).replace(".py", "")
print(f"DAG '{DAG_ID}' parsed")
STACK_NAME = "stack_raw_niw_wmg_complyflow"  # CHANGE TO MATCH YOUR STACK/PIPELINE NAME!
LAMBDA_NAME = "stack-raw-niw-wmg-complyflow-loader-lambda"
SCHEMA_NAME = STACK_NAME.replace("stack_", "").upper()
# DB_NAME = Variable.get("SNOWFLAKE_DB_NAME")
# TARGET = Variable.get("ENVIRONMENT_NAME")
BATCH_STRING = '{{ macros.ds_format( ts_nodash , "%Y%m%dT%H%M%S", "%Y/%m/%d/%H%M%S") }}'
# DBT_PROFILES_DIR = Variable.get("DBT_PROFILES_DIR")

tables_to_process = [
    "employee_list",
    "employee_training",
    "employee_documents",
    "employee_hr_fields",
    "employee_worker_categories",
    "staff_training",
    "staff_documents",
    "staff_hr_custom_fields",
    "staff_categories",
    "staff_list",
]
# tables_to_process =['employee_list']


# Instantiate DAG
with DAG(
    dag_id=DAG_ID,  # The name of your dag
    start_date=datetime(2024, 2, 9),  # The specific start date when your dag will begin
    schedule="@once",
    default_args=DEFAULT_ARGS,  # The default arguments taken from above
    catchup=False,  # If API load accepts date specific filtering, make this value equal to True
    max_active_runs=1,  # Enforces that only one instance of this DAG can run at a time. DAG run will finish before moving to the next run.
) as dag:

    dag.doc_md = dag_docs
    # Invoke Lambda
    for table in tables_to_process:

        env = "{{ var.value.ENVIRONMENT_NAME }}"

        # Add/Edit Lambda parameters here
        lambda_event = {
            "batch_string": BATCH_STRING,
            "api_url": f"stack_raw_niw_wmg_complyflow_{env}_url",
            "employee_url": f"stack_raw_niw_wmg_complyflow_{env}_employee_url",
            "staff_url": f"stack_raw_niw_wmg_complyflow_{env}_staff_url",
            "username": f"stack_raw_niw_wmg_complyflow_{env}_username",
            "pwd": f"stack_raw_niw_wmg_complyflow_{env}_password",
            "secret_id": "data_utility_pipeline_sources",
            "tables_to_process": table,
        }

        sf_table_name_upper = table.upper()
        STAGE_PATH = f"/landing/{BATCH_STRING}/{table}/{table}"

        # lambda_loader_task = BashOperator(
        #     task_id=f"lambda_loader_task_{table}", event=lambda_event, lambda_name=LAMBDA_NAME
        # )
        # lambda_loader_task.doc_md = task1_doc

        bash_script_template = """
            echo "Data Load for {{params.TTN}} table started."
            $HOME/.local/bin/dbt run-operation load_data_to_raw --args "{{'{'}}db_name: {{params.DB_NAME}}, schema_name: {{params.SCHEMA_NAME}}, stage_path: {{params.STAGE_PATH}}, batch_string: {{params.BATCH_STRING}}, target_table_name: {{params.TTN}}{{'}'}}" --project-dir $HOME/dags/{{params.STACK_NAME}}/dbt/ --profiles-dir {{params.DBT_PROFILES_DIR}} --target {{params.TARGET}}
            echo "Data Load for {{params.TTN}} complete."
        """

        # bash_script = bash_script_template.format(
        #     STACK_NAME=STACK_NAME,
        #     SCHEMA_NAME=SCHEMA_NAME,
        #     STAGE_PATH=STAGE_PATH,
        #     BATCH_STRING=BATCH_STRING,
        #     TTN=sf_table_name_upper,
        # )

        dbt_load_to_snowflake_task = BashOperator(
            task_id=f"dbt_load_to_snowflake_task_{table}",
            bash_command=bash_script_template,
            task_concurrency=1,
            params={
                "DB_NAME": "{{ var.value.SNOWFLAKE_DB_NAME }}",
                "DBT_PROFILES_DIR": "{{ var.value.DBT_PROFILES_DIR }}",
                "TARGET": "{{ var.value.ENVIRONMENT_NAME }}",
                "STACK_NAME": STACK_NAME,
                "SCHEMA_NAME": SCHEMA_NAME,
                "STAGE_PATH": STAGE_PATH,
                "BATCH_STRING": BATCH_STRING,
                "TTN": sf_table_name_upper,
            },
        )

        dbt_load_to_snowflake_task.doc_md = task2_doc

        # lambda_loader_task
        dbt_load_to_snowflake_task
