# This DAG demonstrates how to use Airflow Variables in your DAG without ever using Variables.get at root of Python
# definintion file.
# Create variables in the Airflow UI with the following names and values:
# OTTO: 123

from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os

DAG_ID = os.path.basename(__file__).replace(".py", "")
STACK_NAME = "stack_" + DAG_ID
BATCH_STRING = '{{ macros.ds_format( ts_nodash , "%Y%m%dT%H%M%S", "%Y/%m/%d/%H%M%S") }}'
print(f"DAG '{DAG_ID}' parsed")

# Define the DAG
dag = DAG(
    DAG_ID,
    start_date=datetime(2022, 1, 1),
    # schedule="* * * * *",  # Run every minute
    schedule="@once",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=60),
    tags=["variables"],
    params={"example_key": "example_value"},
    provide_context=True,  # cache vars
)

print_var = BashOperator(
    task_id="print_var",
    # Use Airflow template to get the variable value at runtime
    bash_command='echo "Value of Var OTTO: {{ var.value.OTTO }}"',
    dag=dag,
)

bash_script = """
    echo "{{ var.value.OTTO }} rules."
"""

dbt_load_to_snowflake_task = BashOperator(task_id=f"dbt_load_to_snowflake_task", bash_command=bash_script, dag=dag)

print_var >> dbt_load_to_snowflake_task

for task_num in ["1", "2", "3"]:

    bash_script = """
    echo "Task #{{ params.task_num }} is running. Environment: {{ var.value.ENVIRONMENT_NAME }}"
    """

    env = "{{ var.value.ENVIRONMENT_NAME }}"

    bash_task = BashOperator(
        task_id=f"do_stuff_task_{task_num}", bash_command=bash_script, params={"task_num": task_num}, dag=dag
    )

    dbt_load_to_snowflake_task >> bash_task


def prepare_lambda_event(**kwargs):
    # Fetch environment name from Airflow Variables
    # print(kwargs)
    # print("Task isntance: " + kwargs["ti"])
    env = kwargs["templates_dict"]["env"]
    batch_string = kwargs["templates_dict"]["batch_string"]
    table = kwargs["templates_dict"]["table"]

    lambda_event = {
        "batch_string": batch_string,
        "api_url": f"stack_raw_niw_wmg_complyflow_{env}_url",
        "employee_url": f"stack_raw_niw_wmg_complyflow_{env}_employee_url",
        "staff_url": f"stack_raw_niw_wmg_complyflow_{env}_staff_url",
        "username": f"stack_raw_niw_wmg_complyflow_{env}_username",
        "pwd": f"stack_raw_niw_wmg_complyflow_{env}_password",
        "secret_id": "data_utility_pipeline_sources",
        "tables_to_process": table,
    }

    # For demonstration, printing the event. You might replace this with your actual logic.
    import json

    print(json.dumps(lambda_event, indent=4))
    # Return the lambda_event for XCom

    kwargs["ti"].xcom_push(key=f"lambda_event", value=lambda_event)
    # return lambda_event


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

for table in tables_to_process:
    prepare_lambda_event_task = PythonOperator(
        task_id=f"prepare_lambda_event_{table}",
        python_callable=prepare_lambda_event,
        templates_dict={
            "env": "{{ var.value.ENVIRONMENT_NAME }}",
            "batch_string": BATCH_STRING,
            "table": table,
        },
        dag=dag,
    )

    # simulate lambda invoke
    # the only way to get the lambda_event m by via XCOMS
    invoke_lambda_task = BashOperator(
        task_id=f"invoke_lambda_{table}",
        # bash_command=f"echo 'Invoking Lambda for {table} using lambda_event: {{ ti.xcom_pull(task_ids='prepare_lambda_event_{table}', key='lambda_event') }}'",
        bash_command=f"echo 'Invoking Lambda for {table} using lambda_event: {{ ti.xcom_pull(task_ids='prepare_lambda_event_{table}') }}'",
        dag=dag,
    )

    dbt_load_to_snowflake_task >> prepare_lambda_event_task
    prepare_lambda_event_task >> invoke_lambda_task

# lambda_loader_task = InvokeLambda(
#         task_id=f"lambda_loader_task_{table}",
#         event = lambda_event,
#         lambda_name = LAMBDA_NAME
#     )
#     lambda_loader_task.doc_md = task1_doc
