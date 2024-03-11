# Create variables in the Airflow UI with the following names and values:
# OTTO: 123

from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define the DAG
dag = DAG(
    "using_variables",
    start_date=datetime(2022, 1, 1),
    schedule="* * * * *",  # Run every minute
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=["variables"],
    params={"example_key": "example_value"},
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
