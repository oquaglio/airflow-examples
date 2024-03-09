# Create variables in the Airflow UI with the following names and values:
# OTTO: 123

from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

OTTO = "{{ var.value.OTTO }}"

# Define the DAG
dag = DAG(
    "using_variables",
    start_date=datetime(2022, 1, 1),
    schedule_interval="* * * * *",  # Run every minute
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=["otto", "variables"],
    params={"example_key": "example_value"},
)

task = BashOperator(
    task_id="task1",
    # Use Airflow template to get the variable value at runtime
    bash_command='echo "Value of Var OTTO: {{ var.value.OTTO }}"',
    dag=dag,
)

task2 = BashOperator(
    task_id="task2",
    # Use Airflow template to get the variable value at runtime
    bash_command='echo "Value of Var OTTO: ${OTTO}"',
    dag=dag,
)

task >> task2
