# Create variables in the Airflow UI with the following names and values:
# OTTO: 123

from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# Define the DAG
dag = DAG("example_dag", start_date=datetime(2022, 1, 1), schedule_interval="@daily")

# Define a task that dynamically reads the "s3_bucket_name" variable at runtime
task = BashOperator(
    task_id="example_task",
    # Use Airflow template to get the variable value at runtime
    bash_command='echo "Value of Var OTTO: {{ var.value.OTTO }}"',
    dag=dag,
)
