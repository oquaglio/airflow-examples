from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import json


# Utility function to divide a list into n chunks
def divide_chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i : i + n]


# Load the list of strings from an Airflow Variable (assuming JSON format)
list_of_strings = json.loads(Variable.get("list_of_strings"))

# Number of tasks per DAG
tasks_per_dag = 5

# Divide the list into subsets for different DAGs
subsets = list(divide_chunks(list_of_strings, tasks_per_dag))

# Dynamically generate DAGs based on subsets
for i, subset in enumerate(subsets):
    dag_id = f"dynamic_dag_{i}"
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": datetime(2022, 1, 1),
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }

    # Create a new DAG for each subset
    with DAG(dag_id, catchup=False, default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
        start = DummyOperator(task_id="start")

        # Create tasks for each string in the subset
        for task_str in subset:
            task = DummyOperator(task_id=f"task_{task_str}")
            start >> task

        globals()[dag_id] = dag  # Add the DAG to the globals() so Airflow can find it
