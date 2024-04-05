from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.date_time import DateTimeSensor
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

import math

# Define your JSON object here
task_names = ["task1", "task2", "task3", "task4", "task5"]

# The DAG definition
with DAG(
    "dynamic_task_group_staggered_dag",
    default_args={
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="A DAG that creates tasks in a task group with staggered starts",
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    tags=["example", "test"],
) as dag:

    start_dag = DummyOperator(task_id="start_dag")

    with TaskGroup("dynamic_tasks_group") as tg:
        # Calculate the delay needed to evenly distribute tasks across the hour
        total_seconds = 3600  # 60 minutes * 60 seconds
        delay_interval = total_seconds // len(task_names)

        for index, task_name in enumerate(task_names):
            # Calculate the delay for the current task
            delay_for_task = timedelta(seconds=index * delay_interval)
            target_datetime = datetime.now() + delay_for_task

            wait_for_time = DateTimeSensor(
                task_id=f"wait_for_{task_name}",
                target_time=target_datetime.strftime("%Y-%m-%d %H:%M:%S"),
                mode="poke",
                poke_interval=10,  # Check every 10 seconds
                timeout=math.ceil(delay_interval),  # Should not exceed the next task's start time, rounded up
            )

            execute_task = PythonOperator(
                task_id=f"execute_{task_name}",
                python_callable=lambda **kwargs: print(f"Executing {task_name}"),
            )

            wait_for_time >> execute_task

    end_dag = DummyOperator(task_id="end_dag")

    start_dag >> tg >> end_dag
