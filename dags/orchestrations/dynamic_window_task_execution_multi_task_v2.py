from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import BranchPythonOperator, PythonOperator

# from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
import os

# import sys

# sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from orchestrations.utils.scheduling_utils import choose_tasks_for_current_window

DAG_ID = os.path.basename(__file__).replace(".py", "")

# Example list of strings
list_of_stacks = [
    "stack_1",
    "stack_2",
    "stack_3",
    "stack_4",
    "stack_5",
    "stack_6",
    "stack_7",
    "stack_8",
    "stack_9",
    "stack_10",
    "stack_11",
    "stack_12",
    "stack_13",
    "stack_14",
    "stack_15",
    "stack_16",
    "stack_17",
    "stack_18",
    "stack_19",
    "stack_20",
    "stack_21",
    "stack_22",
    "stack_23",
    "stack_24",
    "stack_25",
    "stack_26",
    "stack_27",
    "stack_28",
    "stack_29",
    "stack_30",
    "stack_31",
    "stack_32",
    "stack_33",
]


# Function to print the string (simulate task execution)
def first_task(task_string, **kwargs):
    import time

    print(f"Executing second task for: {task_string}")
    delay_in_secs = kwargs.get("delay_in_secs")
    time.sleep(delay_in_secs)
    print(f"Done.")


def second_task(task_string, **kwargs):
    import time

    print(f"Executing second task for: {task_string}")
    delay_in_secs = kwargs.get("delay_in_secs")
    time.sleep(delay_in_secs)
    print(f"Done.")


def delay_execution(**kwargs):
    """Custom delay function."""
    import time

    delay_in_secs = kwargs.get("delay_in_secs")

    time.sleep(delay_in_secs)  # Sleep for 60 seconds


# DAG definition
with DAG(
    DAG_ID,
    default_args={
        "owner": "otto",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",  # Every 5 minutes
    max_active_runs=1,  # It's safe to allow parallel runs since the tasks are grouped into windows
    tags=["example", "test"],
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")

    with TaskGroup("tg_stacks") as tg_stacks:
        # choose the tasks for the current window
        select_tasks_operator = BranchPythonOperator(
            task_id="choose_tasks_to_run",
            python_callable=choose_tasks_for_current_window,
            do_xcom_push=False,
            retries=0,
            op_kwargs={
                "task_group_id": "tg_stacks",
                "list_of_tasks": list_of_stacks,
                "minutes_per_window": 5,  # split tasks into 12 groups
            },
        )

        # Create tasks for each stack
        for stack in list_of_stacks:
            with TaskGroup(f"tg_{stack.replace(' ', '_')}") as tg_stack_tasks:
                task1 = PythonOperator(
                    task_id=f"task_{stack.replace(' ', '_')}_first_task",
                    python_callable=first_task,
                    op_kwargs={"delay_in_secs": 30},
                    do_xcom_push=False,
                    op_args=[stack],
                    retries=2,
                    retry_delay=timedelta(seconds=10),
                )

                # skipped of all reties on task 1 fail
                task2 = PythonOperator(
                    task_id=f"task_{stack.replace(' ', '_')}_second_task",
                    python_callable=second_task,
                    op_kwargs={"delay_in_secs": 30},
                    do_xcom_push=False,
                    op_args=[stack],
                    retries=2,
                    retry_delay=timedelta(seconds=10),
                )

                select_tasks_operator >> task1 >> task2

    # run when all tasks in task groups complete (success, failed, or skipped)
    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    start >> tg_stacks >> end
