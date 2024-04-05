from datetime import timedelta
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago

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


# Function to divide the list into 12 windows
def divide_tasks_into_windows(input_list, number_of_windows):
    """Divides a list into a specified number of windows"""
    k, m = divmod(len(input_list), number_of_windows)
    return (input_list[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(number_of_windows))


# Dividing the list of strings into 12 windows
# windows = list(divide_stacks_into_windows(list_of_stacks))
# print(windows)


# Split tasks up over 1hr cycle based on minutes_per_window
def choose_tasks_for_current_window(**kwargs):
    """Determines the active window based on the execution time and configured window duration."""
    from datetime import datetime

    task_group_id = kwargs.get("task_group_id")
    list_of_tasks = kwargs.get("list_of_tasks")
    # Assuming minutes_per_window is now directly provided
    minutes_per_window = kwargs.get("minutes_per_window")
    test_time = kwargs.get("test_time")  # For testing, allows overriding the execution_date

    # Use test_time if provided, else extract the execution_date from the context
    if test_time and isinstance(test_time, datetime):
        execution_date = test_time
    else:
        execution_date = kwargs.get("execution_date")
        if not execution_date:
            raise ValueError("execution_date not found in context and no test_time provided")

    # Validate that minutes_per_window is provided and is a valid number
    if not minutes_per_window or minutes_per_window <= 0:
        raise ValueError("Invalid or missing 'minutes_per_window'. It must be a positive number.")

    # Calculate the number of windows based on the cycle duration and window duration
    # This replaces the direct use of 'number_of_windows'
    number_of_windows = 60 // minutes_per_window

    # Ensure the cycle divides evenly into the specified windows; adjust logic if it does not
    if 60 % minutes_per_window != 0:
        raise ValueError("The cycle duration (of 60 mins) does not divide evenly by the minutes per window.")

    # Calculate the window index based on execution_date's minute within the cycle
    window_index = (execution_date.minute // minutes_per_window) % number_of_windows
    tasks_for_current_window = list(divide_tasks_into_windows(list_of_tasks, number_of_windows))[
        window_index
    ]  # Determine the set of tasks for the current window

    # Generate task IDs for the current window
    task_ids = [f"{task_group_id}.task_{s.replace(' ', '_')}" for s in tasks_for_current_window]
    return task_ids


# def choose_stacks_for_current_window(**kwargs):
#     return ["string_tasks.task_stack_1", "string_tasks.task_stack_2", "string_tasks.task_stack_3"]


# Function to print the string (simulate task execution)
def print_string(task_string):
    if task_string == "stack_15":
        raise ValueError(f"Simulated failure for task: {task_string}")
    print(f"Executing task for string: {task_string}")


# Defining the DAG
with DAG(
    "dynamic_window_task_execution",
    default_args={
        "start_date": days_ago(1),
        "catchup": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    schedule_interval="*/5 * * * *",  # Every 5 minutes
    max_active_runs=1,
    tags=["example", "test"],
) as dag:

    start = DummyOperator(task_id="start")

    task_group_id = "stack_tasks"

    with TaskGroup(task_group_id) as tg:
        branch_op = BranchPythonOperator(
            task_id="branch_window",
            python_callable=choose_tasks_for_current_window,
            op_kwargs={
                "task_group_id": task_group_id,
                "list_of_tasks": list_of_stacks,
                "minutes_per_window": 5,  # must match the DAG schedule interval
            },
        )

        # Create tasks for each string
        for stack in list_of_stacks:
            task = PythonOperator(
                task_id=f"task_{stack.replace(' ', '_')}",
                python_callable=print_string,
                op_args=[stack],
                retries=1,
                retry_delay=timedelta(seconds=5),
            )
            branch_op >> task

    end = DummyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,  # Ensures `end` runs regardless of previous tasks' states
    )

    start >> tg >> end


if __name__ == "__main__":
    from datetime import datetime

    # Test to make sure all tasks are allotted a window
    minutes_per_window = 5
    for minute in [minutes_per_window * i for i in range(0, 60 // minutes_per_window)]:
        test_override_time = datetime(2022, 1, 1, 15, minute)
        print(
            f"Selected stacks for {minutes_per_window} min window at minute {test_override_time.minute}:",
            choose_tasks_for_current_window(
                task_group_id="stack_tasks",
                list_of_tasks=list_of_stacks,
                minutes_per_window=minutes_per_window,
                test_time=test_override_time,
            ),
        )
