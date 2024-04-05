from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup

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


# Function to determine which window to execute
def choose_tasks_for_current_window(**kwargs):
    """Determines the active window based on the current time"""
    task_group_id = kwargs.get("task_group_id")
    list_of_tasks = kwargs.get("list_of_tasks")
    number_of_windows = kwargs.get("number_of_windows", 12)
    current_minute = datetime.now().minute
    window_index = current_minute // 5
    tasks_for_current_window = list(divide_tasks_into_windows(list_of_tasks, number_of_windows))[
        window_index % number_of_windows
    ]  # Ensure cycling every hour
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
        "start_date": datetime(2022, 1, 1),
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
    number_of_windows = 12  # e.g, if schedule is 5 mins, make this 12

    with TaskGroup(task_group_id) as tg:
        branch_op = BranchPythonOperator(
            task_id="branch_window",
            python_callable=choose_tasks_for_current_window,
            op_kwargs={
                "task_group_id": task_group_id,
                "list_of_tasks": list_of_stacks,
                "number_of_windows": number_of_windows,
            },
        )

        # Create tasks for each string
        for stack in list_of_stacks:
            task = PythonOperator(
                task_id=f"task_{stack.replace(' ', '_')}",
                python_callable=print_string,
                op_args=[stack],
            )
            branch_op >> task

    end = DummyOperator(task_id="end")

    start >> tg >> end


if __name__ == "__main__":
    print(
        "Selected Stacks for Current Window (of 12):",
        choose_tasks_for_current_window(
            task_group_id="stack_tasks", list_of_tasks=list_of_stacks, number_of_windows=12
        ),
    )

    print(
        "Selected Stacks for Current Window (of 1):",
        choose_tasks_for_current_window(task_group_id="stack_tasks", list_of_tasks=list_of_stacks, number_of_windows=1),
    )

    print(
        "Selected Stacks for Current Window (of 6):",
        choose_tasks_for_current_window(task_group_id="stack_tasks", list_of_tasks=list_of_stacks, number_of_windows=6),
    )
