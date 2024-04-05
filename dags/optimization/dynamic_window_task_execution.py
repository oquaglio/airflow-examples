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
]


# Function to divide the list into 12 windows
def divide_stacks_into_windows(input_list, number_of_windows=12):
    """Divides a list into a specified number of windows"""
    k, m = divmod(len(input_list), number_of_windows)
    return (input_list[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(number_of_windows))


# Dividing the list of strings into 12 windows
windows = list(divide_stacks_into_windows(list_of_stacks))
print(windows)


# Function to determine which window to execute
def choose_stacks_for_current_window(**kwargs):
    """Determines the active window based on the current time"""
    current_minute = datetime.now().minute
    window_index = current_minute // 5
    window_tasks = windows[window_index % 12]  # Ensure cycling every hour
    task_ids = [f"string_tasks.task_{s.replace(' ', '_')}" for s in window_tasks]
    return task_ids


# def choose_stacks_for_current_window(**kwargs):
#     return ["string_tasks.task_stack_1", "string_tasks.task_stack_2", "string_tasks.task_stack_3"]


# Function to print the string (simulate task execution)
def print_string(task_string):
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

    with TaskGroup("string_tasks") as tg:
        branch_op = BranchPythonOperator(
            task_id="branch_window",
            python_callable=choose_stacks_for_current_window,
            provide_context=True,
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
    selected_tasks = choose_stacks_for_current_window()
    print("Selected Stacks for Current Window:", selected_tasks)
