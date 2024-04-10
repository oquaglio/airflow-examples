def divide_tasks_into_windows(input_list, number_of_windows):
    """Divides a list into a specified number of windows"""
    k, m = divmod(len(input_list), number_of_windows)
    return (input_list[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(number_of_windows))


def choose_tasks_for_current_window(**kwargs):
    """Determines the tasks in the active window based on the execution time, cycle duration for all windows, and mins per window."""
    from datetime import datetime

    task_group_id = kwargs.get("task_group_id")
    list_of_tasks = kwargs.get("list_of_tasks")
    minutes_per_window = kwargs.get("minutes_per_window")
    # Duration before re-starting the cycle (before starting back at first task in the list).
    # E.g. if you want to schedule each task twice per hour, set this to 30.
    # Setting 60 will schedule each task once per hour
    cycle_duration_mins = kwargs.get("cycle_duration_mins", 60)
    # For testing, allows overriding the execution_date
    test_time = kwargs.get("test_time")

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

    # Validate cycle_duration_mins
    if not cycle_duration_mins or cycle_duration_mins <= 0:
        raise ValueError("Invalid or missing 'cycle_duration_mins'. It must be a positive number.")

    # Calculate the number of windows based on the total minutes for windows and window duration
    number_of_windows = cycle_duration_mins // minutes_per_window

    # Ensure the total minutes for windows divides evenly into the specified windows; adjust logic if it does not
    if cycle_duration_mins % minutes_per_window != 0:
        raise ValueError("The total minutes for windows does not divide evenly by the minutes per window.")

    # Calculate the window index based on execution_date's minute within the cycle
    window_index = (execution_date.minute // minutes_per_window) % number_of_windows
    tasks_for_current_window = list(divide_tasks_into_windows(list_of_tasks, number_of_windows))[window_index]

    task_ids_to_run = []
    for task_name in tasks_for_current_window:
        task_id = f"{task_group_id}.tg_{task_name.replace(' ', '_')}.task_{task_name.replace(' ', '_')}_first_task"
        task_ids_to_run.append(task_id)

    return task_ids_to_run


if __name__ == "__main__":
    from datetime import datetime

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

    # Test to make sure all tasks are allotted a window
    minutes_per_window = 5
    cycle_duration_mins = 60
    print(f"Selected stacks for cycle duration: {cycle_duration_mins} and mins per window: {minutes_per_window}")

    for minute in [minutes_per_window * i for i in range(0, 60 // minutes_per_window)]:
        test_override_time = datetime(2022, 1, 1, 15, minute)
        print(
            f"Selected stacks for {minutes_per_window} min window at minute {test_override_time.minute}:",
            choose_tasks_for_current_window(
                task_group_id="tg_stacks",
                list_of_tasks=list_of_stacks,
                minutes_per_window=minutes_per_window,
                cycle_duration_mins=cycle_duration_mins,
                test_time=test_override_time,
            ),
        )

    # Test to make sure all tasks are allotted a window
    print("")
    minutes_per_window = 10
    cycle_duration_mins = 60
    print(f"Selected stacks for cycle duration: {cycle_duration_mins} and mins per window: {minutes_per_window}")

    for minute in [minutes_per_window * i for i in range(0, 60 // minutes_per_window)]:
        test_override_time = datetime(2022, 1, 1, 15, minute)
        print(
            f"Selected stacks for {minutes_per_window} min window at minute {test_override_time.minute}:",
            choose_tasks_for_current_window(
                task_group_id="tg_stacks",
                list_of_tasks=list_of_stacks,
                minutes_per_window=minutes_per_window,
                cycle_duration_mins=cycle_duration_mins,
                test_time=test_override_time,
            ),
        )

    print("")
    minutes_per_window = 5
    cycle_duration_mins = 30
    print(f"Selected stacks for cycle duration: {cycle_duration_mins} and mins per window: {minutes_per_window}")

    for minute in [minutes_per_window * i for i in range(0, 60 // minutes_per_window)]:
        test_override_time = datetime(2022, 1, 1, 15, minute)
        print(
            f"Selected stacks for {minutes_per_window} min window at minute {test_override_time.minute}:",
            choose_tasks_for_current_window(
                task_group_id="tg_stacks",
                list_of_tasks=list_of_stacks,
                minutes_per_window=minutes_per_window,
                cycle_duration_mins=cycle_duration_mins,
                test_time=test_override_time,
            ),
        )
