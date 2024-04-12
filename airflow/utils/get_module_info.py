import importlib
import inspect


def get_classes_from_module(module_name):
    """
    Dynamically import a module and return a list of its classes.

    Args:
    - module_name (str): The name of the module to inspect.

    Returns:
    - List[Tuple[str, type]]: A list of tuples where each tuple contains the name of the class and the class itself.
    """
    # Dynamically import the module
    module = importlib.import_module(module_name)

    # Inspect the module and filter for classes
    classes = inspect.getmembers(module, inspect.isclass)

    return classes


def print_classes(classes):
    for name, cls in classes:
        print(f"{name}: {cls}")


def print_classes_in_module(module_name):
    classes = get_classes_from_module(module_name)
    print(f"\nClasses in {module_name}:")
    print_classes(classes)


# Airflow
print_classes_in_module("airflow.sensors.time_delta")
print_classes_in_module("airflow.www.utils")
print_classes_in_module("airflow.utils.log")

# Common
print_classes_in_module("timedelta")
print_classes_in_module("datetime")
print_classes_in_module("collections")
