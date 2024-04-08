import importlib
import inspect

# The name of the module you want to inspect
module_name = "airflow.sensors.time_delta"

# Dynamically import the module
module = importlib.import_module(module_name)

# Inspect the module and filter for classes
classes = [member for member, member_type in inspect.getmembers(module, inspect.isclass)]

# Print the list of class names
print(classes)
