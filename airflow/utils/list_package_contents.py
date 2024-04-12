import importlib
import pkgutil
import inspect


def list_modules_and_classes(package_name):
    """
    Lists all submodules, subpackages, and classes within a given package.

    Args:
    - package_name (str): The name of the package to inspect.
    """
    # Dynamically import the package
    package = importlib.import_module(package_name)

    print(f"Inspecting package: {package_name}\n{'=' * 30}")

    # Iterate through the package's modules and subpackages
    for importer, modname, ispkg in pkgutil.walk_packages(package.__path__, prefix=package_name + "."):
        try:
            # Dynamically import the module
            module = importlib.import_module(modname)
            print(f"\nModule: {modname}")

            # List classes defined in the module
            classes = inspect.getmembers(module, inspect.isclass)
            classes = [cls for cls in classes if cls[1].__module__ == modname]  # Filter classes defined in this module
            if classes:
                print("  Classes:")
                for name, _ in classes:
                    print(f"    - {name}")
            else:
                print("  No classes found")

        except ImportError as e:
            print(f"  Could not import module: {e}")


if __name__ == "__main__":

    list_modules_and_classes(package_name="airflow")
