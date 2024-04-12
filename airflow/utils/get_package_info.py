import pkgutil
import importlib


def list_modules_in_package(package_name):
    """
    Lists all submodules and subpackages within a given package.
    """
    # Dynamically import the package
    package = importlib.import_module(package_name)

    # Get the package path
    package_path = package.__path__
    prefix = f"{package_name}."

    for importer, modname, ispkg in pkgutil.walk_packages(package_path, prefix=prefix):
        print(modname)


if __name__ == "__main__":
    list_modules_in_package(package_name="airflow")

    list_modules_in_package(package_name="airflow_dbt")
