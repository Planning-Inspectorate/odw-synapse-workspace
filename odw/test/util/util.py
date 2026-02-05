from odw.test.util.session_util import PytestSparkSessionUtil
import os
from filelock import FileLock
import json
from typing import Dict, Union


def generate_local_path(path: str):
    return os.path.join(PytestSparkSessionUtil().get_spark_warehouse_name(), path)


def get_all_files_in_directory(inst, source_path: str):
    directory_contents = os.listdir(source_path)
    files = [os.path.join(source_path, x) for x in directory_contents if os.path.isfile(os.path.join(source_path, x))]
    folders = [os.path.join(source_path, x) for x in directory_contents if os.path.isdir(os.path.join(source_path, x))]
    while folders:
        subfolder = folders.pop()
        subfolder_contents = os.listdir(subfolder)
        new_files = [os.path.join(subfolder, x) for x in subfolder_contents if os.path.isfile(os.path.join(subfolder, x))]
        files += new_files
        new_folders = [os.path.join(subfolder, x) for x in subfolder_contents if os.path.isdir(os.path.join(subfolder, x))]
        folders += new_folders
    return files


def format_to_adls_path(inst, container_name: str, blob_path: str, storage_name: str = None, storage_endpoint: str = None) -> str:
    print("container_name: ", container_name)
    print("blob_path: ", blob_path)
    # This gets written to the 'spark-warehouse-xxxxxx/' folder
    return os.path.join(container_name, blob_path)


def format_adls_path_to_local_path(inst, container_name: str, blob_path: str, storage_name: str = None, storage_endpoint: str = None) -> str:
    """
    Format adlsg2 parameters to reflect a location in the `spark-warehouse-xxxxxx`
    """
    return os.path.join(PytestSparkSessionUtil().get_spark_warehouse_name(), container_name, blob_path)


def add_orchestration_entry(entry: Dict[str, Union[str, int]]):
    orchestration_path = os.path.join(PytestSparkSessionUtil().get_spark_warehouse_name(), "odw-config", "orchestration", "orchestration.json")
    new_definitions = [
        entry
    ]
    lock = FileLock(f"{orchestration_path}.lock")
    with lock.acquire():
        with open(orchestration_path, "r+") as f:
            existing_content = json.load(f)
            f.seek(0)
            updated_content = {"definitions": existing_content["definitions"] + new_definitions}
            json.dump(updated_content, f, indent=4)
