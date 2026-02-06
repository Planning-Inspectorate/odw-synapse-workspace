from odw.test.util.session_util import PytestSparkSessionUtil
import os
from filelock import FileLock
import json
from typing import Dict, Union


"""
This module contains various utiliity functions that can be used to run the etl process locally via patching
"""

def generate_local_path(path: str):
    """
    Patch function for Util.get_path_to_file , which mimics the functionality for a local file system
    """
    return os.path.join(PytestSparkSessionUtil().get_spark_warehouse_name(), path)


def get_all_files_in_directory(inst, source_path: str):
    """
    Return all files under the given source_path. Some ETL functionality mimics this using mssparkutils, so this can be used to patch
    the function in the ETL processes
    """
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
    """
    Patch function for SynapseDataIO._format_to_adls_path. This should be used if the spark write mechanism automatically
    writes to`spark-warehouse-xxxxxx`, which is the case for spark tables.

    :return str: A string of the form `container_name/blob_path`
    This will return `container_name/blob_path`
    """
    # This gets written to the 'spark-warehouse-xxxxxx/' folder
    return os.path.join(container_name, blob_path)


def format_adls_path_to_local_path(inst, container_name: str, blob_path: str, storage_name: str = None, storage_endpoint: str = None) -> str:
    """
    Patch function for SynapseDataIO._format_to_adls_path. This should be used if the spark read/write mechanism does not
    automatically write to `spark-warehouse-xxxxxx`. This is the case for file-based read/writes. This can also be used
    to generate a path to a file in the warehouse, incase any pre/post processing needs to be done in the test
    
    :return str: A string of the form `spark-warehouse-xxxxxx/container_name/blob_path`
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
