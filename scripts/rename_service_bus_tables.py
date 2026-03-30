from azure.storage.blob import BlobServiceClient
from azure.identity import AzureCliCredential
from concurrent.futures import ThreadPoolExecutor, as_completed
import json


"""
Single-use utility script to rename service bus files that have ':' symbols in their names by replacing ':' with '_'
You must set the STORAGE_ACCOUNT_NAME manually before running. The script includes validation checks to protect against data loss
Example usage: python3 scripts/rename_service_bus_tables.py
"""

STORAGE_ACCOUNT_NAME = ""
if not STORAGE_ACCOUNT_NAME:
    raise ValueError("The storage account name needs to be set")


def rename_blob(blob_name: str):
    return blob_name.replace(":", "_")


blob_service_client = BlobServiceClient(f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net", credential=AzureCliCredential())
container_client = blob_service_client.get_container_client(container="odw-raw")

service_bus_base_path = "ServiceBus"
print("Gathering files with ':' in their path")
blobs_to_rename = [x.name for x in container_client.list_blobs(name_starts_with=service_bus_base_path) if x.name.endswith("json")]
rename_map = {
    x.name: rename_blob(x.name)
    for x in container_client.list_blobs(name_starts_with=service_bus_base_path)
    if x.name.endswith("json") and ":" in x.name
}

print("Copying files with ':' in their path with ':' replaced with '_'")


def copy_data(original_file_name: str, new_file_name: str):
    new_blob_client = container_client.get_blob_client(new_file_name)
    if new_blob_client.exists():
        raise RuntimeError(f"The blob {original_file_name} has already been renamed to {new_file_name} - please check this file")
    original_blob_client = container_client.get_blob_client(original_file_name)
    new_blob_client.start_copy_from_url(original_blob_client.url)
    original_blob_client.close()
    new_blob_client.close()


with ThreadPoolExecutor() as tpe:
    futures_map = {tpe.submit(copy_data, k, v): (k, v) for k, v in rename_map.items()}
    failed_files = []
    for future in as_completed(futures_map):
        k, v = futures_map[future]
        try:
            result = future.result()
        except Exception:
            failed_files.append(k)

if failed_files:
    raise ValueError(f"Failed to rename the following files: {json.dumps(failed_files, indent=4)}")


print("Validating the copy was successful before continuing")


# Validate migration is complete
def validate(file_name: str):
    with container_client.get_blob_client(file_name) as new_blob_client:
        assert new_blob_client.exists()


with ThreadPoolExecutor() as tpe:
    futures_map = {
        tpe.submit(
            validate,
            v,
        ): v
        for v in rename_map.values()
    }
    validation_errors = []
    for future in as_completed(futures_map):
        v = futures_map[future]
        try:
            result = future.result()
        except Exception:
            validation_errors.append(v)

if validation_errors:
    raise ValueError(f"Validation failed for some files: {json.dumps(validation_errors, indent=4)}")

print("Deleting old data")


# Clear old data
def delete_data(original_file_name: str):
    with container_client.get_blob_client(original_file_name) as new_blob_client:
        new_blob_client.delete_blob()


with ThreadPoolExecutor() as tpe:
    tpe.map(delete_data, rename_map.keys())

print("Validating that all files were deleted")


def validate_deleted(file_name: str):
    with container_client.get_blob_client(file_name) as new_blob_client:
        assert not new_blob_client.exists()


with ThreadPoolExecutor() as tpe:
    futures_map = {
        tpe.submit(
            validate_deleted,
            v,
        ): v
        for v in rename_map.keys()
    }
    validation_errors = []
    for future in as_completed(futures_map):
        v = futures_map[future]
        try:
            result = future.result()
        except Exception:
            validation_errors.append(v)
if validation_errors:
    raise ValueError(f"Failed to delete the following old files: {json.dumps(validation_errors, indent=4)}")
print("Successfully renamed all fules")
