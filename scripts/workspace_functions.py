"""
Functions to interact with Azure Synapse objects
"""
import os
import json
import asyncio
import timeit
import sys
import glob

# to track coroutine allocation for event loop handling
# an issue when running in Jupyter notebooks only
import tracemalloc
from azure.identity import DefaultAzureCredential
import requests
import aiohttp
import nest_asyncio
import aiofiles
from deepdiff import DeepDiff

tracemalloc.start()

_CREDENTIAL: DefaultAzureCredential = DefaultAzureCredential()
_API_VERSION: str = "2020-12-01"
_SYNAPSE_ENDPOINT: str = "https://pins-synw-odw-dev-uks.dev.azuresynapse.net"
_token: str = _CREDENTIAL.get_token("https://dev.azuresynapse.net/.default").token

_headers: dict = {
    "Authorization": f"Bearer {_token}",
    "Content-Type": "application/json",
}
_timeout: int = 5


def delete_notebook(notebook: str) -> dict | None:
    """
    Deletes a notebook from Synapse Live Mode in the target workspace
    """
    notebook_url: str = (
        f"{_SYNAPSE_ENDPOINT}/notebooks/{notebook}?api-version={_API_VERSION}"
    )
    response: requests.Response = requests.delete(
        notebook_url, headers=_headers, timeout=_timeout
    )

    if response.status_code in [200, 202]:
        return response.json()
    elif response.status_code == 204:
        return None
    else:
        raise Exception(
            f"Failed to delete notebook {notebook}:\n{response.status_code}\n{response.text}"
        )


def get_notebook(notebook: str) -> dict:
    """
    Gets a notebook from Synapse Live Mode in the target workspace
    """
    notebook_url: str = (
        f"{_SYNAPSE_ENDPOINT}/notebooks/{notebook}?api-version={_API_VERSION}"
    )
    response: requests.Response = requests.get(
        notebook_url, headers=_headers, timeout=_timeout
    )
    return response.json()


def create_or_update_notebook(notebook: str) -> dict:
    """
    This currently doesn't work.
    Error: Notebook cells must be defined in the payload.
    Not clear how the payload should be formatted.
    """
    notebook_url: str = (
        f"{_SYNAPSE_ENDPOINT}/notebooks/{notebook}?api-version={_API_VERSION}"
    )
    script_dir: str = os.path.dirname(__file__)
    notebook_path: str = os.path.join(
        script_dir, f"../workspace/notebook/{notebook}.json"
    )
    with open(notebook_path, "r", encoding="utf-8") as file:
        notebook_data = json.load(file)

    request_body: dict = {"name": notebook, "properties": notebook_data}

    response: requests.Response = requests.put(
        notebook_url, headers=_headers, json=request_body, timeout=_timeout
    )
    return response.json()


async def list_json_files(object_type: str) -> list[str]:
    """
    List all JSON file names in the folder corresponding to the object_type asynchronously.
    """
    script_dir: str = os.path.dirname(__file__)
    object_path: str = os.path.join(script_dir, f"../workspace/{object_type}")

    if not os.path.exists(object_path):
        print(f"Error: The folder {object_path} does not exist.")
        return []

    # Use asyncio.to_thread to run the blocking operation in a separate thread
    json_files: list[str] = await asyncio.to_thread(
        glob.glob, os.path.join(object_path, "*.json")
    )

    # Extract file names (just the name, not the full path,
    # and remove the .json extension to accurately compare with the workspace names)
    file_names: list[str] = [
        os.path.splitext(os.path.basename(file))[0] for file in json_files
    ]
    return file_names


async def fetch_objects(
    session: aiohttp.ClientSession, workspace_url: str, object_type: str
) -> list[str]:
    """
    Fetch all objects of a given type with pagination.
    """
    object_list: list = []
    url: str = f"{workspace_url}/{object_type}/?api-version={_API_VERSION}"

    while url:
        async with session.get(url, timeout=_timeout) as response:
            response_json: dict = await response.json()
            object_list.extend(obj["name"] for obj in response_json.get("value", []))
            url: str = response_json.get("nextLink")
    return object_list


async def fetch_object_details(
    session: aiohttp.ClientSession,
    workspace_url: str,
    object_type: str,
    object_name: str,
    keys: list = None,
) -> dict:
    """
    Fetch the full JSON details of an object from the API.
    """
    url = f"{workspace_url}/{object_type}/{object_name}?api-version={_API_VERSION}"
    async with session.get(url) as response:
        if response.status == 200:
            json_data = await response.json()
            if keys:
                filtered_data = {
                    key: json_data[key] for key in keys if key in json_data
                }
                return filtered_data
        else:
            raise Exception(
                f"Failed to fetch {object_type} details for {object_name}:\n{response.status}"
            )


async def load_local_json(file_path: str, keys: list[str]) -> dict:
    """
    Load a local json file asynchronously and return its contents as a dictionary
    """
    async with aiofiles.open(file_path, mode="r", encoding="utf-8") as file:
        content = await file.read()
        json_data = json.loads(content)
        if keys:
            filtered_data = {key: json_data[key] for key in keys if key in json_data}
            return filtered_data


async def compare_json_objects(
    session: aiohttp.ClientSession,
    workspace_url: str,
    object_type: str,
    object_names: list,
    local_dir: str,
) -> dict:
    """
    Compare the JSON content of fetched objects from the API with local JSON files.
    """
    mismatched_objects = []
    missing_local_files = []
    api_fetch_errors = []

    for object_name in object_names:
        # Fetch the object JSON from the API
        try:
            api_json = await fetch_object_details(
                session,
                workspace_url,
                object_type,
                object_name,
                keys=["name", "properties"],
            )
        except Exception as e:
            api_fetch_errors.append((object_name, str(e)))
            continue

        # Construct the local file path
        local_file_path = os.path.join(local_dir, f"{object_name}.json")

        # Check if the local file exists
        if not os.path.exists(local_file_path):
            missing_local_files.append(object_name)
            continue

        # Load the local JSON asynchronously
        local_json = await load_local_json(
            file_path=local_file_path, keys=["name", "properties"]
        )

        # Compare the JSON objects
        if api_json != local_json:
            mismatched_objects.append(object_name)

        differences = DeepDiff(api_json, local_json, ignore_order=True)

    return {
        "mismatched_objects": mismatched_objects,
        "missing_local_files": missing_local_files,
        "api_fetch_errors": api_fetch_errors,
        "Differences": differences,
    }


def compare_lists(fetched_objects: list, json_files: list) -> tuple:
    """
    Compare two lists (fetched_objects and json_files) and return the missing items from each list.
    """

    set_objects: set = set(fetched_objects)
    set_json_files: set = set(json_files)

    # Find items in fetched_objects but not in json_files
    missing_from_json_files: set = set_objects - set_json_files
    # Find items in json_files but not in fetched_objects
    missing_from_fetched_objects: set = set_json_files - set_objects

    return missing_from_json_files, missing_from_fetched_objects


async def get_modified_objects():
    """
    Main function to compare JSON objects fetched from the API with local JSON files.
    """
    object_type = "notebooks"
    object_names = ["py_versions"]
    local_dir = "../workspace/notebook"

    async with aiohttp.ClientSession(headers=_headers) as session:
        comparison_results = await compare_json_objects(
            session, _SYNAPSE_ENDPOINT, object_type, object_names, local_dir
        )

        # Print results
        print("Comparison Results:")
        print("Mismatched JSON objects:", comparison_results.get("mismatched_objects"))
        print("Missing local files:", comparison_results.get("missing_local_files"))
        print("Fetch errors:", comparison_results.get("api_fetch_errors"))
        print("Differences:", comparison_results.get("Differences"))


async def main() -> None:
    """
    Main function to list all objects concurrently using asyncio.
    """
    object_types: list[str] = [
        "notebooks",
        "datasets",
        "linkedservices",
        "pipelines",
        "sqlScripts",
        "triggers",
    ]

    object_data: dict = {}

    async with aiohttp.ClientSession(headers=_headers) as session:
        tasks: list = [
            fetch_objects(session, _SYNAPSE_ENDPOINT, object_type)
            for object_type in object_types
        ]

        # remove the last letter 's' for the folder names
        json_folders: list = [folder[:-1] for folder in object_types]

        tasks += [list_json_files(folder) for folder in json_folders]

        results: list = await asyncio.gather(*tasks, return_exceptions=True)

        fetched_objects: list = results[
            : len(object_types)
        ]  # First set of results: fetched objects
        json_files: list = results[
            len(object_types) :
        ]  # Second set of results: JSON files

        for idx, object_type in enumerate(object_types):
            object_data[object_type] = {
                "fetched_objects": fetched_objects[idx],
                "json_files": json_files[idx],
            }

        for object_type in object_types:
            fetched = object_data[object_type]["fetched_objects"]
            files = object_data[object_type]["json_files"]
            missing_from_json, missing_from_objects = compare_lists(fetched, files)

            print(f"Comparing {object_type}:")

            if missing_from_json:
                print(f"In workspace but not in main branch: {missing_from_json}")
            else:
                print("No items missing from main branch.")

            if missing_from_objects:
                print(f"In main branch but not in workspace: {missing_from_objects}")
            else:
                print("No items missing from workspace.")

            print()


def run_main():
    """
    Function to call the async main function.
    Used for timeit to get the execution time.
    This is not possible with async functions.
    """
    asyncio.run(main())


if __name__ == "__main__":
    if sys.stdout.isatty():
        # For standard script execution, e.g. in terminal or DevOps pipeline
        NUMBER_OF_EXECUTIONS: int = 1
        total_time: float = timeit.timeit(run_main, number=NUMBER_OF_EXECUTIONS)
        average_time: float = total_time / NUMBER_OF_EXECUTIONS
        print(f"Average execution time: {average_time:.4f} seconds")
    else:
        try:
            asyncio.run(main())
        # Handle existing event loop in Jupyter
        except RuntimeError:
            nest_asyncio.apply()
            loop = asyncio.get_event_loop()
            loop.run_until_complete(main())
