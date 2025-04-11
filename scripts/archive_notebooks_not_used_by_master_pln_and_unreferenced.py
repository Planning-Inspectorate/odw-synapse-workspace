import os
import json

def extract_pipelines(file_path, pipeline_dir, visited=None):
    """
    Recursively extracts pipeline names from a JSON file that are linked to pln_master.
    """
    if visited is None:
        visited = set()

    pipelines = []

    with open(file_path, 'r') as f:
        data = json.load(f)

    activities = data.get("properties", {}).get("activities", [])
    for activity in activities:
        pipelines.extend(process_activity(activity, pipeline_dir, visited))

    return pipelines

def process_activity(activity, pipeline_dir, visited):
    """
    Processes an activity to extract pipeline names recursively.
    """
    pipelines = []

    type_properties = activity.get("typeProperties", {})
    pipeline_reference = type_properties.get("pipeline", {}).get("referenceName")
    if pipeline_reference and pipeline_reference not in visited:
        pipelines.append(pipeline_reference)
        visited.add(pipeline_reference)

        pipeline_file = os.path.join(pipeline_dir, f"{pipeline_reference}.json")
        if os.path.exists(pipeline_file):
            pipelines.extend(extract_pipelines(pipeline_file, pipeline_dir, visited))
        else:
            print(f"Warning: File not found for pipeline {pipeline_reference} at {pipeline_file}")

    for key in ["ifFalseActivities", "ifTrueActivities", "activities"]:
        nested_activities = type_properties.get(key, [])
        for nested_activity in nested_activities:
            pipelines.extend(process_activity(nested_activity, pipeline_dir, visited))

    return pipelines

def list_unused_pipelines(pipeline_dir, dependencies_file):
    """
    Lists pipelines that are not referenced in the dependencies file.
    """
    with open(dependencies_file, 'r') as f:
        dependencies = set(line.strip() for line in f if line.strip())

    all_pipelines = set(
        # Remove the .json extension
        os.path.splitext(file)[0]
        for file in os.listdir(pipeline_dir)
        if file.endswith('.json')
    )

    unused_pipelines = all_pipelines - dependencies

    return unused_pipelines

def extract_notebooks_from_pipeline(pipeline_file):
    """
    Extracts notebook references from a pipeline JSON file.
    """
    notebooks = []

    # Read the pipeline JSON file
    with open(pipeline_file, 'r') as f:
        data = json.load(f)

    # Iterate through activities in the pipeline
    activities = data.get("properties", {}).get("activities", [])
    for activity in activities:
        if activity.get("type") == "SynapseNotebook":
            # Extract the notebook reference name
            notebook_reference = activity.get("typeProperties", {}).get("notebook", {}).get("referenceName")
            if notebook_reference:
                notebooks.append(notebook_reference)

    return notebooks

def archive_notebook(notebook_name, notebook_dir):
    """
    Archives a notebook by prepending "archive/" to its folder field in the JSON file.
    """
    notebook_file = os.path.join(notebook_dir, f"{notebook_name}.json")
    if not os.path.exists(notebook_file):
        print(f"Error: Notebook file '{notebook_file}' does not exist.")
        return False

    # Read the notebook JSON file as a string
    with open(notebook_file, 'r') as f:
        file_content = f.read()

    # Parse the JSON data to locate the folder field
    data = json.loads(file_content)
    folder = data.get("properties", {}).get("folder", {})
    if "name" in folder and not folder["name"].startswith("archive"):
        old_folder_name = folder["name"]
        new_folder_name = f"archive/{old_folder_name}"

        # Replace only the folder name in the original file content
        updated_content = file_content.replace(
            f'"name": "{old_folder_name}"',
            f'"name": "{new_folder_name}"'
        )

        # Write the updated content back to the file
        with open(notebook_file, 'w') as f:
            f.write(updated_content)

        print(f"Archived notebook: {notebook_name}")
        return True
    elif "name" in folder and folder["name"].startswith("archive"):
        print(f"Notebook '{notebook_name}' is already archived.")
    else:
        print(f"Notebook '{notebook_name}' does not have a valid folder field.")
    return False

def main(): 
    # Extract pipeline dependencies from the master pipeline & write to file
    pipeline_dir = "workspace/pipeline" 
    master_pipeline_file = os.path.join(pipeline_dir, "pln_master.json")
    notebook_dir = "workspace/notebook"
    all_pipelines = extract_pipelines(master_pipeline_file, pipeline_dir)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_file = os.path.join(script_dir, "pipeline_dependencies.txt")

    with open(output_file, 'w') as f:
        for pipeline in sorted(set(all_pipelines)):
            f.write(f"{pipeline}\n")

    print(f"Pipeline dependencies written to {output_file}")

    # Put list of pipelines that are not run, which is retrieved from hierarchy.txt file/output of buildHierarchyOfPipelines.py script
    not_run_pipelines = {
        "pln_1",
        "pln_2",
    }

    # Put list of unreferenced notebooks, which is retrieved from hierarchy.txt file/output of buildHierarchyOfPipelines.py script
    unreferenced_notebooks = {
        "notebook_1",
        "notebook_2"
    }
   
    # Check if pipeline directory and dependencies file exist
    if not os.path.exists(pipeline_dir):
        print(f"Error: Pipeline directory '{pipeline_dir}' does not exist.")
        return

    if not os.path.exists(output_file):
        print(f"Error: Dependencies file '{output_file}' does not exist.")
        return

    # Get the list of unused pipelines by master pipeline
    unused_pipelines = list_unused_pipelines(pipeline_dir, output_file)

    # Extract notebooks from unused pipelines that are also in the not_run_pipelines list
    notebooks_to_archive = []
    for pipeline_name in unused_pipelines:
        if pipeline_name in not_run_pipelines:
            pipeline_file = os.path.join(pipeline_dir, f"{pipeline_name}.json")
            if os.path.exists(pipeline_file):
                notebooks_to_archive.extend(extract_notebooks_from_pipeline(pipeline_file))

    # Add unreferenced notebooks to the list of notebooks to archive
    notebooks_to_archive.extend(unreferenced_notebooks)

    print(f"Total notebooks to archive: {len(notebooks_to_archive)}")

    # Archive the notebooks
    for notebook_name in notebooks_to_archive:
        archive_notebook(notebook_name, notebook_dir)

if __name__ == "__main__":
    main()
