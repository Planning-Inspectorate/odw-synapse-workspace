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

def check_if_in_archive(pipeline_name, pipeline_dir):
    """
    Checks if a pipeline JSON file has "archive" in its folder field.
    """
    pipeline_file = os.path.join(pipeline_dir, f"{pipeline_name}.json")
    if not os.path.exists(pipeline_file):
        return False

    # Read the JSON file and check the folder field
    with open(pipeline_file, 'r') as f:
        data = json.load(f)
        folder = data.get("properties", {}).get("folder", {}).get("name", "")
        return "archive" in folder

def archive_pipeline(pipeline_name, pipeline_dir):
    """
    Archives a pipeline by prepending "archive/" to its folder field in the JSON file.
    """
    pipeline_file = os.path.join(pipeline_dir, f"{pipeline_name}.json")
    if not os.path.exists(pipeline_file):
        print(f"Error: Pipeline file '{pipeline_file}' does not exist.")
        return False

    # Read the JSON file
    with open(pipeline_file, 'r') as f:
        file_content = f.read()
    
    data = json.loads(file_content)

    # Update the folder field to include "archive/"
    folder = data.get("properties", {}).get("folder", {})
    if "name" in folder and not folder["name"].startswith("archive/"):
        folder["name"] = f"archive/{folder['name']}"
        print(f"Archiving pipeline: {pipeline_name}")

        updated_content = file_content.replace(
            f'"name": "{folder["name"].replace("archive/", "")}"',
            f'"name": "{folder["name"]}"'
        )
        
        # Write the updated content back to the file
        with open(pipeline_file, 'w') as f:
            f.write(updated_content)
        return True

    return False

def main(): 
    # Extract pipeline dependencies from the master pipeline & write to file
    pipeline_dir = "workspace/pipeline" 
    master_pipeline_file = os.path.join(pipeline_dir, "pln_master.json")
    all_pipelines = extract_pipelines(master_pipeline_file, pipeline_dir)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_file = os.path.join(script_dir, "pipeline_dependencies.txt")

    with open(output_file, 'w') as f:
        for pipeline in sorted(set(all_pipelines)):
            f.write(f"{pipeline}\n")

    print(f"Pipeline dependencies written to {output_file}")

    # Check if pipeline directory and dependencies file exist
    if not os.path.exists(pipeline_dir):
        print(f"Error: Pipeline directory '{pipeline_dir}' does not exist.")
        return

    if not os.path.exists(output_file):
        print(f"Error: Dependencies file '{output_file}' does not exist.")
        return

    # Get the list of unused pipelines by master pipeline
    unused_pipelines = list_unused_pipelines(pipeline_dir, output_file)

    # Check which unused pipelines are in the archive folder
    archived_unused_pipelines = [
        pipeline for pipeline in unused_pipelines
        if check_if_in_archive(pipeline, pipeline_dir)
    ]

    pipelines_left_to_archive = unused_pipelines - set(archived_unused_pipelines)

    print(f"Total unused pipelines: {len(unused_pipelines)}")
    print(f"Archived unused pipelines: {len(archived_unused_pipelines)}")
    print(f"Pipelines left to be archived: {len(pipelines_left_to_archive)}")

    if pipelines_left_to_archive:
        print("\nArchiving pipelines left to be archived...")
        for pipeline in sorted(pipelines_left_to_archive):
            archived = archive_pipeline(pipeline, pipeline_dir)
            if archived:
                print(f"- Archived: {pipeline}")
            else:
                print(f"- Failed to archive: {pipeline}")


if __name__ == "__main__":
    main()
