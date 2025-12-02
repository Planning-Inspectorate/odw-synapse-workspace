import os
import json
import re

def extract_notebooks_from_pipeline(pipeline_file):
    """
    Extracts all notebook references from a pipeline JSON file.
    """
    notebooks = []

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

        # Check for nested activities (e.g., in ForEach or IfCondition activities)
        for key in ["activities", "ifTrueActivities", "ifFalseActivities"]:
            nested_activities = activity.get("typeProperties", {}).get(key, [])
            for nested_activity in nested_activities:
                if nested_activity.get("type") == "SynapseNotebook":
                    notebook_reference = nested_activity.get("typeProperties", {}).get("notebook", {}).get("referenceName")
                    if notebook_reference:
                        notebooks.append(notebook_reference)

    return notebooks

def get_notebooks_from_pipelines(pipelines_to_archive, pipeline_dir):
    """
    Retrieves all notebooks referenced in the specified pipelines.
    """
    all_notebooks = []

    for pipeline_name in pipelines_to_archive:
        pipeline_file = os.path.join(pipeline_dir, f"{pipeline_name}.json")
        if os.path.exists(pipeline_file):
            notebooks = extract_notebooks_from_pipeline(pipeline_file)
            all_notebooks.extend(notebooks)
        else:
            print(f"Warning: Pipeline file '{pipeline_file}' does not exist.")

    return all_notebooks

def extract_all_strings_from_json(data):
    strings = []
    if isinstance(data, dict):
        for value in data.values():
            strings.extend(extract_all_strings_from_json(value))
    elif isinstance(data, list):
        for item in data:
            strings.extend(extract_all_strings_from_json(item))
    elif isinstance(data, str):
        strings.append(data)
    return strings

def extract_referenced_notebooks(notebook_path, notebook_dir, processed_files=None):
    """
    Extracts all notebooks referenced in a given notebook by looking for '%run' and 'mssparkutils.notebook.run' strings.
    Recursively processes referenced notebooks to extract further references.
    """
    if processed_files is None:
        processed_files = set()

    # Avoids re-processing the same notebook
    if notebook_path in processed_files:
        return []

    processed_files.add(notebook_path)
    referenced_notebooks = []

    try:
        with open(notebook_path, 'r', encoding='utf-8') as f:
            try:
                content = json.load(f)
            except json.JSONDecodeError:
                print(f"Could not parse JSON from: {notebook_path}")
                return []

        # Find all occurrences of "%run" and "mssparkutils.notebook.run(" & extract the paths
        all_strings = extract_all_strings_from_json(content)
        match_run = [re.findall(r'%run\s+([^\s]+)', s) for s in all_strings]
        match_func = [re.findall(r"mssparkutils\.notebook\.run\(['\"]([^'\"]+)['\"]", s) for s in all_strings]
        all_matches = sum(match_run, []) + sum(match_func, [])
        if all_matches:
            for match in all_matches:
                # Extract the string from the end until the first "/"
                notebook_name = match.rsplit('/', 1)[-1]
                # Clean up the notebook name (remove unwanted characters like quotes)
                cleaned_name = notebook_name.replace('\\', '').replace('"', '').replace("'", '')
                normalized_path = os.path.normpath(os.path.join(notebook_dir, cleaned_name))

                if not normalized_path.endswith('.json'):
                    normalized_path += '.json'

                if os.path.exists(normalized_path):
                    referenced_notebooks.append(normalized_path)
                else:
                    print(f"Warning: Referenced notebook not found: {normalized_path}")

        # Recursively loop through each referenced notebook
        for notebook in referenced_notebooks:
            referenced_notebooks.extend(
                extract_referenced_notebooks(notebook, notebook_dir, processed_files)
            )

    except FileNotFoundError as e:
        print(f"Error processing notebook {notebook_path}: {e}")
    except Exception as e:
        print(f"Unexpected error processing notebook {notebook_path}: {e}")

    return referenced_notebooks


def recursively_extract_all_referenced_notebooks(notebooks, notebook_dir):
    """
    Recursively extracts all referenced notebooks from a list of notebooks.
    """
    all_referenced_notebooks = set()
    for notebook in notebooks:
        if not notebook.endswith('.json'):
            notebook += '.json'

        notebook_path = os.path.join(notebook_dir, notebook)

        # Extract referenced notebooks
        referenced_notebooks = extract_referenced_notebooks(notebook_path, notebook_dir)
        all_referenced_notebooks.update(referenced_notebooks)
    return all_referenced_notebooks

def extract_notebook_names_from_directory(notebook_dir):
    """
    Extracts all notebook names from the specified directory.
    """
    notebook_names = []
    for root, _, files in os.walk(notebook_dir):
        for file in files:
            if file.endswith('.json'):  # Only include JSON files
                # Remove the "workspace/notebook/" prefix and add the notebook name
                relative_path = os.path.relpath(os.path.join(root, file), notebook_dir)
                notebook_names.append(relative_path)
    return notebook_names

def archive_notebook(notebook_name, notebook_dir):
    """
    Archives a notebook by prepending "archive/" to its folder field in the JSON file.
    """
    notebook_file = os.path.join(notebook_dir, notebook_name)
    if not os.path.exists(notebook_file):
        print(f"Error: Notebook file '{notebook_file}' does not exist.")
        return False

    with open(notebook_file, 'r', encoding='utf-8') as f:
        file_content = f.read()

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
        with open(notebook_file, 'w', encoding='utf-8') as f:
            f.write(updated_content)

        return True
    elif "name" in folder and folder["name"].startswith("archive"):
        print(f"Notebook '{notebook_name}' is already archived.")
    else:
        print(f"Notebook '{notebook_name}' does not have a valid folder field.")

    return False


def main():
    pipeline_dir = "workspace/pipeline"
    #List of pipelines that are linked to pln_master (note: if doesn't exist, run archive_pipelines_not_used_by_master_pln_and_not_run.py script)
    dependencies_file = "scripts/pipeline_dependencies.txt"
    notebook_dir = "workspace/notebook"

    # List of pipelines that are not run, which is retrieved from hierarchy.txt file/output of buildHierarchyOfPipelines.py script
    not_run_pipelines = {
        "pln_temp_eployee_syn_service_to_raw",
        "pln_load_harmonised_to_curated",
        "pln_load_standardised_to_harmonised",
        "0_Horizon_SQL_Tables_Raw_part1",
        "0_Timesheets_Data_Copy_RAW",
        "0_Legacy_Timesheet_Data_Copy_RAW",
        "0_Horizon_SQL_Tables_Raw_part2",
        "0_Raw_Case_Reference_Tables",
        "pl_copy_sap_load_tables_to_raw_storage",
        "pln_copy_mipins_TEST",
        "0_Raw_Checkmark_Data_part2",
        "pln_copy_mipins",
        "0_pln_source_to_raw_fileshare_copy_activity",
        "CopyPipeline_ahn",
        "0_pln_source_to_raw_MiPINS",
        "0_Raw_Checkmark_Data_part1",
        "pln_mipins_raw_to_curated_new_1",
        "pln_temp_employee_syn_raw_to_curated",
        "pln_load_employee_standardised_to_harmonised",
        "pln_load_raw_to_standardised",
        "pln_mipins_raw_to_curated_new_2",
        "pln_migration_test",
        "pln_mipins_raw_to_curated",
        "pln_odw_master",
        "0_ZenDesk_Data_Transfer",
        "0_ODT_Data_Transfer",
        "pln_raw_to_standardised_e2e",
        "0_Raw_Horizon_Main",
        "pln_hr_cube_objects",
        "pln_hr_measures_schedule",
        "pln_hr_ingestion_initial",
        "pln_hr_deployment_clean_slate",
        "0_Raw_High_Court_Data_Copy",
        "pln_hr_ingestion_harmonised_and_measures",
        "pln_document_metadata_clean_slate",
        "pln_casework_harmonised_deployment",
        "pln_fact_sickness",
        "pln_nisp_relevant_reps_clean_slate",
        "pln_horizon_folder_clean_slate",
        "pln_nsip_project_clean_slate",
        "pln_nsip_s51_advice_clean_slate",
        "pln_nsip_exam_timetable_clean_slate",
        "pln_service_user_clean_slate",
        "data_validation_test",
        "pln_service_bus_nsip_s51_advice",
        "pln_service_bus_nsip_exam_timetable",
        "0_Raw_IMS",
        "pln_service_bus_service_user",
        "pln_ims_main",
        "pln_service_bus_nsip_project",
        "0_Raw_Horizon_BIS_Event",
        "0_Zendesk_API_to_RAW",
        "0_Zendesk_API_to_RAW_historical_load",
        "pln_high_court_main",
        "pln_initial_high_court",
        "pln_nsip_relevant_reps_main",
        "pln_nsip_exam_timetable_main",
        "pln_nsip_s51_advice_main",
        "pln_zendesk_main",
        "pln_nsip_project_main",
        "pln_service_user_main",
        "0_Horizon_Appeals_Data_Transfer_Raw",
        "pln_casework_source_to_raw_legacy",
        "pln_casework_source_to_raw",
        "0_Horizon_Document_Folder",
        "pln_document_metadata_main",
        "pln_casework_main",
        "pln_casework_deployment_clean_slate",
        "pln_nsip_subscription_clean_slate",
        "pln_nsip_project_update_clean_slate",
        "pln_service_bus_nsip_subscription",
        "pln_service_bus_folder",
        "pln_service_bus_nsip_document",
        "pln_application_run_stage_service_bus",
        "pln_service_bus_nsip_representation",
        "pln_service_bus_nsip_project_update",
        "pln_application_run_stage_Horizon",
        "pln_folder_main",
        "pln_applications_master",
        "pln_create_horizon_harmonised_tables",
        "pln_1335_release",
        "delete_selected_tables",
        "nsip_representation_migration",
        "pln_curated_load",
        "pln_main_source_system_fact",
        "create_selected_tables",
        "pln_sb_reload",
        "rel_1347_nsip_representation",
        "rel_1047_migration_db",
        "rel_1273_s51",
        "rel_1262_entra_id",
        "pln_odw_1333",
        "rel_1269_document metadata",
        "rel_1309_nsip_exam",
        "rel_1416_master_fixes",
        "service_user",
        "rel_971_logging_monitoring",
        "rel_has_156",
        "rel_1349_appeal_document",
        "rel_1381_appeal_has",
        "pln_service_bus_appeals_document",
        "pln_service_bus_appeals_has",
        "pln_service_bus_appeals_event",
        "0_Raw_Horizon_Appeals_Document_Metadata_copy1",
        "0_Raw_Horizon_Appeals_Event",
        "Appeals_Event_Clean_Slate",
        "rel_1272_nsip_data",
        "rel_1374_aie",
        "rel_1151_appeals_events",
        "rel_1298_relevant_representation",
        "rel_1403_entraid",
        "pln_appeals_document_main",
        "rel_2_0_0",
        "pln_rel_1_1_13",
        "rel_2_0_3",
        "rel_2_0_4",
        "rel_2_0_5",
        "rel_2_0_7",
        "rel_2_0_8",
        "rel_2_0_6",
        "rel_THEODW-992-WelshFields",
        "rel_2_0_9",
        "rel_2_0_11_nsip_reps_migrated",
        "rel_3_0_0",
        "rel_3_0_4",
        "reload_sb_harmonised_tables",
        "rel_3_0_3",
        "rel_4_0_0",
        "input_file_backfill",
        "rel_4_0_1",
        "rel_6_0_1",
        "0_Raw_Horizon_DaRT_Inspectors",
        "0_Raw_Horizon_DaRT_LPA",
        "Horizon-Inspectors",
        "Horizon-LPA",
        "pln_listed_building_initial_export",
        "DaRT-master",
        "pln_workspace_rebuild",
        "rel_6_0_3_s78",
        "rel_7_0_0",
        "rel_6_0_2",
        "rel_7_0_1",
        "pln_SAPHR_SharepointData",
        "rel_8_0_4_hotfix",
        "pln_publish_random_messages_to_sb"
    }

    # Ensure the pipeline directory and dependencies file exist
    if not os.path.exists(pipeline_dir):
        print(f"Error: Pipeline directory '{pipeline_dir}' does not exist.")
        return

    if not os.path.exists(dependencies_file):
        print(f"Error: Dependencies file '{dependencies_file}' does not exist.")
        return

    # Get the list of used pipelines by master pipeline
    with open(dependencies_file, 'r') as f:
        used_pipelines = set(line.strip() for line in f if line.strip())

    all_pipelines = set(
        os.path.splitext(file)[0]
        for file in os.listdir(pipeline_dir)
        if file.endswith('.json')
    )

    pipelines_that_are_run = all_pipelines - not_run_pipelines
    # print(f"Pipelines that run: {pipelines_that_are_run}")

    # Get pipelines that are both used and have ran in other pipelines, ensuring no duplicates
    pipelines_needed = (used_pipelines).union(pipelines_that_are_run)
    # print(f"Pipelines we need: {pipelines_needed}")

    # Get all notebooks referenced in the pipelines
    notebooks = get_notebooks_from_pipelines(pipelines_needed, pipeline_dir)
    notebooks = [notebook if notebook.endswith('.json') else f"{notebook}.json" for notebook in notebooks]
    # if notebooks:
    #     print("Notebooks referenced in the pipelines:")
    #     for notebook in notebooks:
    #         print(f"- {notebook}")
    # else:
    #     print("No notebooks found in the specified pipelines.")

    referenced_notebooks = recursively_extract_all_referenced_notebooks(notebooks, notebook_dir)
    referenced_notebooks = [notebook.replace("workspace/notebook/", "") for notebook in referenced_notebooks]

    final_list_of_needed_notebooks = list(dict.fromkeys(notebooks + list(referenced_notebooks)))
    # print("All referenced notebooks:")
    # for notebook in sorted(final_list_of_needed_notebooks):
    #     print(f"- {notebook}")

    # Extracts all notebook names from the directory.
    total_notebooks = extract_notebook_names_from_directory(notebook_dir)
    # print("All notebooks in the workspace/notebook directory:")
    # for notebook in sorted(total_notebooks):
    #     print(f"- {notebook}")

    not_needed_notebooks = list(set(total_notebooks) - set(final_list_of_needed_notebooks))
    # print("Notebooks that are not needed:")
    # for notebook in sorted(not_needed_notebooks):
    #     print(f"- {notebook}")

    # Remove any notebooks that are in the utils folder from the not_needed_notebooks,
    # as there were notebooks (from final_list_of_needed_notebooks) that used the entire folder
    filtered_not_needed_notebooks = []

    for notebook in not_needed_notebooks:
        notebook_path = os.path.join(notebook_dir, notebook)
        try:
            # Open and parse the notebook JSON file
            with open(notebook_path, 'r', encoding='utf-8') as f:
                notebook_data = json.load(f)

            # Check if the folder name starts with 'utils'
            folder_name = notebook_data.get("properties", {}).get("folder", {}).get("name", "")
            if not folder_name.startswith("utils"):
                filtered_not_needed_notebooks.append(notebook)
        except Exception as e:
            print(f"Error processing notebook {notebook}: {e}")

    not_needed_notebooks = filtered_not_needed_notebooks

    print("Filtered list of notebooks that are not needed:")
    for notebook in sorted(not_needed_notebooks):
        print(f"- {notebook}")

    # Archive not needed notebooks
    if not_needed_notebooks:
        print("\nArchiving notebooks...")
        for notebook in sorted(not_needed_notebooks):
            archived = archive_notebook(notebook, notebook_dir)
            if archived:
                print(f"- Archived: {notebook}")
            else:
                print(f"- Failed to archive: {notebook}")
    else:
        print("No notebooks to archive.")

if __name__ == "__main__":
    main()
