import os
import json

def extract_used_pipelines(file_path, pipeline_dir, visited=None):
    if visited is None:
        visited = set()
    pipelines = set()
    with open(file_path, 'r') as f:
        data = json.load(f)
    activities = data.get("properties", {}).get("activities", [])
    for activity in activities:
        pipelines.update(process_pipeline_references(activity, pipeline_dir, visited))
    return pipelines

def process_pipeline_references(activity, pipeline_dir, visited):
    pipelines = set()
    type_props = activity.get("typeProperties", {})
    ref = type_props.get("pipeline", {}).get("referenceName")
    if ref and ref not in visited:
        pipelines.add(ref)
        visited.add(ref)
        nested_file = os.path.join(pipeline_dir, f"{ref}.json")
        if os.path.exists(nested_file):
            pipelines.update(extract_used_pipelines(nested_file, pipeline_dir, visited))
    for key in ["ifFalseActivities", "ifTrueActivities", "activities"]:
        for nested in type_props.get(key, []):
            pipelines.update(process_pipeline_references(nested, pipeline_dir, visited))
    return pipelines

def get_all_pipelines(pipeline_dir):
    return set(
        os.path.splitext(file)[0]
        for file in os.listdir(pipeline_dir)
        if file.endswith('.json')
    )

def extract_datasets_from_activity(activity):
    datasets = set()
    for key in ["inputs", "outputs"]:
        for ds in activity.get(key, []):
            ref = ds.get("referenceName")
            if ref:
                datasets.add(ref)
    type_props = activity.get("typeProperties", {})
    for key in ["dataset", "inputDataset", "outputDataset"]:
        ref = type_props.get(key, {}).get("referenceName")
        if ref:
            datasets.add(ref)
    if "activities" in type_props:
        for sub in type_props["activities"]:
            datasets.update(extract_datasets_from_activity(sub))
    return datasets

def extract_datasets_from_pipeline_file(pipeline_file):
    with open(pipeline_file, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            print(f" Skipping invalid JSON: {pipeline_file}")
            return set()
    datasets = set()
    activities = data.get("properties", {}).get("activities", [])
    for activity in activities:
        datasets.update(extract_datasets_from_activity(activity))
    return datasets

def get_datasets_from_unused_pipelines(pipeline_dir, unused_pipelines):
    all_datasets = set()
    for pipeline_name in unused_pipelines:
        file_path = os.path.join(pipeline_dir, f"{pipeline_name}.json")
        if os.path.exists(file_path):
            datasets = extract_datasets_from_pipeline_file(file_path)
            all_datasets.update(datasets)
    return all_datasets

def update_dataset_paths_in_place(unused_datasets, dataset_dir, dataset_to_pipelines, unused_pipelines):
    print(f"\n✏️ Updating dataset JSONs to use folder 'Archive/Unused' under Synapse workspace...\n")

    for dataset in unused_datasets:
        file_path = os.path.join(dataset_dir, f"{dataset}.json")
        if not os.path.exists(file_path):
            print(f" File not found: {file_path}")
            continue

        pipelines = dataset_to_pipelines.get(dataset, set())
        is_used_in_active_pipeline = any(p not in unused_pipelines for p in pipelines)
        should_archive = not is_used_in_active_pipeline

        with open(file_path, 'r', encoding='utf-8') as f:
            try:
                content = json.load(f)
            except json.JSONDecodeError:
                print(f"Invalid JSON in: {file_path}")
                continue

        changed = False
        folder_section = content.setdefault("properties", {}).setdefault("folder", {})

        if should_archive:
            if folder_section.get("name") != "Archive/Unused":
                folder_section["name"] = "Archive/Unused"
                changed = True
        else:
            if folder_section.get("name") == "Archive/Unused":
                del folder_section["name"]
                changed = True

        location = content["properties"].get("typeProperties", {}).get("location", {})
        folder_path = location.get("folderPath")
        if isinstance(folder_path, dict) and "value" in folder_path:
            original_expr = folder_path["value"]
            is_archived = original_expr.strip().startswith("@concat('archive/")
            if should_archive and not is_archived:
                folder_path["value"] = f"@concat('archive/', {original_expr})"
                changed = True
            elif not should_archive and is_archived:
                inner_expr = original_expr.strip()[len("@concat('archive/', "):-1]
                folder_path["value"] = inner_expr
                changed = True

        if changed:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(content, f, indent="\t", ensure_ascii=False)
            print(f"{' Archived' if should_archive else ' Unarchived'}: {dataset}.json")

def main():
    pipeline_dir = "workspace/pipeline"
    dataset_dir = "workspace/dataset"
    master_pipeline_file = os.path.join(pipeline_dir, "pln_master.json")

    if not os.path.exists(master_pipeline_file):
        print(" Master pipeline not found.")
        return

    used_pipelines = extract_used_pipelines(master_pipeline_file, pipeline_dir)
    all_pipelines = get_all_pipelines(pipeline_dir)
    unused_pipelines = all_pipelines - used_pipelines

    print(f"🔍 Found {len(unused_pipelines)} unused pipelines.")

    unused_datasets = get_datasets_from_unused_pipelines(pipeline_dir, unused_pipelines)

    print(f"\n Datasets used only in unused pipelines ({len(unused_datasets)}):")
    for ds in sorted(unused_datasets):
        print(f"- {ds}")

    dataset_to_pipelines = {}
    for pipeline_name in all_pipelines:
        file_path = os.path.join(pipeline_dir, f"{pipeline_name}.json")
        if os.path.exists(file_path):
            datasets = extract_datasets_from_pipeline_file(file_path)
            for dataset in datasets:
                dataset_to_pipelines.setdefault(dataset, set()).add(pipeline_name)

    update_dataset_paths_in_place(unused_datasets, dataset_dir, dataset_to_pipelines, unused_pipelines)

if __name__ == "__main__":
    main()
