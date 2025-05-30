import os
import json

def extract_datasets_from_activity(activity):
    datasets = set()

    for io_key in ["inputs", "outputs"]:
        for ds in activity.get(io_key, []):
            ref = ds.get("referenceName")
            if ref:
                datasets.add(ref)

    nested = activity.get("typeProperties", {})
    for key in ["dataset", "inputDataset", "outputDataset"]:
        ref = nested.get(key, {}).get("referenceName")
        if ref:
            datasets.add(ref)

    if "activities" in nested:
        for sub_activity in nested["activities"]:
            datasets.update(extract_datasets_from_activity(sub_activity))

    return datasets

def extract_datasets_from_pipeline_file(pipeline_file):
    datasets = set()
    with open(pipeline_file, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            print(f" Skipping invalid JSON: {pipeline_file}")
            return datasets

    activities = data.get("properties", {}).get("activities", [])
    for activity in activities:
        datasets.update(extract_datasets_from_activity(activity))

    return datasets

def get_pipeline_datasets(pipeline_dir):
    all_datasets = set()
    for file in os.listdir(pipeline_dir):
        if file.endswith(".json"):
            full_path = os.path.join(pipeline_dir, file)
            all_datasets.update(extract_datasets_from_pipeline_file(full_path))
    return all_datasets

def get_all_workspace_datasets(dataset_dir):
    all_datasets = set()
    for file in os.listdir(dataset_dir):
        if file.endswith(".json"):
            dataset_name = os.path.splitext(file)[0]
            all_datasets.add(dataset_name)
    return all_datasets

def archive_unused_datasets(unused_datasets, dataset_dir):
    print(f"\n📁 Archiving unused datasets using JSON metadata...")

    for ds in sorted(unused_datasets):
        file_path = os.path.join(dataset_dir, f"{ds}.json")
        if not os.path.exists(file_path):
            print(f" Not found: {ds}.json")
            continue

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = json.load(f)
        except json.JSONDecodeError:
            print(f" Invalid JSON: {ds}.json")
            continue

        changed = False

        # Set folder section
        folder_section = content.setdefault("properties", {}).setdefault("folder", {})
        if folder_section.get("name") != "Archive/Unused-Dataset":
            folder_section["name"] = "Archive/Unused-Dataset"
            changed = True

        # Optionally update folderPath expression in typeProperties.location
        location = content.get("properties", {}).get("typeProperties", {}).get("location", {})
        if isinstance(location, dict):
            folder_path = location.get("folderPath")
            if isinstance(folder_path, dict) and "value" in folder_path:
                original_expr = folder_path["value"]
                is_archived = original_expr.strip().startswith("@concat('archive/")
                if not is_archived:
                    folder_path["value"] = f"@concat('archive/', {original_expr})"
                    changed = True

        if changed:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(content, f, indent=4)
            print(f" Archived: {ds}.json")
        else:
            print(f"⏭️ Already archived: {ds}.json")

def main():
    pipeline_dir = "workspace/pipeline"
    dataset_dir = "workspace/dataset"

    if not os.path.exists(pipeline_dir) or not os.path.exists(dataset_dir):
        print(" One or more required folders not found.")
        return

    print("🔍 Collecting datasets used in pipelines...")
    pipeline_datasets = get_pipeline_datasets(pipeline_dir)

    print("📦 Collecting all datasets in workspace/dataset...")
    all_datasets = get_all_workspace_datasets(dataset_dir)

    used = all_datasets & pipeline_datasets
    unused = all_datasets - pipeline_datasets

    print("\n USED DATASETS:")
    for ds in sorted(used):
        print(f"   {ds}")

    print("\n🗃️ UNUSED DATASETS:")
    for ds in sorted(unused):
        print(f"   {ds}")

    # Archive unused datasets by editing their folder metadata
    archive_unused_datasets(unused, dataset_dir)

if __name__ == "__main__":
    main()
