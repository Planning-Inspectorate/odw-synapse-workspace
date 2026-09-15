# # Validate horizon_field_mapping.csv against the Horizon extract CSVs
#
# Checks whether every 'Source field' value in horizon_field_mapping.csv
# matches a real column in at least one of the CSV files in Horizon_extracts/.
# Outputs any Source fields that have no match so they can be corrected.

import os
import re

import pandas as pd

BASE_DIR             = "/Users/nisalihalwathura/PINS/ODW-Service/odw-synapse-workspace/S62aMigration"
HORIZON_EXTRACTS_DIR = os.path.join(BASE_DIR, "csv_and_xlsx_files/Horizon_extracts")
MAPPING_CSV          = os.path.join(BASE_DIR, "outputs/horizon_field_mapping.csv")
OUTPUT_FILE          = os.path.join(BASE_DIR, "outputs/horizon_field_mapping_validation.csv")

print("Block 1 done - config set")

# Reads just the header row of every CSV file and builds a lookup by filename.
# Source fields use the format filename_token.column_name. The token is matched
# against the complete extract filename because exports have dated prefixes.

_camel = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
def _norm(t): return re.sub(r"\s+", " ", _camel.sub(" ", str(t)).lower()).strip()

# filename -> {normalised column name: original column name}
all_cols: dict[str, dict[str, str]] = {}

for filename in sorted(os.listdir(HORIZON_EXTRACTS_DIR)):
    if not filename.endswith(".csv"):
        continue
    filepath = os.path.join(HORIZON_EXTRACTS_DIR, filename)
    headers  = pd.read_csv(filepath, nrows=0).columns.tolist()
    all_cols[filename] = {_norm(col): col for col in headers}

print(f"Block 2 done - {len(all_cols)} distinct normalised column names found across all CSVs")

# check every source field in the mapping csv
mapping_df = pd.read_csv(MAPPING_CSV, dtype=str).fillna("")

def _source_parts(source_field):
    """Return the filename token and referenced column names."""
    references = re.findall(r"([\w-]+)\.([\w]+)", source_field)
    if not references:
        return "", []
    token = references[0][0].strip()
    source_columns = []
    for reference_token, source_column in references:
        if reference_token == token and source_column not in source_columns:
            source_columns.append(source_column)
    return token, source_columns

results = []
for _, row in mapping_df.iterrows():
    field        = row["Field"].strip()
    source_field = row["Source field"].strip()

    if not source_field:
        continue

    file_token, source_columns = _source_parts(source_field)
    matching_files = sorted(
        filename for filename in all_cols if file_token in filename
    )
    missing_columns = []
    matched_files = set()
    for source_column in source_columns:
        norm_column = _norm(source_column)
        column_files = [
            filename for filename in matching_files
            if norm_column in all_cols[filename]
        ]
        if column_files:
            matched_files.update(column_files)
        else:
            missing_columns.append(source_column)

    found = bool(source_columns) and not missing_columns

    results.append({
        "Field":        field,
        "Source field": source_field,
        "Found":        "YES" if found else "NO",
        "Found in files": ", ".join(sorted(matched_files)),
        "Filename token": file_token,
        "Missing columns": ", ".join(missing_columns),
    })

found   = [r for r in results if r["Found"] == "YES"]
missing = [r for r in results if r["Found"] == "NO"]

print(f"\nBlock 3 done:")
print(f"  {len(found)} Source fields found in at least one CSV")
print(f"  {len(missing)} Source fields NOT found in any CSV:")
for r in missing:
    print(f"    Field={r['Field']!r:45}  Source field={r['Source field']!r}")

# write validation report
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)
print(f"\nBlock 4 done - full validation report written to {OUTPUT_FILE}")
