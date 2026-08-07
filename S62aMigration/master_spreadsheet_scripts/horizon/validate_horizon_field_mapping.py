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

# Reads just the header row of every CSV file and builds a lookup:
#   normalised column name -> {filename, original column name}
# This is the set of "real" columns we will check Source fields against.

_camel = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
def _norm(t): return re.sub(r"\s+", " ", _camel.sub(" ", str(t)).lower()).strip()

# norm -> list of (filename, original_col)
all_cols: dict[str, list[tuple[str, str]]] = {}

for filename in sorted(os.listdir(HORIZON_EXTRACTS_DIR)):
    if not filename.endswith(".csv"):
        continue
    filepath = os.path.join(HORIZON_EXTRACTS_DIR, filename)
    headers  = pd.read_csv(filepath, nrows=0).columns.tolist()
    for col in headers:
        all_cols.setdefault(_norm(col), []).append((filename, col))

print(f"Block 2 done - {len(all_cols)} distinct normalised column names found across all CSVs")

# check every source field in the mapping csv
mapping_df = pd.read_csv(MAPPING_CSV, dtype=str).fillna("")

results = []
for _, row in mapping_df.iterrows():
    field        = row["Field"].strip()
    source_field = row["Source field"].strip()

    if not source_field:
        continue

    norm_sf = _norm(source_field)
    matches = all_cols.get(norm_sf, [])

    results.append({
        "Field":        field,
        "Source field": source_field,
        "Found":        "YES" if matches else "NO",
        "Found in files": ", ".join(sorted({f for f, _ in matches})) if matches else "",
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
