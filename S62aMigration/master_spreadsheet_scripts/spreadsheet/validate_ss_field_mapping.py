
import os
import re
 
import pandas as pd
 
BASE_DIR             = "/Users/nisalihalwathura/PINS/ODW-Service/odw-synapse-workspace/S62aMigration"
SPREADSHEET_EXTRACTS_DIR = os.path.join(BASE_DIR, "csv_and_xlsx_files/SS_data")
MAPPING_CSV               = os.path.join(BASE_DIR, "outputs/spreadsheet_field_mapping.csv")
OUTPUT_FILE                = os.path.join(BASE_DIR, "outputs/spreadsheet_field_mapping_validation.csv")
 
print("Block 1 done - config set")
 
# reads just the header row of specific tabs in every Excel file and builds a lookup:
# normalised column name -> {filename, sheet name, original column name}
# This is the set of "real" columns we will check Source fields against.
 
TARGET_SHEETS = ['Pre-application - DONE', 'Application (Major)', 'Application (Non Major)']
 
_camel = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
def _norm(t): return re.sub(r"\s+", " ", _camel.sub(" ", str(t)).lower()).strip()
 
# norm -> list of (filename, sheet, original_col)
all_cols: dict[str, list[tuple[str, str, str]]] = {}
 
for filename in sorted(os.listdir(SPREADSHEET_EXTRACTS_DIR)):
    if not filename.endswith((".xlsx", ".xlsm")):
        continue
    filepath = os.path.join(SPREADSHEET_EXTRACTS_DIR, filename)
 
    try:
        xl = pd.ExcelFile(filepath)
    except Exception as e:
        print(f"  WARNING: could not open {filename}: {e}")
        continue
 
    for sheet in TARGET_SHEETS:
        if sheet not in xl.sheet_names:
            print(f"  WARNING: sheet '{sheet}' not found in {filename}")
            continue
        headers = pd.read_excel(xl, sheet_name=sheet, nrows=0).columns.tolist()
        for col in headers:
            all_cols.setdefault(_norm(col), []).append((filename, sheet, col))
 
print(f"Block 2 done - {len(all_cols)} distinct normalised column names found across target sheets")
 
# check every source field in the mapping csv
mapping_df = pd.read_csv(MAPPING_CSV, dtype=str).fillna("")
 
# Same fixes applied in map_ss_onto_template.py - keep in sync so this
# validator doesn't keep flagging entries that are already resolved there.
SOURCE_FIELD_ALIASES = {
    "Grant":                    "Grant/Refuse",
    "Development description":  "Development",
    "S106 suubmitted date":     "S106 submitted date",
    "Inspector interim findings (letter 25 to applicant)":
        "Inspectors Interim findings (letter 25 to applicant)",
    "Hearing date notification":
        "Hearing date notification (at least 2 weeks before hearing for major applications)",
    "Hearing date\nEvent notes on Horizon to be used for time of event": "Hearing Date",
}
 
KNOWN_TRUNCATED_SOURCE_FIELDS = {
    _norm("Procedure WR"),
    _norm("Valid letters (17 & 9) to LPA"),
    _norm("Fee return date"),
    _norm("Consultations deadline (see statutory consultees and Town Parish "
          "Council list for list of parties consulted"),
}
 
# Same corrections/additions applied in map_ss_onto_template.py - keep in
# sync so this validator doesn't keep flagging entries already resolved
# there. See that script for why each one is needed.
MANUAL_SOURCE_FIELD_OVERRIDES = {
    "Cil amount": "CIL amount",
}
MANUAL_FIELD_ADDITIONS = [
    ("Agent organisation name",     "Agent"),
    ("Agent email",                 "Agent"),
    ("Applicant organisation name", "Applicant"),
]
_extra_rows = pd.DataFrame(MANUAL_FIELD_ADDITIONS, columns=["Field", "Source field"])
_extra_rows["Source"] = "Spreadsheet"
mapping_df = pd.concat([mapping_df, _extra_rows], ignore_index=True)

results = []
for _, row in mapping_df.iterrows():
    field        = row["Field"].strip()
    source_field = row["Source field"].strip()
    source_field = MANUAL_SOURCE_FIELD_OVERRIDES.get(field, source_field)
    source_field = SOURCE_FIELD_ALIASES.get(source_field, source_field)
 
    if not source_field:
        continue
 
    norm_sf = _norm(source_field)
    matches = all_cols.get(norm_sf, [])
    if not matches and norm_sf in KNOWN_TRUNCATED_SOURCE_FIELDS:
        # These are individually verified truncated prefixes - kept as an
        # explicit whitelist rather than a generic prefix fallback, since a
        # generic version was found to over-match on short/common words
        # (e.g. "Inspector" also matching "Inspector USV or ARSV date").
        # Kept in sync with map_ss_onto_template.py.
        for norm_col, col_entries in all_cols.items():
            if norm_col.startswith(norm_sf):
                matches = col_entries
                break
 
    results.append({
        "Field":        field,
        "Source field": source_field,
        "Found":        "YES" if matches else "NO",
        "Found in files": ", ".join(sorted({f"{f} [{s}]" for f, s, _ in matches})) if matches else "",
    })
 
found   = [r for r in results if r["Found"] == "YES"]
missing = [r for r in results if r["Found"] == "NO"]
 
print(f"\nBlock 3 done:")
print(f"  {len(found)} Source fields found in at least one sheet")
print(f"  {len(missing)} Source fields NOT found in any sheet:")
for r in missing:
    print(f"    Field={r['Field']!r:45}  Source field={r['Source field']!r}")
 
# write validation report
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)
print(f"\nBlock 4 done - full validation report written to {OUTPUT_FILE}")
 