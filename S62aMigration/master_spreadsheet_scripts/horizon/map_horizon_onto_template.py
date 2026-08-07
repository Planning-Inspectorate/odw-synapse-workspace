# %% [markdown]
# # Map all Horizon extracts onto MASTER LEGACY cases S62A
#
# Follows the same block-by-block pattern as ssmapping_stepbystep.py.
#
# horizon_field_mapping.csv tells us:
#   'Source field' = column name (in human-readable form) in the Horizon CSV
#   'Field'        = column name in MASTER LEGACY cases S62A .xlsx
#
# each CSV in Horizon_extracts/ is processed in turn. Because multiple CSVs
# share the same CaseReference (each file just has different columns for the
# same case), rows from all files are merged into ONE row per case in the
# output. Any CSV columns that have no entry in horizon_field_mapping.csv are
# flagged in a separate report.

# CONFIG
import csv
import os
import re
import shutil
from datetime import datetime, date

import openpyxl
import pandas as pd

BASE_DIR = "/Users/nisalihalwathura/PINS/ODW-Service/odw-synapse-workspace/S62aMigration"

HORIZON_EXTRACTS_DIR = os.path.join(BASE_DIR, "csv_and_xlsx_files/Horizon_extracts")
MAPPING_CSV          = os.path.join(BASE_DIR, "outputs/horizon_field_mapping.csv")
MASTER_FILE          = os.path.join(BASE_DIR, "csv_and_xlsx_files/MASTER LEGACY cases S62A .xlsx")
TEMPLATE_SHEET       = "Template"
TEMPLATE_HEADER_ROW     = 2
TEMPLATE_FIRST_DATA_ROW = 3

OUTPUT_FILE          = os.path.join(BASE_DIR, "outputs/MASTER LEGACY cases S62A - with Horizon data.xlsx")
AUDIT_LOG_FILE       = os.path.join(BASE_DIR, "outputs/horizon_migration_audit_log.csv")
UNMAPPED_COLS_FILE   = os.path.join(BASE_DIR, "outputs/horizon_unmapped_columns_report.csv")

print("Block 1 done - config set")


# MAPPING HELPER
# build_mapping_for_file() reads the actual column headers from one CSV and
# returns the same MAPPING list format: (template_column, source_column,
# transform, extra) - one entry per column that has a match in the mapping CSV.
# It also returns the list of column names that had NO match (for the report).

_camel      = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
def _norm(t): return re.sub(r"\s+", " ", _camel.sub(" ", str(t)).lower()).strip()

_mapping_df = pd.read_csv(MAPPING_CSV, dtype=str).fillna("")

# Pre-build a lookup: normalized Source field -> template Field, using the
# mapping CSV. A normalized Source field can appear in multiple mapping rows
# (e.g. "Appointed person" maps to Inspector 1, 2 and 3) so we keep a list.
_sf_to_fields: dict[str, list[str]] = {}
for _, _r in _mapping_df.iterrows():
    _field = _r["Field"].strip()
    _sf    = _norm(_r["Source field"].strip())
    if _field and _sf:
        _sf_to_fields.setdefault(_sf, []).append(_field)

# Normalized Source fields known to the mapping CSV (used to detect unmapped cols)
_all_mapped_sf = set(_sf_to_fields.keys())

def build_mapping_for_file(filepath):
    """Returns (MAPPING, unmapped_columns) for the given CSV file.
    MAPPING  = list of (template_column, source_column, 'direct', None)
    unmapped = list of column names that have no entry in horizon_field_mapping
    """
    headers = pd.read_csv(filepath, nrows=0).columns.tolist()
    mapping = []
    unmapped = []
    for col in headers:
        norm_col = _norm(col)
        if norm_col in _sf_to_fields:
            for template_col in _sf_to_fields[norm_col]:
                mapping.append((template_col, col, "direct", None))
        else:
            unmapped.append(col)
    return mapping, unmapped

print("Block 2 done - mapping helper ready")


# TRANSFORMS
def is_blank(value):
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    if isinstance(value, str) and value.strip() in ("", "NULL", "null"):
        return True
    return False

def transform_direct(value, extra):
    return None if is_blank(value) else str(value).strip()

def transform_date(value, extra):
    if is_blank(value):
        return None
    if isinstance(value, (datetime, date)):
        return value.date() if isinstance(value, datetime) else value
    try:
        return pd.to_datetime(str(value).strip(), dayfirst=True).date()
    except (ValueError, TypeError):
        return None

def transform_constant(value, extra):
    return extra

TRANSFORM_FUNCTIONS = {
    "direct":   transform_direct,
    "date":     transform_date,
    "constant": transform_constant,
}

print(f"Block 3 done - {len(TRANSFORM_FUNCTIONS)} transform functions ready")


# LOOP ALL CSVs AND BUILD ONE ROW PER CASE
# case_rows  : {CaseReference: {template_column: value}}  - merged across all files
# all_audit  : list of (case_ref, template_col, issue)
# unmapped_report: list of {file, column} for columns not in the mapping CSV

def build_output_rows(df, mapping):
    output_rows   = []
    audit_entries = []
    for _, source_row in df.iterrows():
        row_result = {}
        for template_column, source_column, transform_name, extra in mapping:
            source_value = source_row.get(source_column)
            transform_fn = TRANSFORM_FUNCTIONS[transform_name]
            result = transform_fn(source_value, extra)
            if result is not None:
                if template_column in row_result and row_result[template_column] is not None:
                    row_result[template_column] = f"{row_result[template_column]}; {result}"
                else:
                    row_result[template_column] = result
            elif transform_name != "constant" and not is_blank(source_value):
                audit_entries.append((
                    row_result.get("Case reference"),
                    template_column,
                    f"could not convert value: {source_value!r}"
                ))
        output_rows.append(row_result)
    return output_rows, audit_entries

case_rows      = {}   # CaseReference -> merged row dict
all_audit      = []
unmapped_report = []  # {file, column}

# Load the authoritative list of S62A case references so we only process
# rows that belong to S62A cases (some CSVs like case_involvement_o cover
# the entire Horizon system and would otherwise bring in hundreds of thousands
# of non-S62A cases).
_ref_file = os.path.join(HORIZON_EXTRACTS_DIR, "20260716_query_s62A_case_reference_list.csv")
S62A_CASES = set(pd.read_csv(_ref_file, dtype=str)["CaseReference"].dropna().str.strip())
print(f"  S62A case reference list loaded: {len(S62A_CASES)} cases")

csv_files = sorted(f for f in os.listdir(HORIZON_EXTRACTS_DIR) if f.endswith(".csv"))

for filename in csv_files:
    filepath = os.path.join(HORIZON_EXTRACTS_DIR, filename)
    mapping, unmapped_cols = build_mapping_for_file(filepath)

    # Record unmapped columns for this file
    for col in unmapped_cols:
        unmapped_report.append({"File": filename, "Column": col})

    if not mapping:
        print(f"  {filename}: no mapped columns - skipped")
        continue

    df = pd.read_csv(filepath, dtype=str).dropna(how="all")
    if "CaseReference" in df.columns:
        df = df[df["CaseReference"].str.strip().isin(S62A_CASES)]
    rows, audit = build_output_rows(df, mapping)
    all_audit.extend(audit)

    # Merge into case_rows by CaseReference (first value seen wins per column)
    for row_result in rows:
        case_ref = row_result.get("Case reference")
        if not case_ref:
            continue
        if case_ref not in case_rows:
            case_rows[case_ref] = {}
        for col, val in row_result.items():
            if col not in case_rows[case_ref] or case_rows[case_ref][col] is None:
                case_rows[case_ref][col] = val

    print(f"  {filename}: {len(mapping)} mapped columns, {len(unmapped_cols)} unmapped, {len(df)} rows")

all_rows = list(case_rows.values())
print(f"\nBlock 4 done - {len(all_rows)} unique cases, {len(all_audit)} audit flags, {len(unmapped_report)} unmapped column entries")


# SPOT CHECK - show the first case
if all_rows:
    print("Block 5 spot check - first case:")
    for k, v in all_rows[0].items():
        print(f"  {k!r}: {v!r}")


# WRITE OUTPUT
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
shutil.copy(MASTER_FILE, OUTPUT_FILE)   # never modify the original

wb = openpyxl.load_workbook(OUTPUT_FILE)
ws = wb[TEMPLATE_SHEET]

# Build column-name -> Excel column number from the header row
column_lookup = {}
for col_num in range(1, ws.max_column + 1):
    header_value = ws.cell(row=TEMPLATE_HEADER_ROW, column=col_num).value
    if header_value:
        column_lookup[str(header_value).strip()] = col_num

excel_row = TEMPLATE_FIRST_DATA_ROW
for row_result in all_rows:
    for template_column, value in row_result.items():
        if template_column not in column_lookup:
            print(f"WARNING: '{template_column}' not found in Template headers - skipped")
            continue
        col_num = column_lookup[template_column]
        cell = ws.cell(row=excel_row, column=col_num)
        cell.value = value
        if isinstance(value, date):
            cell.number_format = "YYYY-MM-DD"
    excel_row += 1

wb.save(OUTPUT_FILE)
print(f"Block 6 done - wrote {len(all_rows)} rows to {OUTPUT_FILE}")


# WRITE AUDIT LOG AND UNMAPPED COLUMNS REPORT
os.makedirs(os.path.dirname(AUDIT_LOG_FILE), exist_ok=True)

with open(AUDIT_LOG_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["Case reference", "Template column", "Issue"])
    writer.writerows(all_audit)
print(f"Audit log:              {AUDIT_LOG_FILE} ({len(all_audit)} entries)")

with open(UNMAPPED_COLS_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["File", "Column"])
    writer.writeheader()
    writer.writerows(unmapped_report)
print(f"Unmapped columns report: {UNMAPPED_COLS_FILE} ({len(unmapped_report)} entries)")
