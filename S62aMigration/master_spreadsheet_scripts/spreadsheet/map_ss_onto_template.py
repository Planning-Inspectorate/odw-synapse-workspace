import csv
import os
import re
from datetime import datetime, date
 
import openpyxl
import pandas as pd
 
SOURCE_FILE   = "csv_and_xlsx_files/SS_data/Section-62a-Cases-COPY.xlsx"
MAPPING_CSV   = "outputs/spreadsheet_field_mapping.csv"
TEMPLATE_FILE = "csv_and_xlsx_files/MASTER LEGACY cases S62A .xlsx"
TEMPLATE_SHEET          = "Template"
TEMPLATE_HEADER_ROW     = 2
TEMPLATE_FIRST_DATA_ROW = 3
 
OUTPUT_FILE        = "outputs/S62A_All_Sheets_migrated.xlsx"
AUDIT_LOG_FILE      = "outputs/spreadsheet_migration_audit_log.csv"
UNMAPPED_COLS_FILE  = "outputs/spreadsheet_unmapped_columns_report.csv"
 
print("Block 1 done - config set")
 
# %%
# ============================================================
# BLOCK 2: MAPPING - built from spreadsheet_field_mapping.csv
# ============================================================
# Start with just Major while testing - uncomment the rest once it works.
SHEET_CONFIGS = [
    ("Application (Major)", {
        "Application Status": "Closed",
        "Application phase": "Application",   
        "Application classification": "Major",
        "CIL amount": 0,
    }),
    ("Application (Non Major)", {
        "Application Status": "Closed",
        "Application phase": "Application",
        "Application classification": "Non major",
        "CIL amount": 0,
    }),
    ("Pre-application - DONE", {          
        "Application Status": "Closed",
        "Application phase": "Pre-application",
    }),
]
 

TRANSFORM_OVERRIDES = {
    "Case reference":              ("before_bracket", None),
    "Site address 1":              ("address_part", "address1"),
    "Site address 2":              ("address_part", "address2"),
    "Site town or city":           ("address_part", "town"),
    "Site county":                 ("address_part", "county"),
    "Site post code":              ("address_part", "postcode"),
    "Agent organisation name":     ("agent_org", None),
    "Agent email":                 ("agent_email", None),
    "Decision outcome":            ("grant_refuse", None),
}
 

FIELD_NAME_OVERRIDES = {
    "Application received date":                    "Pre-app/Application received date",
    "Application valid date":                       "Application valid",
    "Appointed person / Inspector 1":                "Inspector 1",
    "Appointed person / Inspector 2":                "Inspector 2",
    "Appointed person / Inspector 3":                "Inspector 3",
    "Date agreed for additional information":        "Date agreed for additional info",
    "Fee received date (appears if above if yes)":   "Fee received date",
    "Further information requested":                 "Further info requested",
    "LPA name":                                       "LPA",
    "LPA questionnaire received date":                "LPA questionnaire rec'd date",
    "LPA questionnaire sent date":                    "LPA questionnaire sent",
    "Additional meetings required":                   "Additional meeting required date",
    "Secondary LPA name":                             "Secondary LPA",
    "Cil amount":                                     "CIL amount",
    "BNG exempt":                                     "BNG Exempt",
    "Hearing date":                                   "Hearing  date",  
}
 

SKIP_FIELDS = {
    # "Secondary LPA name" is mapped to "Address" in the discovery sheet
    # (looks like a data-entry error there) - the site address already has
    # its own home in Site address 1-5, so leave Secondary LPA blank rather
    # than duplicate the address text into it.
    "Secondary LPA name",
    # "Appointed person / Inspector 2/3" share the same single "Inspector"
    # source column as Inspector 1 - the spreadsheet only has one inspector
    # name, so only Inspector 1 gets it; 2 and 3 stay blank.
    "Appointed person / Inspector 2",
    "Appointed person / Inspector 3",
}

SPLIT_FIELD_OVERRIDES = {
    "Site address": [
        ("Site address 1",    "address1"),
        ("Site address 2",    "address2"),
        ("Site town or city", "town"),
        ("Site county",       "county"),
        ("Site post code",    "postcode"),
    ],
}
 
_camel = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
def _norm(t):
    return re.sub(r"\s+", " ", _camel.sub(" ", str(t)).lower()).strip()
 

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
 
_mapping_df = pd.read_csv(MAPPING_CSV, dtype=str).fillna("")
 
_sf_to_fields: dict[str, list[str]] = {}
for _, _r in _mapping_df.iterrows():
    _field = _r["Field"].strip()
    _sf_raw = _r["Source field"].strip()
    _sf_raw = SOURCE_FIELD_ALIASES.get(_sf_raw, _sf_raw)
    _sf    = _norm(_sf_raw)
    if _field and _sf:
        _sf_to_fields.setdefault(_sf, []).append(_field)

# Corrections/additions to Sophie's discovery sheet (S62A-Data-Discovery.xlsx),
# applied here in code rather than waiting on that sheet to be edited. If the
# sheet is corrected there later, these become harmless no-ops - safe to
# leave in place either way.
MANUAL_SOURCE_FIELD_OVERRIDES = {
    # Discovery sheet points "Cil amount" at the free-text "LPA CIL response"
    # column - the real numeric CIL value lives in the actual "CIL amount"
    # column instead. "LPA CIL response" still correctly feeds "CIL liable".
    "Cil amount": "CIL amount",
}

MANUAL_FIELD_ADDITIONS = [
    # Agent and Applicant have no Spreadsheet-source row in the discovery
    # sheet at all, so without this they're silently dropped.
    ("Agent organisation name",     "Agent"),
    ("Agent email",                 "Agent"),
    ("Applicant organisation name", "Applicant"),
]

for _field, _new_source in MANUAL_SOURCE_FIELD_OVERRIDES.items():
    for _norm_sf, _fields in list(_sf_to_fields.items()):
        if _field in _fields:
            _fields.remove(_field)
            if not _fields:
                del _sf_to_fields[_norm_sf]
    _sf_to_fields.setdefault(_norm(_new_source), []).append(_field)

for _field, _source in MANUAL_FIELD_ADDITIONS:
    _sf_to_fields.setdefault(_norm(_source), []).append(_field)
 
def transform_for_field(field):
    if field in TRANSFORM_OVERRIDES:
        return TRANSFORM_OVERRIDES[field]
    if "date" in field.lower():
        return ("date", None)
    return ("direct", None)
 

TRUNCATED_SOURCE_PREFIXES = {
    _norm("Procedure WR"): ["Procedure"],
    _norm("Valid letters (17 & 9) to LPA"): ["Valid letters sent"],
    _norm("Fee return date"): ["Fee refund"],
    _norm("Consultations deadline (see statutory consultees and Town Parish "
          "Council list for list of parties consulted"): ["Representations period"],
}
 
def build_mapping_for_sheet(columns):
    """Returns (MAPPING, unmapped_columns) for a sheet's column headers.
    MAPPING  = list of (template_column, source_column, transform, extra)
    unmapped = source columns with no entry in spreadsheet_field_mapping.csv
    """
    mapping = []
    unmapped = []
 

    ref_col = next((c for c in columns if _norm(c) == "ref"), None)
    if ref_col:
        mapping.append(("Case reference", ref_col, "before_bracket", None))
 
    for col in columns:
        if col == ref_col:
            continue  # already handled explicitly above
        norm_col = _norm(col)
        matched_targets = _sf_to_fields.get(norm_col)
        if matched_targets is None:
            for prefix, targets in TRUNCATED_SOURCE_PREFIXES.items():
                if norm_col.startswith(prefix):
                    matched_targets = targets
                    break
        if matched_targets:
            for raw_template_col in matched_targets:
                if raw_template_col in SKIP_FIELDS:
                    continue
                if raw_template_col in SPLIT_FIELD_OVERRIDES:
                    for split_col, extra in SPLIT_FIELD_OVERRIDES[raw_template_col]:
                        mapping.append((split_col, col, "address_part", extra))
                elif raw_template_col == "Representations period":
                    # Fed by two different source columns - route by which one.
                    target = "Representations period - End" if "deadline" in col.lower() \
                        else "Representations period - start"
                    mapping.append((target, col, "date", None))
                else:
                    template_col = FIELD_NAME_OVERRIDES.get(raw_template_col, raw_template_col)
                    transform_name, extra = transform_for_field(template_col)
                    mapping.append((template_col, col, transform_name, extra))
        else:
            unmapped.append(col)
    return mapping, unmapped
 
print(f"Block 2 done - {len(_sf_to_fields)} mapped source fields loaded")
 
# %%
# ============================================================
# BLOCK 3: TRANSFORMS
# ============================================================
def is_blank(value):
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False
 
def transform_direct(value, extra):
    return None if is_blank(value) else value
 
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
 
def transform_before_bracket(value, extra):
    return None if is_blank(value) else str(value).split("(")[0].strip()
 
UK_POSTCODE_PATTERN = re.compile(r"\b([A-Za-z]{1,2}\d[A-Za-z\d]?\s*\d[A-Za-z]{2})\b")
 
def split_address_parts(text):
    """Best-effort split of one free-text address into its pieces.
    Returns a dict with keys: address1, address2, town, county, postcode."""
    if is_blank(text):
        return {}
    text = str(text).strip()
 
    postcode = None
    match = UK_POSTCODE_PATTERN.search(text)
    if match:
        postcode = match.group(1).upper()
        text = text[:match.start()].rstrip(", ")   # drop postcode + trailing comma
 
    parts = [p.strip() for p in text.split(",") if p.strip()]
    county = parts[-1] if len(parts) >= 1 else None
    town = parts[-2] if len(parts) >= 2 else None
    address_parts = parts[:-2] if len(parts) > 2 else []
    address1 = address_parts[0] if len(address_parts) >= 1 else None
    address2 = "; ".join(address_parts[1:]) if len(address_parts) > 1 else None
 
    return {"address1": address1, "address2": address2, "town": town,
            "county": county, "postcode": postcode}
 
def transform_address_part(value, extra):
    """extra = which piece to return: 'address1', 'address2', 'town', 'county', 'postcode'."""
    return split_address_parts(value).get(extra)
 
def transform_grant_refuse(value, extra):
    if is_blank(value):
        return None
    text = str(value).strip().lower()
    if "grant" in text:
        return "Granted"
    if "refus" in text:
        return "Refused"
    return None
 
EMAIL_PATTERN = re.compile(r"[\w.\-]+@[\w.\-]+")
 
def transform_agent_org(value, extra):
    """Takes the text before the first '(' as the org name, e.g. 'CBRE (a@x.com; b@y.com)' -> 'CBRE'."""
    if is_blank(value):
        return None
    return str(value).split("(")[0].strip()
 
def transform_agent_email(value, extra):
    """Pulls every email address out of the cell (there can be several), joined with '; '."""
    if is_blank(value):
        return None
    emails = EMAIL_PATTERN.findall(str(value))
    return "; ".join(emails) if emails else None
 
TRANSFORM_FUNCTIONS = {
    "direct": transform_direct, "date": transform_date, "constant": transform_constant,
    "before_bracket": transform_before_bracket, "address_part": transform_address_part,
    "grant_refuse": transform_grant_refuse,
    "agent_org": transform_agent_org, "agent_email": transform_agent_email,
}
 
print(f"Block 3 done - {len(TRANSFORM_FUNCTIONS)} transform functions ready")
 
# %%
# ============================================================
# BLOCK 4: READ SOURCE - just the first sheet in SHEET_CONFIGS for now
# ============================================================
def read_source_rows(sheet_name):
    df = pd.read_excel(SOURCE_FILE, sheet_name=sheet_name, header=0)
    df = df.dropna(how="all")
    first_col = df.columns[0]
    df = df[df[first_col].notna()]
    return df
 
_test_sheet, _test_constants = SHEET_CONFIGS[0]
df_test = read_source_rows(_test_sheet)
 
print(f"Block 4 done - {_test_sheet}: loaded {len(df_test)} rows")
print(df_test.head())

# %%
# ============================================================
# BLOCK 5: BUILD ROWS - try it on ONE row first before all of them
# ============================================================
def build_output_rows(df, mapping, constants, sheet_label):
    output_rows   = []
    audit_entries = []
    for _, source_row in df.iterrows():
        row_result = dict(constants)
        for template_column, source_column, transform_name, extra in mapping:
            source_value = source_row.get(source_column)
            transform_fn = TRANSFORM_FUNCTIONS[transform_name]
            result = transform_fn(source_value, extra)
            if result is not None:
                existing = row_result.get(template_column)
                if existing and str(result) not in str(existing).split("; "):
                    row_result[template_column] = f"{existing}; {result}"
                elif not existing:
                    row_result[template_column] = result
            elif transform_name not in ("constant", "address_part") and not is_blank(source_value):
                audit_entries.append((row_result.get("Case reference"), sheet_label,
                                       template_column, f"could not convert value: {source_value!r}"))
        output_rows.append(row_result)
    return output_rows, audit_entries
 
mapping_test, unmapped_test = build_mapping_for_sheet(df_test.columns.tolist())
one_row_result, one_row_audit = build_output_rows(df_test.head(1), mapping_test, _test_constants, _test_sheet)
 
print(f"Block 5 test done - {_test_sheet}: {len(mapping_test)} mapped columns, {len(unmapped_test)} unmapped")
print("Single row result:")
for k, v in one_row_result[0].items():
    print(f"  {k}: {v!r}")
print(f"  ({len(one_row_audit)} audit flags for this row)")
# ^ CHECK: does every value look sensible? Fix TRANSFORM_OVERRIDES/FIELD_NAME_OVERRIDES before continuing.
 
# %%
# ============================================================
# BLOCK 5b: now run it on ALL rows, for every sheet in SHEET_CONFIGS
# ============================================================
all_rows        = []
all_audit       = []
unmapped_report = []  # {Sheet, Column}
 
for sheet_name, constants in SHEET_CONFIGS:
    df = read_source_rows(sheet_name)
    mapping, unmapped_cols = build_mapping_for_sheet(df.columns.tolist())
    for col in unmapped_cols:
        unmapped_report.append({"Sheet": sheet_name, "Column": col})
 
    rows, audit = build_output_rows(df, mapping, constants, sheet_name)
    all_rows.extend(rows)
    all_audit.extend(audit)
    print(f"  {sheet_name}: {len(mapping)} mapped columns, {len(unmapped_cols)} unmapped, {len(df)} rows")
 
print(f"Block 5b done - {len(all_rows)} rows built, {len(all_audit)} audit flags")

# %%
# ============================================================
# BLOCK 6: WRITE OUTPUT
# ============================================================
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
wb = openpyxl.load_workbook(TEMPLATE_FILE)
ws = wb[TEMPLATE_SHEET]
 
column_lookup = {}
for col_num in range(1, ws.max_column + 1):
    header_value = ws.cell(row=TEMPLATE_HEADER_ROW, column=col_num).value
    if header_value:
        column_lookup[str(header_value).strip()] = col_num
 
_missing_fields = set()
excel_row = TEMPLATE_FIRST_DATA_ROW
for row_result in all_rows:
    # Clear every template column on this row first, so leftover example
    # data already sitting in the template (row 3 in particular has real
    # placeholder values, not blanks) can't leak through for any field this
    # row_result doesn't have a value for.
    for col_num in column_lookup.values():
        ws.cell(row=excel_row, column=col_num).value = None
    for template_column, value in row_result.items():
        template_column = FIELD_NAME_OVERRIDES.get(template_column, template_column)
        if template_column not in column_lookup:
            _missing_fields.add(template_column)
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
 
if _missing_fields:
    print(f"\n{len(_missing_fields)} unmatched Field name(s) - compare against real Template headers below")
    print("Unmatched Field names from the mapping CSV:")
    for f in sorted(_missing_fields):
        print(f"  {f!r}")
    print("Real Template headers:")
    for h in sorted(column_lookup):
        print(f"  {h!r}")
 
# %%
# ============================================================
# BLOCK 7: AUDIT LOG + UNMAPPED COLUMNS REPORT
# ============================================================
os.makedirs(os.path.dirname(AUDIT_LOG_FILE), exist_ok=True)
 
with open(AUDIT_LOG_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["Case reference", "Source sheet", "Template column", "Issue"])
    writer.writerows(all_audit)
print(f"Audit log:              {AUDIT_LOG_FILE} ({len(all_audit)} entries)")
 
with open(UNMAPPED_COLS_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["Sheet", "Column"])
    writer.writeheader()
    writer.writerows(unmapped_report)
print(f"Unmapped columns report: {UNMAPPED_COLS_FILE} ({len(unmapped_report)} entries)")
 
# %%