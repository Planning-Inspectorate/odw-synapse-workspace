import csv
import os
import re
import calendar
from datetime import datetime, date, timedelta

import openpyxl
import pandas as pd
from dateutil import parser as dateparser
from openpyxl.styles import PatternFill, Font

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def find_data_root(start_dir, marker="csv_and_xlsx_files", max_up=4):
    d = start_dir
    for _ in range(max_up + 1):
        if os.path.isdir(os.path.join(d, marker)):
            return d
        parent = os.path.dirname(d)
        if parent == d:
            break
        d = parent
    raise FileNotFoundError(
        f"Could not find a '{marker}' folder above {start_dir} "
        f"(searched {max_up + 1} levels up) - check the script's location."
    )

DATA_ROOT = find_data_root(BASE_DIR)

def find_source_file(data_root):
    ss_data_dir = os.path.join(data_root, "csv_and_xlsx_files", "SS_data")
    if not os.path.isdir(ss_data_dir):
        raise FileNotFoundError(f"SS_data folder not found: {ss_data_dir}")
    candidates = [
        f for f in os.listdir(ss_data_dir)
        if f.lower().endswith(".xlsx")
        and "62a" in f.lower()
        and "cases" in f.lower()
        and "copy" in f.lower()
    ]
    if not candidates:
        raise FileNotFoundError(
            f"Could not find a 'Section 62a Cases ... COPY.xlsx'-style file in {ss_data_dir}. "
            f"Files present: {os.listdir(ss_data_dir)}"
        )
    if len(candidates) > 1:
        print(f"WARNING: multiple candidate source files found, using the first: {candidates}")
    return os.path.join(ss_data_dir, candidates[0])

SOURCE_FILE   = find_source_file(DATA_ROOT)
MAPPING_XLSX  = os.path.join(DATA_ROOT, "csv_and_xlsx_files/SS_data/S62A_Column_mapping.xlsx")
MAPPING_SHEET = "Lookup"
TEMPLATE_FILE = os.path.join(DATA_ROOT, "csv_and_xlsx_files/MASTER LEGACY cases S62A .xlsx")
TEMPLATE_SHEET          = "Template"
TEMPLATE_HEADER_ROW     = 2
TEMPLATE_FIRST_DATA_ROW = 3

OUTPUT_FILE          = os.path.join(DATA_ROOT, "outputs/S62A_All_Sheets_migrated.xlsx")
AUDIT_LOG_FILE        = os.path.join(DATA_ROOT, "outputs/spreadsheet_migration_audit_log.csv")
UNMAPPED_COLS_FILE    = os.path.join(DATA_ROOT, "outputs/spreadsheet_unmapped_columns_report.csv")
MAPPING_ISSUES_FILE   = os.path.join(DATA_ROOT, "outputs/spreadsheet_mapping_config_issues.csv")

os.makedirs(os.path.join(DATA_ROOT, "outputs"), exist_ok=True)

print("Block 1 done - config set")


SET_TO_BE_RE = re.compile(r'set to be\s+"([^"]+)"', re.IGNORECASE)

def load_lookup_mapping():
    wb = openpyxl.load_workbook(MAPPING_XLSX, data_only=True)
    ws = wb[MAPPING_SHEET]
    rows = []
    current_category = None
    for r in range(2, ws.max_row + 1):
        category = ws.cell(row=r, column=1).value
        field    = ws.cell(row=r, column=2).value
        if category:
            current_category = category
        if not field:
            continue
        rows.append({
            "category": current_category,
            "field": str(field).strip(),
            "Pre-application - DONE":  ws.cell(row=r, column=3).value,
            "Application (Major)":     ws.cell(row=r, column=4).value,
            "Application (Non Major)": ws.cell(row=r, column=5).value,
            "notes": ws.cell(row=r, column=7).value,
        })
    return rows

LOOKUP_ROWS = load_lookup_mapping()


SHEET_CONSTANTS = {
    "Pre-application - DONE":  {},
    "Application (Major)":     {},
    "Application (Non Major)": {},
}


MANUAL_SOURCE_OVERRIDES = {
    "Site address 2":    ("Address", "address_part", "address2"),
    "Site town or city":  ("Address", "address_part", "town"),
    "Site county":        ("Address", "address_part", "county"),
    "Site post code":     ("Address", "address_part", "postcode"),
    "Agent organisation name": ("Agent", "agent_parse", "org"),
    "Agent first name":        ("Agent", "agent_parse", "first"),
    "Agent last name":         ("Agent", "agent_parse", "last"),
    "Agent email":              ("Agent", "agent_parse", "email"),
    "Applicant first name":     ("Applicant", "applicant_parse", "first"),
    "Applicant last name":      ("Applicant", "applicant_parse", "last"),
    "Applicant type":            ("Applicant", "applicant_parse", "type"),
    "LPA reference":      ("Ref",     "lpa_reference", None),
    "Pre-application fee due": ("Amount Invoiced", "fee_amount", None),
}


FIELD_TRANSFORM_OVERRIDES = {
    "Applicant organisation name": ("applicant_parse", "org"),
    "Case reference":          ("case_reference", None),
    "Site address 1":          ("address_part", "address1"),
    "Decision outcome":        ("grant_refuse", None),
    "Site visit type":         ("site_visit_type", None),
    "Site visit date":         ("site_visit_date", None),
    "CIL liable":               ("cil_liable", None),
    "CIL amount":                ("cil_amount", None),
    "Inspector band":          ("specialism_band", "band"),
    "Specialism":              ("specialism_band", "specialism"),
    "Press notice placed":     ("press_notice", "placed"),
    "Press notice reference":  ("press_notice", "reference"),
    "Press notice date":       ("press_notice", "date"),
    "EIA screening outcome":              ("eia_outcome", None),
    "Date environment statement was received": ("eia_received_date", None),
    "Date Environmental Statement rec'd":      ("eia_received_date", None),
    "SAP8 to SAP Helpdesk (inc customer number)": ("customer_number", None),
    "Application valid": ("application_valid_date", None),
    "Interested parties press notice deadline": ("date_earliest", None),
    "Interim findings date": ("date_earliest", None),
    "Representations period - start": ("date_earliest", None),
    "LPA interested parties deadline": ("date_earliest", None),
    "Further info requested": ("date_later_ignore_due", None),
    "S106 submitted date": ("date_later_ignore_due", None),
    "Representations period - End": ("date_strict", None),
    "Expected submission date": ("direct", None),
}


SHEET_FIELD_SKIPS = {
    "Pre-application - DONE":  {"Application Status"},
    "Application (Major)":     {"CIL amount", "Application Status"},
    "Application (Non Major)": {"Application Status"},
}


SOURCE_COLUMN_ALIASES = {
    "Application (Non Major)": {
        "SAP5 to FSSD": "SAP5 to FSSD / Fee requested by BACS",
    },
}

def _norm_col(text):
    return re.sub(r"\s+", " ", str(text).strip()).lower()

MANUAL_SOURCE_OVERRIDES_NORM = {_norm_col(k): (k, v) for k, v in MANUAL_SOURCE_OVERRIDES.items()}

def resolve_source_column(sheet_name, source_col, available_by_norm, available):
    alias = SOURCE_COLUMN_ALIASES.get(sheet_name, {}).get(source_col)
    if alias:
        source_col = alias
    if source_col in available:
        return source_col
    return available_by_norm.get(_norm_col(source_col))

def build_mapping_for_sheet(sheet_name, available_columns):
    mapping = []
    config_issues = []
    available = set(available_columns)
    available_by_norm = {_norm_col(c): c for c in available_columns}
    constants = dict(SHEET_CONSTANTS.get(sheet_name, {}))
    skip_fields_norm = {_norm_col(f) for f in SHEET_FIELD_SKIPS.get(sheet_name, set())}

    for row in LOOKUP_ROWS:
        field = row["field"]
        field_norm = _norm_col(field)
        if field_norm in skip_fields_norm:
            continue

        override = MANUAL_SOURCE_OVERRIDES_NORM.get(field_norm)
        if override is not None:
            _matched_key, (source_col, transform_name, extra) = override
        else:
            raw_source = row.get(sheet_name)
            raw_source = str(raw_source).strip() if raw_source is not None else ""
            if not raw_source or raw_source.upper() == "N/A":
                continue
            m = SET_TO_BE_RE.search(raw_source)
            if m:
                constants[field] = m.group(1)
                continue
            source_col = raw_source
            transform_name, extra = FIELD_TRANSFORM_OVERRIDES.get(field, (None, None))
            if transform_name is None:
                transform_name, extra = ("date_strict", None) if "date" in field.lower() else ("direct", None)

        resolved_col = resolve_source_column(sheet_name, source_col, available_by_norm, available)
        if resolved_col is None:
            config_issues.append((sheet_name, field, source_col,
                                   "source column not found on this sheet"))
            continue

        if field_norm == _norm_col("Pre-application fee due"):
            print(f"  [debug] '{field}' on '{sheet_name}': "
                  f"override={'YES - ' + _matched_key if override is not None else 'no (used Lookup sheet mapping)'}, "
                  f"source column -> {resolved_col!r}, transform={transform_name!r}")

        mapping.append((field, resolved_col, transform_name, extra))

    ref_resolved = resolve_source_column(sheet_name, "Ref", available_by_norm, available)
    if ref_resolved:
        mapping.append(("_redetermined_ref", ref_resolved, "redetermined_ref", None))
        mapping.append(("Received notification of intent", ref_resolved, "notification_of_intent", None))
        mapping.append(("Withdrawn date", ref_resolved, "withdrawn_date", None))

    press_notice_resolved = resolve_source_column(
        sheet_name, "PINS Interested parties press notice placed and TMP ref", available_by_norm, available)
    if press_notice_resolved:
        mapping.append(("_press_notice_raw", press_notice_resolved, "direct", None))

    inspector1_index = next((i for i, m in enumerate(mapping) if m[0] == "Inspector 1"), None)
    if inspector1_index is not None:
        inspector_col = mapping[inspector1_index][1]
        mapping[inspector1_index] = ("Inspector 1", inspector_col, "inspector_split", "1")
        mapping.append(("Inspector 2", inspector_col, "inspector_split", "2"))
        mapping.append(("Inspector 3", inspector_col, "inspector_split", "3"))

    return mapping, constants, config_issues

print(f"Block 2 done - {len(LOOKUP_ROWS)} template fields loaded from Lookup sheet")


def is_blank(value):
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    return False

def transform_direct(value, extra):
    return None if is_blank(value) else value

GENERIC_DATE_TOKEN_RE = re.compile(
    r'\d{1,2}\s*[/\-]\s*\d{1,2}\s*[/\-]\s*\d{2,4}'
    r'|\d{4}-\d{1,2}-\d{1,2}'
    r'|\d{1,2}\s+[A-Za-z]{3,9}\.?\s*,?\s*\d{2,4}'
    r'|\d{1,2}\s*-\s*[A-Za-z]{3,9}\s*-\s*\d{2,4}'
)

MIN_REASONABLE_YEAR = 2000
MAX_REASONABLE_YEAR = 2035

def _sane_date(d):
    if d is None:
        return None
    if not (MIN_REASONABLE_YEAR <= d.year <= MAX_REASONABLE_YEAR):
        return None
    return d

def transform_date(value, extra):
    if is_blank(value):
        return None
    if isinstance(value, (datetime, date)):
        return _sane_date(value.date() if isinstance(value, datetime) else value)

    text = str(value).strip()

    tokens = GENERIC_DATE_TOKEN_RE.findall(text)
    if tokens:
        chosen = tokens[1] if len(tokens) >= 2 else tokens[0]
        try:
            parsed = _sane_date(dateparser.parse(chosen, fuzzy=True, dayfirst=True).date())
            if parsed:
                return parsed
        except (ValueError, TypeError, OverflowError):
            pass

    try:
        return _sane_date(pd.to_datetime(text, dayfirst=True).date())
    except (ValueError, TypeError):
        return None

def _parse_date_tokens(tokens):
    """Parse every date token found and return the ones that succeeded -
    used to pick a true chronological min/max rather than assuming
    whichever token happens to appear first/second in the text is
    earlier/later. Implausible years (typos) are filtered out here too."""
    parsed = []
    for tok in tokens:
        try:
            d = _sane_date(dateparser.parse(tok, fuzzy=True, dayfirst=True).date())
            if d:
                parsed.append(d)
        except (ValueError, TypeError, OverflowError):
            continue
    return parsed

def transform_date_strict(value, extra):
    return transform_date(value, extra)

def transform_date_earliest(value, extra):
    if is_blank(value):
        return None
    if isinstance(value, (datetime, date)):
        return _sane_date(value.date() if isinstance(value, datetime) else value)

    text = str(value).strip()
    tokens = GENERIC_DATE_TOKEN_RE.findall(text)
    if tokens:
        parsed = _parse_date_tokens(tokens)
        if parsed:
            return min(parsed)

    try:
        return _sane_date(pd.to_datetime(text, dayfirst=True).date())
    except (ValueError, TypeError):
        return None

DUE_WORD_RE = re.compile(r'\bdue\b', re.IGNORECASE)

def _tokens_excluding_due(text, window=15):
    kept = []
    for m in GENERIC_DATE_TOKEN_RE.finditer(text):
        preceding = text[max(0, m.start() - window):m.start()]
        if DUE_WORD_RE.search(preceding):
            continue
        kept.append(m.group(0))
    return kept

def transform_date_later_ignore_due(value, extra):
    if is_blank(value):
        return None
    if isinstance(value, (datetime, date)):
        return _sane_date(value.date() if isinstance(value, datetime) else value)

    text = str(value).strip()
    all_tokens = GENERIC_DATE_TOKEN_RE.findall(text)
    if not all_tokens:
        try:
            return _sane_date(pd.to_datetime(text, dayfirst=True).date())
        except (ValueError, TypeError):
            return None

    kept_tokens = _tokens_excluding_due(text)
    if not kept_tokens:
        return None

    parsed = _parse_date_tokens(kept_tokens)
    return max(parsed) if parsed else None

APPLICATION_VALID_WITHDRAWN_RE = re.compile(r'\bwithdrawn\b', re.IGNORECASE)

def transform_application_valid_date(value, extra):
    if isinstance(value, str) and APPLICATION_VALID_WITHDRAWN_RE.search(value):
        before_text = APPLICATION_VALID_WITHDRAWN_RE.split(value, maxsplit=1)[0]
        return transform_date(before_text, extra)
    return transform_date(value, extra)

def transform_before_bracket(value, extra):
    return None if is_blank(value) else str(value).split("(")[0].strip()

MONTH_TO_NUM = {m.lower()[:3]: i for i, m in enumerate(
    ["", "January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"]) if m}

EXPECTED_DATE_TOKEN_RE = re.compile(
    r'\d{1,2}\s*/\s*\d{1,2}\s*/\s*\d{2,4}'
    r'|\d{4}-\d{1,2}-\d{1,2}'
    r'|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}'
    r'|\d{1,2}\s*-\s*[A-Za-z]{3,9}\s*-\s*\d{2,4}'
)
LATE_MONTH_RE  = re.compile(r'\blate\s+([A-Za-z]{3,9})\.?\s*(\d{4})?', re.IGNORECASE)
EARLY_MONTH_RE = re.compile(r'\bearly\s+([A-Za-z]{3,9})\.?\s*(\d{4})?', re.IGNORECASE)
MID_MONTH_RE   = re.compile(r'\bmid[\s-]*([A-Za-z]{3,9})\.?\s*(\d{4})?', re.IGNORECASE)
WC_RE          = re.compile(r'\bw\s*/?\s*c\.?\s*(.+)', re.IGNORECASE)
TWO_MONTH_SLASH_RE = re.compile(r'\b([A-Za-z]{3,9})\s*/\s*([A-Za-z]{3,9})\b', re.IGNORECASE)
N_WEEKS_RE     = re.compile(r'\b(\d+)\s*week', re.IGNORECASE)
N_WORKING_DAYS_RE = re.compile(r'\b(\d+)\s*working\s*day', re.IGNORECASE)
N_DAYS_RE      = re.compile(r'\b(\d+)\s*day', re.IGNORECASE)
DMY_WORDMONTH_RE = re.compile(r'\b(\d{1,2})\s+([A-Za-z]{3,10})\.?\s*(\d{4})?\b')

def _count_expected_date_tokens(text):
    return len(EXPECTED_DATE_TOKEN_RE.findall(text))

def _add_working_days(start, n):
    if is_blank(start):
        return None
    d = start
    added = 0
    guard = 0
    while added < n:
        d += timedelta(days=1)
        guard += 1
        if guard > 1000:
            raise RuntimeError(
                f"_add_working_days: safety limit hit (start={start!r}, n={n}) - "
                f"start is probably not a real date"
            )
        if d.weekday() < 5:
            added += 1
    return d

def _extract_single_date(text, notification_date):
    m = DMY_WORDMONTH_RE.search(text)
    if m:
        day, month_word, year_str = m.groups()
        month_num = MONTH_TO_NUM.get(month_word.lower()[:3])
        if month_num:
            year = int(year_str) if year_str else (notification_date.year if notification_date else None)
            if year:
                try:
                    return _sane_date(date(year, month_num, int(day)))
                except ValueError:
                    pass
    default_dt = datetime(notification_date.year, 1, 1) if notification_date else datetime(2000, 1, 1)
    try:
        parsed = dateparser.parse(text, fuzzy=True, dayfirst=True, default=default_dt)
        return _sane_date(parsed.date())
    except (ValueError, TypeError, OverflowError):
        return None

def resolve_expected_submission_date(raw, notification_date):
    if is_blank(raw):
        return None
    if isinstance(raw, (datetime, date)):
        return raw.date() if isinstance(raw, datetime) else raw
    text = str(raw).strip()

    if _count_expected_date_tokens(text) >= 2:
        tokens = EXPECTED_DATE_TOKEN_RE.findall(text)
        parsed = _parse_date_tokens(tokens)
        if parsed:
            return max(parsed)
        return text

    m = LATE_MONTH_RE.search(text)
    if m:
        month_num = MONTH_TO_NUM.get(m.group(1).lower()[:3])
        year = int(m.group(2)) if m.group(2) else (notification_date.year if notification_date else None)
        if month_num and year:
            return _sane_date(date(year, month_num, calendar.monthrange(year, month_num)[1]))

    m = EARLY_MONTH_RE.search(text)
    if m:
        month_num = MONTH_TO_NUM.get(m.group(1).lower()[:3])
        year = int(m.group(2)) if m.group(2) else (notification_date.year if notification_date else None)
        if month_num and year:
            return _sane_date(date(year, month_num, 1))

    m = MID_MONTH_RE.search(text)
    if m:
        month_num = MONTH_TO_NUM.get(m.group(1).lower()[:3])
        year = int(m.group(2)) if m.group(2) else (notification_date.year if notification_date else None)
        if month_num and year:
            return _sane_date(date(year, month_num, 15))

    m = WC_RE.search(text)
    if m:
        parsed = _extract_single_date(m.group(1), notification_date)
        if parsed:
            return parsed

    if TWO_MONTH_SLASH_RE.search(text) and not LATE_MONTH_RE.search(text) \
            and not MID_MONTH_RE.search(text) and not EARLY_MONTH_RE.search(text):
        if notification_date:
            return notification_date + timedelta(days=10)

    m = N_WEEKS_RE.search(text)
    if m and notification_date:
        return notification_date + timedelta(weeks=int(m.group(1)))

    m = N_WORKING_DAYS_RE.search(text)
    if m and notification_date:
        return _add_working_days(notification_date, int(m.group(1)))

    m = N_DAYS_RE.search(text)
    if m and notification_date:
        return notification_date + timedelta(days=int(m.group(1)))

    parsed = _extract_single_date(text, notification_date)
    if parsed:
        return parsed

    if notification_date:
        return _add_working_days(notification_date, 10)

    return text

HZ_REF_RE     = re.compile(r'HZ\s*ref\s*[.:]?\s*([^()]*)', re.IGNORECASE)
LPA_REF_RE    = re.compile(r'LPA\s*ref\s*[.:]?\s*([^()]*)', re.IGNORECASE)
RD_SUFFIX_RE  = re.compile(r'^(S62A/\S*?)RD$', re.IGNORECASE)
WITHDRAWN_DATE_RE = re.compile(
    r'WITHDRAWN\D{0,10}?('
    r'\d{1,2}\s*[/\-]\s*\d{1,2}\s*[/\-]\s*\d{2,4}'
    r'|\d{4}-\d{1,2}-\d{1,2}'
    r'|\d{1,2}\s+[A-Za-z]{3,9}\.?\s*\d{2,4}'
    r')',
    re.IGNORECASE,
)

def _clean_extracted_ref(text):
    """None if the extracted text is empty or just placeholder stars/
    whitespace (e.g. an LPA ref not yet assigned: '****')."""
    if text is None:
        return None
    t = text.strip()
    if not t or set(t) <= {"*", " "}:
        return None
    return t

def parse_ref(raw):
    if is_blank(raw):
        return {}
    text = str(raw).strip()
    has_star = "*" in text

    leading = text.split("(")[0].strip()

    rd_match = RD_SUFFIX_RE.match(leading)
    redetermined_ref = leading if rd_match else None
    leading_clean = rd_match.group(1) if rd_match else leading

    hz_match = HZ_REF_RE.search(text)
    lpa_match = LPA_REF_RE.search(text)
    hz_ref = _clean_extracted_ref(hz_match.group(1)) if hz_match else None
    lpa_ref = _clean_extracted_ref(lpa_match.group(1)) if lpa_match else None

    case_reference = redetermined_ref if redetermined_ref else (hz_ref if hz_ref else leading_clean) or None

    withdrawn_date = None
    wd_match = WITHDRAWN_DATE_RE.search(text)
    if wd_match:
        try:
            withdrawn_date = dateparser.parse(wd_match.group(1), fuzzy=True, dayfirst=True).date()
        except (ValueError, TypeError, OverflowError):
            withdrawn_date = None

    return {
        "case_reference": case_reference,
        "lpa_reference": lpa_ref,
        "notification_of_intent": has_star,
        "redetermined_ref": redetermined_ref,
        "withdrawn_date": withdrawn_date,
    }

def transform_case_reference(value, extra):
    return parse_ref(value).get("case_reference")

def transform_lpa_reference(value, extra):
    return parse_ref(value).get("lpa_reference")

def transform_redetermined_ref(value, extra):
    return parse_ref(value).get("redetermined_ref")

def transform_withdrawn_date(value, extra):
    return parse_ref(value).get("withdrawn_date")

def transform_notification_of_intent(value, extra):
    d = parse_ref(value)
    return "Yes" if d.get("notification_of_intent") else None

REF_YEAR_RE = re.compile(r'/(\d{2})/')

def normalize_ref_year(value):
    if not isinstance(value, str):
        return value
    return REF_YEAR_RE.sub(lambda m: f"/20{m.group(1)}/", value)

UK_POSTCODE_PATTERN = re.compile(r"\b([A-Za-z]{1,2}\d[A-Za-z\d]?\s*\d[A-Za-z]{2})\b")

KNOWN_COUNTIES_DISPLAY = ["Essex", "Hertfordshire", "Bristol", "East Sussex", "Lancashire", "Hampshire", "Cambridgeshire"]
KNOWN_COUNTIES = {c.lower() for c in KNOWN_COUNTIES_DISPLAY}
ADDRESS_NOISE_PHRASES = {"nearest postcode"}

TRAILING_COUNTY_RE = re.compile(
    r'[\s,.]+(' + '|'.join(re.escape(c) for c in sorted(KNOWN_COUNTIES_DISPLAY, key=len, reverse=True)) + r')\.?\s*$',
    re.IGNORECASE,
)

def strip_trailing_county(s):
    m = TRAILING_COUNTY_RE.search(s)
    if not m:
        return s, None
    remainder = s[:m.start()].strip()
    if not remainder:
        return s, None
    county_lower = m.group(1).lower()
    county_display = next(c for c in KNOWN_COUNTIES_DISPLAY if c.lower() == county_lower)
    return remainder, county_display

def is_county_token(s):
    return s.strip().rstrip(".").strip().lower() in KNOWN_COUNTIES

def split_address_parts(text):
    if is_blank(text):
        return {}
    text = str(text).strip().replace("\n", ", ")

    postcode = None
    match = UK_POSTCODE_PATTERN.search(text)
    if match:
        postcode = match.group(1).upper()
        text = text[:match.start()].rstrip(", ")

    parts = [p.strip() for p in text.split(",") if p.strip()]
    parts = [p for p in parts if p.lower() not in ADDRESS_NOISE_PHRASES]

    if not parts:
        return {"address1": None, "address2": None, "town": None, "county": None, "postcode": postcode}

    county = None
    if is_county_token(parts[-1]):
        county = next(c for c in KNOWN_COUNTIES_DISPLAY if c.lower() == parts[-1].rstrip(".").strip().lower())
        parts = parts[:-1]

    if not parts:
        return {"address1": None, "address2": None, "town": None, "county": county, "postcode": postcode}

    if len(parts) == 1:
        if county is None:
            remainder, found = strip_trailing_county(parts[0])
            if found:
                return {"address1": remainder, "address2": None, "town": None, "county": found, "postcode": postcode}
        return {"address1": parts[0], "address2": None, "town": None, "county": county, "postcode": postcode}

    town = parts[-1]
    if county is None:
        town_clean, found = strip_trailing_county(town)
        if found:
            town = town_clean
            county = found

    address_lines = parts[:-1]
    address1 = address_lines[0] if address_lines else None
    address2 = "; ".join(address_lines[1:]) if len(address_lines) > 1 else None

    return {"address1": address1, "address2": address2, "town": town or None,
            "county": county, "postcode": postcode}

def transform_address_part(value, extra):
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

AGENT_NAME_WORD_RE = re.compile(r"\b[A-Z][a-z'\-]+\b")
AGENT_TITLE_WORDS = {"mr", "mrs", "ms", "miss", "dr"}
AGENT_ORG_INDICATOR_WORDS = {"ltd", "llp", "limited", "plc", "group", "planning", "partners",
                              "associates", "consultants", "architects", "homes", "direct",
                              "design", "studio", "projects", "developments", "energy",
                              "structures", "estates", "environmental", "architectural",
                              "management", "holdings", "house"}
AGENT_GENERIC_WORDS = {"info", "admin", "office", "enquiries", "the applicant", "agent", "applicant"}
AGENT_TITLE_STRIP_RE = re.compile(r'^(?:Mr|Mrs|Ms|Miss|Dr)\.?\s*', re.IGNORECASE)
AGENT_TITLE_TRAILING_RE = re.compile(r'\s*\b(?:Mr|Mrs|Ms|Miss|Dr)\.?\s*$', re.IGNORECASE)
AGENT_EXACT_NAME_RE = re.compile(r"^([A-Z][a-z'\-]+)\s+([A-Z][a-z'\-]+)$")

def _agent_is_bare_name(text):
    m = AGENT_EXACT_NAME_RE.match(text.strip())
    if not m:
        return False
    return not any(w.lower() in AGENT_ORG_INDICATOR_WORDS for w in m.groups())

def parse_agent(raw):
    if is_blank(raw):
        return {}
    text = str(raw).strip()
    emails = EMAIL_PATTERN.findall(text)
    email_field = "; ".join(emails) if emails else None

    remainder = EMAIL_PATTERN.sub("", text)
    remainder = re.sub(r"[<>()]", " ", remainder)
    remainder = ", ".join(s.strip() for s in re.split(r"[;,]", remainder) if s.strip())
    remainder = re.sub(r"\s+", " ", remainder).strip(" ,;.")

    if not remainder:
        return {"org": None, "firsts": [], "lasts": [], "email": email_field}

    segments = [s.strip() for s in remainder.split(",") if s.strip()]
    if len(segments) >= 2:
        for i, seg in enumerate(segments):
            seg_stripped = AGENT_TITLE_STRIP_RE.sub("", seg)
            if _agent_is_bare_name(seg_stripped):
                m = AGENT_EXACT_NAME_RE.match(seg_stripped)
                first, last = m.groups()
                firsts, lasts = [first], [last]
                org = None
                other_idx = i + 1 if i + 1 < len(segments) else (i - 1 if i > 0 else None)
                if other_idx is not None:
                    other = segments[other_idx]
                    other_stripped = AGENT_TITLE_STRIP_RE.sub("", other)
                    if _agent_is_bare_name(other_stripped):
                        m2 = AGENT_EXACT_NAME_RE.match(other_stripped)
                        firsts.append(m2.group(1))
                        lasts.append(m2.group(2))
                    else:
                        org = other
                return {"org": org, "firsts": firsts, "lasts": lasts, "email": email_field}

    if _agent_is_bare_name(remainder):
        m = AGENT_EXACT_NAME_RE.match(remainder)
        return {"org": None, "firsts": [m.group(1)], "lasts": [m.group(2)], "email": email_field}

    words_with_titles = AGENT_NAME_WORD_RE.findall(remainder)
    words = [w for w in words_with_titles if w.lower().rstrip(".") not in AGENT_TITLE_WORDS]

    for i in range(len(words) - 2, -1, -1):
        first, last = words[i], words[i + 1]
        if first.lower() in AGENT_ORG_INDICATOR_WORDS or last.lower() in AGENT_ORG_INDICATOR_WORDS:
            continue
        idx = remainder.rfind(first)
        leftover = remainder[:idx].strip(" ,;.") if idx > 0 else ""
        leftover = AGENT_TITLE_TRAILING_RE.sub("", leftover).strip(" ,;.")

        firsts, lasts = [first], [last]
        org = None
        if leftover:
            if _agent_is_bare_name(leftover):
                m2 = AGENT_EXACT_NAME_RE.match(leftover)
                firsts.insert(0, m2.group(1))
                lasts.insert(0, m2.group(2))
            elif leftover.lower() not in AGENT_GENERIC_WORDS:
                org = leftover
        return {"org": org, "firsts": firsts, "lasts": lasts, "email": email_field}

    if remainder.lower() not in AGENT_GENERIC_WORDS and "@" not in remainder:
        return {"org": remainder, "firsts": [], "lasts": [], "email": email_field}
    return {"org": None, "firsts": [], "lasts": [], "email": email_field}

def transform_agent_parse(value, extra):
    parsed = parse_agent(value)
    if extra == "first":
        return "; ".join(parsed.get("firsts", [])) or None
    if extra == "last":
        return "; ".join(parsed.get("lasts", [])) or None
    return parsed.get(extra)

APPLICANT_TITLE_STRIP_RE = re.compile(r'^(?:Mr|Mrs|Ms|Miss|Dr|Sir)\.?\s+', re.IGNORECASE)
APPLICANT_TITLE_ONLY_RE = re.compile(r'^(?:mr|mrs|ms|miss|dr|sir)\.?$', re.IGNORECASE)
APPLICANT_COMPANY_KEYWORDS = {
    "ltd", "limited", "kimited", "llp", "plc", "estates", "estate",
    "homes", "home", "developments", "development", "properties", "property",
    "investments", "investment", "holdings", "group", "partnerships", "partnership",
    "trustees", "scheme", "school", "church", "capital", "management", "services",
    "solutions", "energy", "solar", "leisure", "architectural", "design", "studio",
    "procurement", "staging", "analytics", "asset", "unlimited", "creation",
    "company", "co", "pub", "inn", "hotel", "hotels", "house", "houses",
}
APPLICANT_NAME_WORD_RE = re.compile(r"^[A-Z][a-z'\-]*\.?$|^[A-Z]\.?$")

def _applicant_looks_like_company(text):
    lower = text.lower()
    return any(re.search(r'\b' + re.escape(kw) + r'\b', lower) for kw in APPLICANT_COMPANY_KEYWORDS)

def _applicant_is_name_word(w):
    return bool(APPLICANT_NAME_WORD_RE.match(w)) and w.lower().strip('.') not in APPLICANT_COMPANY_KEYWORDS

def _applicant_is_plausible_name(text):
    words = text.split()
    if not (2 <= len(words) <= 4):
        return False
    return all(_applicant_is_name_word(w) for w in words)

def _classify_applicant_segment(seg, org_parts, firsts, lasts):
    seg = seg.strip()
    if not seg or APPLICANT_TITLE_ONLY_RE.match(seg.rstrip('.')):
        return

    seg_notitle = APPLICANT_TITLE_STRIP_RE.sub('', seg).strip()

    if not _applicant_looks_like_company(seg) and _applicant_is_plausible_name(seg_notitle):
        words = seg_notitle.split()
        firsts.append(words[0]); lasts.append(words[-1])
        return

    m_title = APPLICANT_TITLE_STRIP_RE.match(seg)
    if m_title:
        remainder = re.sub(r'[()]', ' ', seg[m_title.end():]).strip()
        words = remainder.split()
        if len(words) >= 2 and _applicant_is_name_word(words[0]) and _applicant_is_name_word(words[1]):
            firsts.append(words[0]); lasts.append(words[1])
            rest = " ".join(words[2:]).strip()
            rest = re.sub(r'^(?:of|on|behalf)\b\.?\s*', '', rest, flags=re.IGNORECASE).strip(' ,')
            if rest:
                org_parts.append(rest)
            return

    m_behalf = re.search(r'\bon\s+behalf\s+of\b', seg, re.IGNORECASE)
    if m_behalf:
        before = seg[:m_behalf.start()].strip(' ,')
        after = seg[m_behalf.end():].strip(' ,()')
        before_words = APPLICANT_TITLE_STRIP_RE.sub('', before).split()
        if len(before_words) >= 2 and _applicant_is_name_word(before_words[0]) and _applicant_is_name_word(before_words[1]):
            firsts.append(before_words[0]); lasts.append(before_words[1])
        elif before.strip():
            org_parts.append(before.strip())
        if after:
            org_parts.append(after)
        return

    if '(' in seg:
        idx = seg.index('(')
        before = seg[:idx].strip()
        inside = seg[idx + 1:].rstrip(')').strip()
        before_words = APPLICANT_TITLE_STRIP_RE.sub('', before).split()
        if len(before_words) >= 2 and _applicant_is_name_word(before_words[0]) \
                and _applicant_is_name_word(before_words[1]) and _applicant_looks_like_company(inside):
            firsts.append(before_words[0]); lasts.append(before_words[1])
            if inside:
                org_parts.append(inside)
            return

    if _applicant_looks_like_company(seg):
        org_parts.append(seg.strip())
        return

    if seg.strip('.,() '):
        org_parts.append(seg.strip())

def parse_applicant(raw):
    if is_blank(raw):
        return {"org": None, "firsts": [], "lasts": []}
    text = str(raw).strip().replace("\n", " ").strip('"').strip()
    if text in ("?", ""):
        return {"org": None, "firsts": [], "lasts": []}
    text = re.sub(r'\b(Mr|Mrs|Ms|Miss|Dr|Sir)\s*&\s*(Mr|Mrs|Ms|Miss|Dr|Sir)\b', r'\1 and \2', text, flags=re.IGNORECASE)
    parts = re.split(r'\s*,\s*(?:and\s+)?|\s*;\s*|\s+and\s+', text, flags=re.IGNORECASE)
    parts = [p.strip() for p in parts if p.strip()]

    org_parts, firsts, lasts = [], [], []
    handled = [False] * len(parts)

    for i, seg in enumerate(parts):
        seg_notitle = APPLICANT_TITLE_STRIP_RE.sub('', seg).strip()
        seg_words = seg_notitle.split()
        is_lone_first_name = (
            len(seg_words) == 1
            and _applicant_is_name_word(seg_words[0])
            and not _applicant_looks_like_company(seg)
        )
        if not is_lone_first_name:
            continue
        for neighbour_idx in (i - 1, i + 1):
            if neighbour_idx < 0 or neighbour_idx >= len(parts) or handled[neighbour_idx]:
                continue
            neighbour = parts[neighbour_idx]
            neighbour_notitle = APPLICANT_TITLE_STRIP_RE.sub('', neighbour).strip()
            if _applicant_looks_like_company(neighbour) or not _applicant_is_plausible_name(neighbour_notitle):
                continue
            shared_last = neighbour_notitle.split()[-1]
            firsts.append(seg_words[0])
            lasts.append(shared_last)
            handled[i] = True
            break

    for i, seg in enumerate(parts):
        if handled[i]:
            continue
        _classify_applicant_segment(seg, org_parts, firsts, lasts)

    org = "; ".join(org_parts) if org_parts else None
    return {"org": org, "firsts": firsts, "lasts": lasts}

def transform_applicant_parse(value, extra):
    parsed = parse_applicant(value)
    if extra == "first":
        return "; ".join(parsed.get("firsts", [])) or None
    if extra == "last":
        return "; ".join(parsed.get("lasts", [])) or None
    if extra == "type":
        if parsed.get("org"):
            return "Organisation"
        if parsed.get("firsts"):
            return "Individual"
        return None
    return parsed.get(extra)

INSPECTOR_SPLIT_RE = re.compile(r'(?<!\d)/(?!\d)')
INSPECTOR_LEADING_AND_RE = re.compile(r'^(?:and|&)\s+', re.IGNORECASE)

def split_inspectors(raw):
    if is_blank(raw):
        return []
    if isinstance(raw, (datetime, date)):
        return []
    text = str(raw)
    text = re.sub(r'\bw/c\b', 'w\x00c', text, flags=re.IGNORECASE)
    parts = INSPECTOR_SPLIT_RE.split(text)
    cleaned = []
    for p in parts:
        p = p.replace('\x00', '/').strip()
        p = INSPECTOR_LEADING_AND_RE.sub('', p).strip()
        if not p or set(p) <= {"?", " "}:
            continue
        cleaned.append(p)
    return cleaned

def transform_inspector_split(value, extra):
    parts = split_inspectors(value)
    idx = int(extra) - 1
    if idx >= len(parts):
        return None
    if extra == "3" and len(parts) > 3:
        return "; ".join(parts[2:])
    return parts[idx]


BAND_TOKEN_RE = re.compile(r'^B(?:AND)?\s*(\d+)$', re.IGNORECASE)

SPECIALISM_CODE_MAP = {
    "RE": "Renewables",
    "GA": "General",
    "HG": "Historic Heritage",
}

def split_specialism_band(text):
    if is_blank(text):
        return {}
    text = str(text).strip()
    tokens = text.split(None, 1)
    band = None
    specialism = text
    if tokens:
        m = BAND_TOKEN_RE.match(tokens[0])
        if m:
            band = f"Band {m.group(1)}"
            specialism = tokens[1].strip() if len(tokens) > 1 else None
    if specialism:
        specialism = SPECIALISM_CODE_MAP.get(specialism.strip().upper(), specialism)
    return {"band": band, "specialism": specialism}

def transform_specialism_band(value, extra):
    return split_specialism_band(value).get(extra)


PRESS_DATE_TOKEN_RE = re.compile(
    r'\d{1,2}\s*[/\-]\s*\d{1,2}\s*[/\-]\s*\d{2,4}'
    r'|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}'
    r'|\d{1,2}\s*-\s*[A-Za-z]{3,9}\s*-\s*\d{2,4}'
)
PRESS_TMP_REF_RE = re.compile(r'\bT[MN]P\.?\s*(?:ref|no)?\.?\s*:?\s*#?\s*(\d{4,})\b', re.IGNORECASE)
PRESS_BOTH_PAPERS_RE = re.compile(r'\bboth\s+papers?\b', re.IGNORECASE)

def _extract_press_date(text):
    m = PRESS_DATE_TOKEN_RE.search(text)
    if not m:
        return None
    try:
        return dateparser.parse(m.group(0), fuzzy=True, dayfirst=True).date()
    except (ValueError, TypeError, OverflowError):
        return None

def _extract_press_placed(text):
    lower = text.lower()
    papers = []
    if "walden" in lower:
        papers.append("Saffron Walden Reporter")
    if "dunmow" in lower:
        papers.append("Dunmow Broadcast")
    if papers:
        return " and ".join(papers)
    if PRESS_BOTH_PAPERS_RE.search(text):
        return "Saffron Walden Reporter and Dunmow Broadcast"
    return None

def _extract_press_reference(text):
    m = PRESS_TMP_REF_RE.search(text)
    if m:
        return m.group(0).strip()
    stripped = text.strip()
    if stripped.isdigit():
        return stripped
    return None

def split_press_notice(value):
    if is_blank(value):
        return {}
    if isinstance(value, (datetime, date)):
        return {"date": value.date() if isinstance(value, datetime) else value,
                "placed": None, "reference": None}
    text = str(value).strip()
    if text.lower() in ("na", "n/a"):
        return {"date": None, "placed": None, "reference": None}
    return {
        "date": _extract_press_date(text),
        "placed": _extract_press_placed(text),
        "reference": _extract_press_reference(text),
    }

def transform_press_notice(value, extra):
    return split_press_notice(value).get(extra)

GBP_AMOUNT_RE = re.compile(r'£\s?([\d,]+(?:\.\d+)?)')

def extract_gbp_amount(text):
    if not text:
        return None
    m = GBP_AMOUNT_RE.search(str(text))
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return None


def transform_eia_outcome(value, extra):
    return transform_direct(value, extra)

EIA_RECEIVED_RE = re.compile(
    r"(?:subm\w*|receiv\w*|rec'?d)\D{0,15}?"
    r"(\d{1,2}\s*[/\-]\s*\d{1,2}\s*[/\-]\s*\d{2,4}|\d{1,2}\s+[A-Za-z]+\s+\d{2,4})",
    re.IGNORECASE,
)

def transform_eia_received_date(value, extra):
    if is_blank(value):
        return None
    if isinstance(value, (datetime, date)):
        return value.date() if isinstance(value, datetime) else value
    match = EIA_RECEIVED_RE.search(str(value))
    if not match:
        return None
    try:
        return dateparser.parse(match.group(1), fuzzy=True, dayfirst=True).date()
    except (ValueError, TypeError, OverflowError):
        return None

EIA_NEGATIVE_OUTCOME_RE = re.compile(
    r'\b(?:neg(?:ative)?|not\s+required|no\s+screening\s+required|not\s+eia\s+development)\b',
    re.IGNORECASE)
EIA_DATE_TOKEN_RE = re.compile(
    r'\b\d{1,2}\s*/\s*\d{1,2}\s*/\s*\d{2,4}\b'
    r'|\b\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}\b'
)

def _normalize_eia_date_token(tok):
    try:
        return dateparser.parse(tok, fuzzy=True, dayfirst=True).strftime("%d/%m/%Y")
    except (ValueError, TypeError, OverflowError):
        return tok

def resolve_eia_screening_and_outcome(raw_screening, raw_outcome):
    screening_text = raw_screening if isinstance(raw_screening, str) else (
        raw_screening.strftime("%d/%m/%Y") if isinstance(raw_screening, (datetime, date)) else "")
    outcome_text = raw_outcome if isinstance(raw_outcome, str) else ""

    combined = f"{screening_text} {outcome_text}".strip()
    if not combined or combined.strip().lower() in ("na", "n/a"):
        return {"screening": None, "outcome": None, "note": None}

    dates_found = []
    if isinstance(raw_screening, (datetime, date)):
        dates_found.append(raw_screening.strftime("%d/%m/%Y"))
    elif isinstance(raw_screening, str):
        dates_found.extend(_normalize_eia_date_token(t) for t in EIA_DATE_TOKEN_RE.findall(raw_screening))

    clean_outcome = None
    if outcome_text:
        dates_found.extend(_normalize_eia_date_token(t) for t in EIA_DATE_TOKEN_RE.findall(outcome_text))
        stripped = EIA_DATE_TOKEN_RE.sub("", outcome_text)
        stripped = re.sub(r'\s{2,}', ' ', stripped)
        stripped = re.sub(r'\s+([.,])', r'\1', stripped)
        clean_outcome = stripped.strip(" .,") or None

    if EIA_NEGATIVE_OUTCOME_RE.search(combined):
        screening_flag = "No"
    elif isinstance(raw_screening, (datetime, date)):
        screening_flag = "Yes"
    elif isinstance(raw_screening, str) and EIA_DATE_TOKEN_RE.search(raw_screening):
        screening_flag = "Yes"
    else:
        screening_flag = "No"

    seen = set()
    dates_unique = [d for d in dates_found if not (d in seen or seen.add(d))]
    note = f"EIA Screening: {'; '.join(dates_unique)}" if dates_unique else None

    return {"screening": screening_flag, "outcome": clean_outcome, "note": note}


CUSTOMER_NUMBER_RE = re.compile(r'\b(\d{6})\b')

def transform_customer_number(value, extra):
    if is_blank(value):
        return None
    match = CUSTOMER_NUMBER_RE.search(str(value))
    return match.group(1) if match else None


def transform_fee_amount(value, extra):
    if is_blank(value):
        return None
    if isinstance(value, (int, float)):
        return value
    match = re.search(r'[\d,]+(?:\.\d+)?', str(value).replace('£', ''))
    if not match:
        return None
    try:
        return float(match.group(0).replace(",", ""))
    except ValueError:
        return None


SITE_VISIT_TYPE_RE = re.compile(r"\b(ARSV|USV)\b", re.IGNORECASE)

def transform_site_visit_type(value, extra):
    if is_blank(value):
        return None
    if isinstance(value, (datetime, date)):
        return None
    match = SITE_VISIT_TYPE_RE.search(str(value))
    return match.group(1).upper() if match else None

SITE_VISIT_TYPE_PREFIX_RE = re.compile(r"^(ARSV|USV)\s*", re.IGNORECASE)
SITE_VISIT_DMY_RE = re.compile(r'\b(\d{1,2})\s+([A-Za-z]{3,10})\.?\s*(\d{4})\b')
SITE_VISIT_DATE_TOKEN_RE = re.compile(
    r'\d{1,2}\s*/\s*\d{1,2}\s*/\s*\d{2,4}'
    r'|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}'
)
SITE_VISIT_TIME_RE = re.compile(r'\b(\d{1,2})[.:](\d{2})\s*(am|pm)?\b', re.IGNORECASE)

def _extract_site_visit_time(text):
    matches = list(SITE_VISIT_TIME_RE.finditer(text))
    if not matches:
        return None
    hour, minute, ampm = int(matches[0].group(1)), int(matches[0].group(2)), matches[0].group(3)
    if not ampm and len(matches) > 1 and matches[1].group(3):
        ampm = matches[1].group(3)
    if ampm:
        ampm = ampm.lower()
        if ampm == "pm" and hour != 12:
            hour += 12
        elif ampm == "am" and hour == 12:
            hour = 0
    if hour > 23 or minute > 59:
        return None
    return hour, minute

def _extract_site_visit_date(text):
    m = SITE_VISIT_DMY_RE.search(text)
    if m:
        day, month_word, year = m.groups()
        month_num = MONTH_TO_NUM.get(month_word.lower()[:3])
        if month_num:
            try:
                return _sane_date(date(int(year), month_num, int(day)))
            except ValueError:
                return None
    try:
        return _sane_date(dateparser.parse(text, fuzzy=True, dayfirst=True).date())
    except (ValueError, TypeError, OverflowError):
        return None

def transform_site_visit_date(value, extra):
    if is_blank(value):
        return None
    if isinstance(value, (datetime, date)):
        return value

    text = str(value).strip()
    text_clean = SITE_VISIT_TYPE_PREFIX_RE.sub("", text).strip()
    if not text_clean:
        return None

    date_tokens = SITE_VISIT_DATE_TOKEN_RE.findall(text_clean)
    if len(date_tokens) >= 2:
        parsed = _parse_date_tokens(date_tokens)
        if parsed:
            return max(parsed)
        return text_clean

    d = _extract_site_visit_date(text_clean)
    if not d:
        return text_clean

    t = _extract_site_visit_time(text_clean)
    if t:
        return datetime(d.year, d.month, d.day, t[0], t[1])
    return d


def transform_cil_liable(value, extra):
    if is_blank(value):
        return None
    text = str(value).strip().lower()
    if text in ("na", "n/a", "??"):
        return None
    if "not" in text or "no cil" in text or re.search(r"\bnot\b", text):
        return "No"
    if "liable" in text:
        return "Yes"
    return None

def transform_cil_amount(value, extra):
    if is_blank(value):
        return None
    if isinstance(value, (int, float)):
        return value
    text = str(value).strip()
    if text.upper() in ("NA", "N/A"):
        return None
    match = re.search(r'[\d,]+(?:\.\d+)?', text.replace('£', ''))
    if not match:
        return None
    try:
        return float(match.group(0).replace(",", ""))
    except ValueError:
        return None

TRANSFORM_FUNCTIONS = {
    "direct": transform_direct,
    "date": transform_date,
    "date_strict": transform_date_strict,
    "date_earliest": transform_date_earliest,
    "date_later_ignore_due": transform_date_later_ignore_due,
    "application_valid_date": transform_application_valid_date,
    "before_bracket": transform_before_bracket,
    "address_part": transform_address_part,
    "grant_refuse": transform_grant_refuse,
    "agent_parse": transform_agent_parse,
    "applicant_parse": transform_applicant_parse,
    "inspector_split": transform_inspector_split,
    "site_visit_type": transform_site_visit_type,
    "site_visit_date": transform_site_visit_date,
    "cil_liable": transform_cil_liable,
    "cil_amount": transform_cil_amount,
    "specialism_band": transform_specialism_band,
    "press_notice": transform_press_notice,
    "eia_outcome": transform_eia_outcome,
    "eia_received_date": transform_eia_received_date,
    "customer_number": transform_customer_number,
    "fee_amount": transform_fee_amount,
    "case_reference": transform_case_reference,
    "lpa_reference": transform_lpa_reference,
    "redetermined_ref": transform_redetermined_ref,
    "withdrawn_date": transform_withdrawn_date,
    "notification_of_intent": transform_notification_of_intent,
}

print(f"Block 3 done - {len(TRANSFORM_FUNCTIONS)} transform functions ready")


SHEET_NAMES = ["Pre-application - DONE", "Application (Major)", "Application (Non Major)"]

def read_source_rows(sheet_name):
    df = pd.read_excel(SOURCE_FILE, sheet_name=sheet_name, header=0)
    df = df.dropna(how="all")
    first_col = df.columns[0]
    df = df[df[first_col].notna()]
    return df

_test_sheet = SHEET_NAMES[1]
df_test = read_source_rows(_test_sheet)

print(f"Block 4 done - {_test_sheet}: loaded {len(df_test)} rows")
print(df_test.head())


AUDIT_HIGHLIGHT_FILL = PatternFill(start_color="FFFFFF00", end_color="FFFFFF00", fill_type="solid")

def build_output_rows(df, mapping, constants, sheet_label):
    output_rows      = []
    audit_entries    = []
    audit_highlights = []
    for row_index, (_, source_row) in enumerate(df.iterrows()):
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
            elif transform_name not in ("address_part", "site_visit_type", "site_visit_date",
                                         "specialism_band", "press_notice",
                                         "eia_received_date", "customer_number",
                                         "lpa_reference", "redetermined_ref",
                                         "notification_of_intent", "agent_parse", "applicant_parse",
                                         "inspector_split", "date_strict", "withdrawn_date",
                                         "application_valid_date", "date_earliest",
                                         "date_later_ignore_due", "cil_amount")\
                    and not is_blank(source_value):

                audit_entries.append((row_result.get("Case reference"), sheet_label,
                                       template_column, f"could not convert value: {source_value!r}"))

                if not row_result.get(template_column):
                    row_result[template_column] = source_value
                audit_highlights.append((row_index, template_column))

        press_notice_raw = row_result.pop("_press_notice_raw", None)
        if not row_result.get("Press notice cost"):
            cost = extract_gbp_amount(press_notice_raw)
            if cost is not None:
                row_result["Press notice cost"] = cost

        notification_date = row_result.get("Notification received date")
        if "Expected submission date" in row_result:
            row_result["Expected submission date"] = resolve_expected_submission_date(
                row_result.get("Expected submission date"),
                notification_date,
            )
        elif notification_date:
            row_result["Expected submission date"] = _add_working_days(notification_date, 10)

        redetermined_ref = row_result.pop("_redetermined_ref", None)
        if redetermined_ref:
            note_addition = f"REDETERMINED: {redetermined_ref}"
            existing_notes = row_result.get("Notes")
            row_result["Notes"] = f"{existing_notes}; {note_addition}" if existing_notes else note_addition

        for ref_field in ("Case reference", "LPA reference", "Pre-app reference"):
            if row_result.get(ref_field):
                row_result[ref_field] = normalize_ref_year(row_result[ref_field])

        if "EIA screening" in row_result or "EIA screening outcome" in row_result:
            eia = resolve_eia_screening_and_outcome(
                row_result.get("EIA screening"),
                row_result.get("EIA screening outcome"),
            )
            row_result["EIA screening"] = eia["screening"]
            row_result["EIA screening outcome"] = eia["outcome"]
            if eia["note"]:
                existing_notes = row_result.get("Notes")
                row_result["Notes"] = f"{existing_notes}; {eia['note']}" if existing_notes else eia["note"]

        inspector1_date = row_result.get("Date Inspector (1) allocated")
        if inspector1_date:
            if row_result.get("Inspector 2") and not row_result.get("Date Inspector (2) allocated"):
                row_result["Date Inspector (2) allocated"] = inspector1_date
            if row_result.get("Inspector 3") and not row_result.get("Date Inspector (3) allocated"):
                row_result["Date Inspector (3) allocated"] = inspector1_date

        if row_result.get("Pre-application fee due"):
            row_result["Pre-application fee required"] = "Yes"

        if row_result.get("Application fee amount"):
            row_result["Application fee"] = "Yes"

        output_rows.append(row_result)
    return output_rows, audit_entries, audit_highlights

mapping_test, constants_test, issues_test = build_mapping_for_sheet(_test_sheet, df_test.columns.tolist())
one_row_result, one_row_audit, one_row_highlights = build_output_rows(df_test.head(1), mapping_test, constants_test, _test_sheet)

print(f"Block 5 test done - {_test_sheet}: {len(mapping_test)} mapped columns, {len(issues_test)} config issues")
print("Single row result:")
for k, v in one_row_result[0].items():
    print(f"  {k}: {v!r}")
print(f"  ({len(one_row_audit)} audit flags for this row)")



all_rows        = []
all_audit       = []
all_config_issues = []
unmapped_report = []
all_audit_highlights = []

for sheet_name in SHEET_NAMES:
    df = read_source_rows(sheet_name)
    mapping, constants, config_issues = build_mapping_for_sheet(sheet_name, df.columns.tolist())
    all_config_issues.extend(config_issues)

    used_columns = {source_col for _, source_col, _, _ in mapping}
    for col in df.columns:
        if col not in used_columns:
            unmapped_report.append({"Sheet": sheet_name, "Column": col})

    rows, audit, highlights = build_output_rows(df, mapping, constants, sheet_name)
    row_offset = len(all_rows)
    all_audit_highlights.extend((row_offset + local_index, col) for local_index, col in highlights)
    all_rows.extend(rows)
    all_audit.extend(audit)
    print(f"  {sheet_name}: {len(mapping)} mapped columns, {len(config_issues)} config issues, {len(df)} rows")

print(f"Block 5b done - {len(all_rows)} rows built, {len(all_audit)} audit flags, {len(all_config_issues)} config issues")


DESTINATION_FIELD_RENAMES = {
    "Pre-application or application": "Application phase",
}

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
wb = openpyxl.load_workbook(TEMPLATE_FILE)
ws = wb[TEMPLATE_SHEET]

column_lookup = {}
for col_num in range(1, ws.max_column + 1):
    header_value = ws.cell(row=TEMPLATE_HEADER_ROW, column=col_num).value
    if header_value:
        column_lookup[str(header_value).strip()] = col_num

_template_max_row = ws.max_row
_template_max_col = ws.max_column
_dummy_cells_found = 0
for r in range(TEMPLATE_FIRST_DATA_ROW, _template_max_row + 1):
    for c in range(1, _template_max_col + 1):
        cell = ws.cell(row=r, column=c)
        if cell.value is not None:
            _dummy_cells_found += 1
            if _dummy_cells_found <= 10:
                print(f"  clearing pre-existing value at row {r}, col {c} "
                      f"(header: {ws.cell(row=TEMPLATE_HEADER_ROW, column=c).value!r}): {cell.value!r}")
        cell.value = None
print(f"Cleared {_dummy_cells_found} pre-existing value(s) from Template rows "
      f"{TEMPLATE_FIRST_DATA_ROW}-{_template_max_row} before writing migrated data")

EXTRA_OUTPUT_COLUMNS = ["Received notification of intent"]
for extra_col_name in EXTRA_OUTPUT_COLUMNS:
    if extra_col_name not in column_lookup:
        new_col_num = ws.max_column + 1
        ws.cell(row=TEMPLATE_HEADER_ROW, column=new_col_num).value = extra_col_name
        column_lookup[extra_col_name] = new_col_num

_missing_fields = set()
excel_row = TEMPLATE_FIRST_DATA_ROW
for row_result in all_rows:
    for col_num in column_lookup.values():
        ws.cell(row=excel_row, column=col_num).value = None
    for template_column, value in row_result.items():
        template_column = DESTINATION_FIELD_RENAMES.get(template_column, template_column)
        if template_column not in column_lookup:
            _missing_fields.add(template_column)
            print(f"WARNING: '{template_column}' not found in Template headers - skipped")
            continue
        col_num = column_lookup[template_column]
        cell = ws.cell(row=excel_row, column=col_num)
        cell.value = value
        existing_font = cell.font
        cell.font = Font(
            name=existing_font.name,
            size=existing_font.size,
            bold=existing_font.bold,
            italic=existing_font.italic,
            color="FF000000",
        )
        if isinstance(value, datetime) and (value.hour or value.minute):
            cell.number_format = "DD/MM/YYYY HH:MM"
        elif isinstance(value, date):
            cell.number_format = "DD/MM/YYYY"
        else:
            cell.number_format = "General"
    excel_row += 1

highlighted = 0
for row_index, template_column in all_audit_highlights:
    template_column = DESTINATION_FIELD_RENAMES.get(template_column, template_column)
    if template_column not in column_lookup:
        continue
    excel_row = TEMPLATE_FIRST_DATA_ROW + row_index
    col_num = column_lookup[template_column]
    ws.cell(row=excel_row, column=col_num).fill = AUDIT_HIGHLIGHT_FILL
    highlighted += 1

wb.save(OUTPUT_FILE)
print(f"Block 6 done - wrote {len(all_rows)} rows to {OUTPUT_FILE}")
print(f"  {highlighted} cell(s) highlighted yellow for manual review")

if _missing_fields:
    print(f"\n{len(_missing_fields)} unmatched Field name(s) - compare against real Template headers below")
    print("Unmatched Field names from the Lookup sheet:")
    for f in sorted(_missing_fields):
        print(f"  {f!r}")
    print("Real Template headers:")
    for h in sorted(column_lookup):
        print(f"  {h!r}")


os.makedirs(os.path.dirname(AUDIT_LOG_FILE), exist_ok=True)

with open(AUDIT_LOG_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["Case reference", "Source sheet", "Template column", "Issue"])
    writer.writerows(all_audit)
print(f"Audit log:               {AUDIT_LOG_FILE} ({len(all_audit)} entries)")

with open(UNMAPPED_COLS_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["Sheet", "Column"])
    writer.writeheader()
    writer.writerows(unmapped_report)
print(f"Unmapped columns report:  {UNMAPPED_COLS_FILE} ({len(unmapped_report)} entries)")

with open(MAPPING_ISSUES_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["Sheet", "Field", "Source column (from Lookup sheet)", "Issue"])
    writer.writerows(all_config_issues)
print(f"Mapping config issues:    {MAPPING_ISSUES_FILE} ({len(all_config_issues)} entries)")