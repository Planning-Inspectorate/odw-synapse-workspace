import pandas as pd

DISCOVERY_FILE = "/Users/nisalihalwathura/PINS/ODW-Service/odw-synapse-workspace/S62aMigration/csv_and_xlsx_files/S62A-Data-Discovery.xlsx"
DISCOVERY_SHEET = "BO Crown Comparison - USE THIS"
OUTPUT_CSV = "/Users/nisalihalwathura/PINS/ODW-Service/odw-synapse-workspace/S62aMigration/outputs/spreadsheet_field_mapping.csv"

df = pd.read_excel(DISCOVERY_FILE, sheet_name=DISCOVERY_SHEET)


def find_column(columns, predicate):
    """Find a column by a predicate on its normalized (lowercased, stripped)
    name, since the real headers can have extra text, e.g. 'Source field
    (Horizon / Spreadsheet)' rather than a plain 'Source field'."""
    for col in columns:
        if predicate(str(col).strip().lower()):
            return col
    raise KeyError("No matching column found")


field_col = find_column(df.columns, lambda c: c == "field")
source_col = find_column(df.columns, lambda c: c == "source")
source_field_col = find_column(df.columns, lambda c: c.startswith("source field"))

# Keep only rows where Source mentions Spreadsheet - covers both a plain
# "Spreadsheet" and combined sources like "Horizon/Spreadsheet".
spreadsheet_mask = df[source_col].str.contains("Spreadsheet", case=False, na=False)
spreadsheet_df = df.loc[spreadsheet_mask, [field_col, source_col, source_field_col]].copy()
spreadsheet_df.columns = ["Field", "Source", "Source field"]


def extract_spreadsheet_field(source, source_field):
    """Source field can list one name per source in Source, separated by
    '/', in the same order (e.g. Source='Horizon/Spreadsheet' pairs with
    Source field='Application description / Development description').
    Pick out only the name that lines up with Spreadsheet's position in Source.
    If there's no '/', the single name already belongs to Spreadsheet."""
    if pd.isna(source_field):
        return source_field

    source_field = str(source_field)
    if "/" not in source_field:
        return source_field.strip()

    source_parts = [p.strip().lower() for p in str(source).split("/")]
    field_parts = [p.strip() for p in source_field.split("/")]

    # find the position of "Spreadsheet" in the Source parts and return the corresponding Source field part.
    for i, part in enumerate(source_parts):
        if "spreadsheet" in part and i < len(field_parts):
            return field_parts[i]

    # Fallback if the positions couldn't be matched up 1:1.
    return field_parts[0]


spreadsheet_df["Source field"] = spreadsheet_df.apply(
    lambda row: extract_spreadsheet_field(row["Source"], row["Source field"]), axis=1
)

spreadsheet_df.to_csv(OUTPUT_CSV, index=False)
print(f"Wrote {len(spreadsheet_df)} Spreadsheet rows to {OUTPUT_CSV}")