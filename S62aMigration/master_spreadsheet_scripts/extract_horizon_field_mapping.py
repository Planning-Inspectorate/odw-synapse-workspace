import pandas as pd

DISCOVERY_FILE = "S62A-Data-Discovery.xlsx"
DISCOVERY_SHEET = "BO Crown Comparison - USE THIS"
OUTPUT_CSV = "output/horizon_field_mapping.csv"

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

# Keep only rows where Source mentions Horizon - covers both a plain
# "Horizon" and combined sources like "Horizon/Spreadsheet".
horizon_mask = df[source_col].str.contains("Horizon", case=False, na=False)
horizon_df = df.loc[horizon_mask, [field_col, source_col, source_field_col]].copy()
horizon_df.columns = ["Field", "Source", "Source field"]


def extract_horizon_field(source, source_field):
    """Source field can list one name per source in Source, separated by
    '/', in the same order (e.g. Source='Horizon/Spreadsheet' pairs with
    Source field='Application description / Development description').
    Pick out only the name that lines up with Horizon's position in Source.
    If there's no '/', the single name already belongs to Horizon."""
    if pd.isna(source_field):
        return source_field

    source_field = str(source_field)
    if "/" not in source_field:
        return source_field.strip()

    source_parts = [p.strip().lower() for p in str(source).split("/")]
    field_parts = [p.strip() for p in source_field.split("/")]

    for i, part in enumerate(source_parts):
        if "horizon" in part and i < len(field_parts):
            return field_parts[i]

    # Fallback if the positions couldn't be matched up 1:1.
    return field_parts[0]


horizon_df["Source field"] = horizon_df.apply(
    lambda row: extract_horizon_field(row["Source"], row["Source field"]), axis=1
)

horizon_df.to_csv(OUTPUT_CSV, index=False)
print(f"Wrote {len(horizon_df)} Horizon rows to {OUTPUT_CSV}")

