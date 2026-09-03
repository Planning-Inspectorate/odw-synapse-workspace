import pandas as pd

DISCOVERY_FILE = "/Users/nisalihalwathura/PINS/ODW-Service/odw-synapse-workspace/S62aMigration/csv_and_xlsx_files/S62A_Column_mapping_for_TEMPLATE.xlsx"
DISCOVERY_SHEET = "Lookup"
OUTPUT_CSV = "/Users/nisalihalwathura/PINS/ODW-Service/odw-synapse-workspace/S62aMigration/outputs/horizon_field_mapping.csv"

# The revised discovery sheet uses fixed columns:
# B = master legacy template field
# H = Horizon column name and source sheet/file
# I = logical conditions for extracting the Horizon value
horizon_df = pd.read_excel(
    DISCOVERY_FILE,
    sheet_name=DISCOVERY_SHEET,
    usecols="B,H,I",
    dtype=str,
).fillna("")

# Keep these output names compatible with the downstream Horizon mapper.
horizon_df.columns = ["Field", "Source field", "Conditions"]

# Ignore completely empty rows, but retain rows where the source or condition
# still needs review.
horizon_df = horizon_df.loc[
    horizon_df["Field"].str.strip().ne("")
].copy()

horizon_df.to_csv(OUTPUT_CSV, index=False)
print(f"Wrote {len(horizon_df)} Horizon mapping rows to {OUTPUT_CSV}")