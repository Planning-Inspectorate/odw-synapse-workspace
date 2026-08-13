import shutil
import pandas as pd

from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Font, Alignment
from openpyxl.utils import get_column_letter


HORIZON_FILE = (
    "/Users/nisalihalwathura/PINS/ODW-Service/odw-synapse-workspace/S62aMigration/outputs/MASTER LEGACY cases S62A - with Horizon data.xlsx"
)

SPREADSHEET_FILE = (
    "/Users/nisalihalwathura/PINS/ODW-Service/odw-synapse-workspace/S62aMigration/outputs/S62A_All_Sheets_migrated.xlsx"
)

SHEET_NAME = "Template"

KEY_COL = "Case reference"

HEADER_ROW = 2

OUTPUT_FILE = (
    "/Users/nisalihalwathura/PINS/ODW-Service/"
    "odw-synapse-workspace/S62aMigration/outputs/"
    "S62A_Horizon_vs_Spreadsheet_comparison.xlsx"
)

# colours/formatting

HEADER_FILL = PatternFill(
    start_color="D9D9D9",
    end_color="D9D9D9",
    fill_type="solid"
)

# Horizon vs Spreadsheet mismatch
MISMATCH_FILL = PatternFill(
    start_color="FFC7CE",
    end_color="FFC7CE",
    fill_type="solid"
)

# Multiple conflicting values within ONE source
CONFLICT_FILL = PatternFill(
    start_color="FFF2CC",
    end_color="FFF2CC",
    fill_type="solid"
)

BOLD_FONT = Font(bold=True)

def norm(value):
    """
    Normalise a value for comparison.

    This does NOT alter the value stored in Excel.
    It is only used to determine whether values are equal.
    """

    if value is None:
        return ""

    if pd.isna(value):
        return ""

    return str(value).strip()


def is_blank(value):
    return norm(value) == ""


def load_source(path, source_name):
    """
    Load a source and consolidate duplicate Case references.

    Returns:

        records
        conflicts

    records:
        {
            "12345": {
                "Case reference": "12345",
                "Status": "Open",
                ...
            }
        }

    conflicts:
        [
            {
                "Case reference": "12345",
                "Source": "Horizon",
                "Field": "Status",
                "Values": "Open / Closed"
            }
        ]
    """

    df = pd.read_excel(
        path,
        sheet_name=SHEET_NAME,
        header=HEADER_ROW - 1
    )

    # Remove rows without a case reference
    df = df.dropna(subset=[KEY_COL])

    # Normalise case references
    df[KEY_COL] = (
        df[KEY_COL]
        .astype(str)
        .str.strip()
    )

    records = {}
    conflicts = []

    for case_reference, group in df.groupby(
        KEY_COL,
        sort=False
    ):

        combined = {}

        # Keep case reference
        combined[KEY_COL] = case_reference

        for column in df.columns:

            if column == KEY_COL:
                continue

            # Get all non-blank values for this case/field

            values = []

            for value in group[column]:

                normalised = norm(value)

                if normalised != "":
                    values.append(
                        (value, normalised)
                    )

            if not values:

                combined[column] = None
                continue

            unique_values = []
            seen = set()

            for original_value, normalised_value in values:

                if normalised_value not in seen:

                    seen.add(normalised_value)

                    unique_values.append(
                        (original_value, normalised_value)
                    )


            if len(unique_values) == 1:

                combined[column] = (
                    unique_values[0][0]
                )

            else:

                display_values = [
                    item[1]
                    for item in unique_values
                ]

                combined[column] = (
                    " / ".join(display_values)
                )

                conflicts.append({
                    "Case reference": case_reference,
                    "Source": source_name,
                    "Field": column,
                    "Values": " / ".join(
                        display_values
                    ),
                    "Number of different values": len(
                        unique_values
                    )
                })

        records[case_reference] = combined

    return records, conflicts


def get_fields(horizon_data, spreadsheet_data):

    horizon_fields = []

    if horizon_data:
        first_horizon_record = next(
            iter(horizon_data.values())
        )
        horizon_fields = list(
            first_horizon_record.keys()
        )

    spreadsheet_fields = []

    if spreadsheet_data:
        first_spreadsheet_record = next(
            iter(spreadsheet_data.values())
        )
        spreadsheet_fields = list(
            first_spreadsheet_record.keys()
        )

    fields = []

    # Horizon fields first
    for field in horizon_fields:
        if field != KEY_COL:
            fields.append(field)

    # Spreadsheet-only fields afterwards
    for field in spreadsheet_fields:
        if field != KEY_COL and field not in fields:
            fields.append(field)

    return fields


def prepare_output_workbook():

    # Copy the spreadsheet workbook first.
    # This preserves the original workbook.
    shutil.copy2(
        SPREADSHEET_FILE,
        OUTPUT_FILE
    )

    wb = load_workbook(
        OUTPUT_FILE
    )

    # Remove previous output sheets if they exist

    if "Comparison" in wb.sheetnames:
        del wb["Comparison"]

    if "Data Quality Issues" in wb.sheetnames:
        del wb["Data Quality Issues"]


    # copy exisiting template sheet

    template_ws = wb[SHEET_NAME]

    comparison_ws = wb.copy_worksheet(
        template_ws
    )

    comparison_ws.title = "Comparison"

    # create data quality issues separately 

    quality_ws = wb.create_sheet(
        "Data Quality Issues"
    )

    return (
        wb,
        comparison_ws,
        quality_ws
    )

def build_comparison_sheet(
    ws,
    horizon_data,
    spreadsheet_data,
    fields
):
    """
    Creates one row per Case reference.

    For every field:

        Field (Horizon) | Field (Spreadsheet)

    are placed directly beside each other.
    """

    case_header = ws.cell(
        row=1,
        column=1,
        value="Case reference"
    )

    case_header.font = BOLD_FONT
    case_header.fill = HEADER_FILL
    case_header.alignment = Alignment(
        horizontal="center",
        vertical="center",
        wrap_text=True
    )

    # Case reference spans both header rows
    ws.merge_cells(
        start_row=1,
        start_column=1,
        end_row=2,
        end_column=1
    )

    # HORIZON / SPREADSHEET COLUMN PAIRS
    field_col_map = {}

    current_col = 2

    for field in fields:

        horizon_col = current_col
        spreadsheet_col = current_col + 1

        field_col_map[field] = {
            "horizon": horizon_col,
            "spreadsheet": spreadsheet_col
        }

        horizon_header = ws.cell(
            row=2,
            column=horizon_col,
            value=f"{field} (Horizon)"
        )

        spreadsheet_header = ws.cell(
            row=2,
            column=spreadsheet_col,
            value=f"{field} (Spreadsheet)"
        )

        for cell in (
            horizon_header,
            spreadsheet_header
        ):
            cell.font = BOLD_FONT
            cell.fill = HEADER_FILL
            cell.alignment = Alignment(
                horizontal="center",
                vertical="center",
                wrap_text=True
            )

        current_col += 2


    all_cases = sorted(
        set(horizon_data.keys())
        |
        set(spreadsheet_data.keys())
    )

    row_idx = 3
    mismatch_count = 0

    for case_reference in all_cases:

        horizon_row = horizon_data.get(
            case_reference,
            {}
        )

        spreadsheet_row = spreadsheet_data.get(
            case_reference,
            {}
        )

        # Case reference
        case_cell = ws.cell(
            row=row_idx,
            column=1,
            value=case_reference
        )

        case_cell.font = BOLD_FONT

        # Compare every field

        for field in fields:

            horizon_col = field_col_map[field][
                "horizon"
            ]

            spreadsheet_col = field_col_map[field][
                "spreadsheet"
            ]

            horizon_value = horizon_row.get(
                field
            )

            spreadsheet_value = (
                spreadsheet_row.get(field)
            )

            # Horizon
            horizon_cell = ws.cell(
                row=row_idx,
                column=horizon_col,
                value=(
                    None
                    if is_blank(horizon_value)
                    else horizon_value
                )
            )

            # Spreadsheet
            spreadsheet_cell = ws.cell(
                row=row_idx,
                column=spreadsheet_col,
                value=(
                    None
                    if is_blank(spreadsheet_value)
                    else spreadsheet_value
                )
            )

            # Difference logic

            horizon_normalised = norm(
                horizon_value
            )

            spreadsheet_normalised = norm(
                spreadsheet_value
            )

            # Don't highlight blank vs populated.
            #
            # Only highlight if BOTH sides contain data
            # and the values differ.

            if (
                horizon_normalised
                and spreadsheet_normalised
                and horizon_normalised
                != spreadsheet_normalised
            ):

                horizon_cell.fill = MISMATCH_FILL
                spreadsheet_cell.fill = MISMATCH_FILL

                mismatch_count += 1

        row_idx += 1

    # formatting

    ws.column_dimensions["A"].width = 20

    for column in range(2, current_col):

        column_letter = get_column_letter(
            column
        )

        ws.column_dimensions[
            column_letter
        ].width = 25

    # Wrap text
    for row in ws.iter_rows(
        min_row=1,
        max_row=row_idx - 1,
        min_col=1,
        max_col=current_col - 1
    ):
        for cell in row:
            cell.alignment = Alignment(
                vertical="top",
                wrap_text=True
            )

    ws.freeze_panes = "B3"

    ws.auto_filter.ref = (
        f"A2:{get_column_letter(current_col - 1)}"
        f"{row_idx - 1}"
    )

    return (
        len(all_cases),
        mismatch_count
    )

def build_quality_sheet(
    ws,
    conflicts
):

    headers = [
        "Case reference",
        "Source",
        "Field",
        "Conflicting values",
        "Number of different values"
    ]


    for column, header in enumerate(
        headers,
        start=1
    ):

        cell = ws.cell(
            row=1,
            column=column,
            value=header
        )

        cell.font = BOLD_FONT
        cell.fill = HEADER_FILL


    row_idx = 2

    for conflict in conflicts:

        ws.cell(
            row=row_idx,
            column=1,
            value=conflict[
                "Case reference"
            ]
        )

        ws.cell(
            row=row_idx,
            column=2,
            value=conflict[
                "Source"
            ]
        )

        ws.cell(
            row=row_idx,
            column=3,
            value=conflict[
                "Field"
            ]
        )

        values_cell = ws.cell(
            row=row_idx,
            column=4,
            value=conflict[
                "Values"
            ]
        )

        ws.cell(
            row=row_idx,
            column=5,
            value=conflict[
                "Number of different values"
            ]
        )

        # Highlight conflicting values
        values_cell.fill = CONFLICT_FILL

        row_idx += 1

    widths = {
        "A": 20,
        "B": 20,
        "C": 30,
        "D": 50,
        "E": 25
    }

    for column, width in widths.items():

        ws.column_dimensions[
            column
        ].width = width

    for row in ws.iter_rows(
        min_row=1,
        max_row=max(row_idx - 1, 1),
        min_col=1,
        max_col=5
    ):

        for cell in row:

            cell.alignment = Alignment(
                vertical="top",
                wrap_text=True
            )

    if conflicts:

        ws.auto_filter.ref = (
            f"A1:E{row_idx - 1}"
        )

    ws.freeze_panes = "A2"

    return len(conflicts)

def main():

    print("Loading Horizon data...")

    horizon_data, horizon_conflicts = (
        load_source(
            HORIZON_FILE,
            "Horizon"
        )
    )

    print(
        f"Horizon cases: "
        f"{len(horizon_data)}"
    )

    print(
        f"Horizon source conflicts: "
        f"{len(horizon_conflicts)}"
    )

    print("Loading spreadsheet data...")

    spreadsheet_data, spreadsheet_conflicts = (
        load_source(
            SPREADSHEET_FILE,
            "Spreadsheet"
        )
    )

    print(
        f"Spreadsheet cases: "
        f"{len(spreadsheet_data)}"
    )

    print(
        f"Spreadsheet source conflicts: "
        f"{len(spreadsheet_conflicts)}"
    )

    fields = get_fields(
        horizon_data,
        spreadsheet_data
    )

    print("preparing output workbook...")

    (
        wb,
        comparison_ws,
        quality_ws
    ) = prepare_output_workbook()

    print("building comparison")

    case_count, mismatch_count = (
        build_comparison_sheet(
            comparison_ws,
            horizon_data,
            spreadsheet_data,
            fields
        )
    )


    all_conflicts = (
        horizon_conflicts
        +
        spreadsheet_conflicts
    )

    print(
        "Building Data Quality Issues sheet..."
    )

    quality_count = build_quality_sheet(
        quality_ws,
        all_conflicts
    )

    wb.save(
        OUTPUT_FILE
    )

    print()
    print("Comparison complete")
    print(
        f"Cases: {case_count}"
    )
    print(
        f"Fields compared: {len(fields)}"
    )
    print(
        f"Horizon vs Spreadsheet mismatches: "
        f"{mismatch_count}"
    )
    print(
        f"Source-level data quality issues: "
        f"{quality_count}"
    )
    print(
        f"Output: {OUTPUT_FILE}"
    )


if __name__ == "__main__":
    main()