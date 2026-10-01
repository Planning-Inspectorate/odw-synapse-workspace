import colorsys
import csv
import os
import re
from copy import copy
from datetime import date, datetime
from pathlib import Path

from openpyxl import load_workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter


HORIZON_FILE = "S62aMigration/outputs/MASTER LEGACY cases S62A - with Horizon data.xlsx"

SPREADSHEET_FILE = "S62aMigration/outputs/S62A_All_Sheets_migrated.xlsx"

MASTER_TEMPLATE_FILE = "S62aMigration/csv_and_xlsx_files/MASTER LEGACY cases S62A .xlsx"

SHEET_NAME = "Template"

KEY_COL = "Case reference"

HEADER_ROW = 2

OUTPUT_FILE = "S62aMigration/outputs/S62A_Horizon_vs_Spreadsheet_comparison.xlsx"

SUMMARY_SHEET_NAME = "Contradiction Summary"

DATE_FIELDS = {
    "Application valid",
    "Valid letters sent",
    "LPA questionnaire sent",
    "Representations period - start",
    "Representations period - End",
}

HORIZON_MAPPING_FILE = "S62aMigration/outputs/horizon_field_mapping.csv"

# Fields that Horizon holds in extended_data but which the spreadsheet also fills:
# always show the spreadsheet column and compare them.
ALWAYS_COMPARE_FIELDS = {
    "Site address 1",
    "Site post code",
}

QUERIES_FILE = "S62aMigration/csv_and_xlsx_files/Queries on data for SS + Horizon data merge.xlsx"

# The team confirms a final value for a case + field by shading that cell
# blue in the queries workbook - sometimes a plain colour fill, sometimes a
# theme colour (Excel's colour picker can apply either for the same visual
# blue), so both are detected. Any blue cell overrides (or fills in) the
# matching cell in the spreadsheet output before comparison.
HORIZON_SUFFIX = " (horizon)"
SPREADSHEET_SUFFIX = " (spreadsheet)"

# Column headers in the queries workbook that don't follow the
# "<field> (horizon)" / "<field> (spreadsheet)" convention and so need
# mapping to the real master template field name by hand. A header that
# starts with "Another" (e.g. "Another date?", "Another Outcome?") is
# resolved automatically to whichever field pair precedes it on the row.
QUERIES_COLUMN_FIELD_OVERRIDES = {
    ("Application status", "To-be status"): "Application Status",
}

# Placeholder text that sometimes fills an otherwise-unresolved cell; never
# treated as a real confirmed value even if the cell happens to be blue.
QUERIES_PLACEHOLDER_VALUES = {"no data", "n/a", "na", "?", "???", "tbc"}

REF_YEAR_RE = re.compile(r'/(\d{2})/')


def _normalise_query_case_reference(value):
    if value is None:
        return None
    value = str(value).strip()
    if not value:
        return None
    # the queries workbook sometimes uses two-digit years (S62A/22/...);
    # normalise to match the case references the migration script writes
    return REF_YEAR_RE.sub(lambda m: f"/20{m.group(1)}/", value)


def _load_theme_colors(workbook):
    """Returns the 12 theme colours as 6-digit hex strings, in the order
    a cell's theme colour index refers to: 0 lt1, 1 dk1, 2 lt2, 3 dk2,
    4-9 accent1-6, 10 hlink, 11 folHlink."""
    theme_xml = workbook.loaded_theme
    if not theme_xml:
        return None
    if isinstance(theme_xml, bytes):
        theme_xml = theme_xml.decode("utf-8")
    match = re.search(r"<a:clrScheme.*?</a:clrScheme>", theme_xml, re.S)
    if not match:
        return None
    scheme = {}
    for tag, attrs in re.findall(r"<a:(\w+)>\s*<a:(?:srgbClr|sysClr)([^/>]*)/?>", match.group(0)):
        value_match = (re.search(r'lastClr="([0-9A-Fa-f]{6})"', attrs)
                       or re.search(r'val="([0-9A-Fa-f]{6})"', attrs))
        if value_match:
            scheme[tag] = value_match.group(1)
    order = ["lt1", "dk1", "lt2", "dk2", "accent1", "accent2", "accent3",
             "accent4", "accent5", "accent6", "hlink", "folHlink"]
    return [scheme.get(name, "000000") for name in order]


def _apply_tint(rgb_hex, tint):
    r, g, b = (int(rgb_hex[i:i + 2], 16) / 255 for i in (0, 2, 4))
    h, l, s = colorsys.rgb_to_hls(r, g, b)
    l = l * (1 + tint) if tint < 0 else l * (1 - tint) + tint
    r, g, b = colorsys.hls_to_rgb(h, max(0, min(1, l)), s)
    return "%02X%02X%02X" % (round(r * 255), round(g * 255), round(b * 255))


def _resolve_fill_rgb(cell, theme_colors):
    if not (cell.fill and cell.fill.patternType):
        return None
    fg = cell.fill.fgColor
    if fg.type == "rgb" and isinstance(fg.rgb, str) and len(fg.rgb) == 8:
        return fg.rgb[2:]
    if fg.type == "theme" and theme_colors and fg.theme < len(theme_colors):
        return _apply_tint(theme_colors[fg.theme], fg.tint or 0)
    return None


def _is_confirmed_blue(rgb_hex):
    """True for the blue shades used to mark a confirmed value, whichever
    way Excel stored the colour. Tuned to exclude the workbook's other
    highlight colours (yellow, orange, red, green, grey)."""
    if not rgb_hex:
        return False
    r, g, b = (int(rgb_hex[i:i + 2], 16) / 255 for i in (0, 2, 4))
    h, l, s = colorsys.rgb_to_hls(r, g, b)
    hue_deg = h * 360
    return 175 <= hue_deg <= 240 and s >= 0.35 and 0.25 <= l <= 0.75


def _load_confirmed_overrides(queries_file=QUERIES_FILE):
    """
    Scans the queries workbook for blue-filled cells and treats each one as
    the team's confirmed final value for that case reference + field.
    Returns {case_reference: {field_name: value}}.
    """
    if not os.path.isfile(queries_file):
        print(f"Queries file not found, skipping confirmed overrides: {queries_file}")
        return {}

    wb = load_workbook(queries_file, data_only=True)
    theme_colors = _load_theme_colors(wb)
    overrides = {}
    for ws in wb.worksheets:
        header_row = None
        col_fields = {}
        for row in ws.iter_rows(min_row=1, max_row=min(5, ws.max_row)):
            found = {}
            last_field = None
            for cell in row:
                if not isinstance(cell.value, str):
                    continue
                text = cell.value.strip()
                key = (ws.title, text)
                if key in QUERIES_COLUMN_FIELD_OVERRIDES:
                    last_field = QUERIES_COLUMN_FIELD_OVERRIDES[key]
                    found[cell.column] = last_field
                elif text.endswith(HORIZON_SUFFIX):
                    last_field = text[: -len(HORIZON_SUFFIX)]
                    found[cell.column] = last_field
                elif text.endswith(SPREADSHEET_SUFFIX):
                    last_field = text[: -len(SPREADSHEET_SUFFIX)]
                    found[cell.column] = last_field
                elif text.lower().startswith("another") and last_field:
                    # a resolution column (e.g. "Another date?") belongs to
                    # whichever field pair precedes it on the same row
                    found[cell.column] = last_field
            if found:
                header_row = row[0].row
                col_fields = found
        if not col_fields:
            continue

        for row in ws.iter_rows(min_row=header_row + 1):
            case_reference = _normalise_query_case_reference(row[0].value)
            if case_reference is None:
                continue
            for cell in row:
                if cell.column not in col_fields:
                    continue
                rgb = _resolve_fill_rgb(cell, theme_colors)
                if not _is_confirmed_blue(rgb):
                    continue
                if _is_blank(cell.value):
                    continue
                if isinstance(cell.value, str) and cell.value.strip().lower() in QUERIES_PLACEHOLDER_VALUES:
                    continue
                overrides.setdefault(case_reference, {})[col_fields[cell.column]] = cell.value
    return overrides


def _apply_confirmed_overrides(spreadsheet_cases, spreadsheet_indexes, overrides):
    """Writes each confirmed value into spreadsheet_cases and returns the set
    of (case_reference, field_index) cells that were actually changed, so the
    caller can carry the blue confirmation colour through to the output."""
    confirmed_cells = set()
    skipped_case, skipped_field = [], []
    for case_reference, field_values in overrides.items():
        case = spreadsheet_cases.get(case_reference)
        if case is None:
            skipped_case.append(case_reference)
            continue
        for field_name, value in field_values.items():
            index = spreadsheet_indexes.get(field_name)
            if index is None:
                skipped_field.append((case_reference, field_name))
                continue
            case[index] = value
            confirmed_cells.add((case_reference, index))
    print(f"Confirmed overrides: {len(confirmed_cells)} applied")
    if skipped_case:
        print(f"  {len(skipped_case)} case(s) not found in the migrated spreadsheet output: "
              f"{sorted(set(skipped_case))}")
    if skipped_field:
        print(f"  {len(skipped_field)} field(s) not found in the template headers: "
              f"{sorted(set(skipped_field))}")
    return confirmed_cells


# highlight differences in red
DIFFERENCE_FILL = PatternFill(
    fill_type="solid",
    start_color="FFC7CE",
    end_color="FFC7CE",
)

# marks a cell whose value came from a confirmed (blue) cell in the queries
# workbook, carrying that same blue through to the comparison output
CONFIRMED_FILL = PatternFill(
    fill_type="solid",
    start_color="00B0F0",
    end_color="00B0F0",
)


def _get_extended_data_fields(mapping_file=HORIZON_MAPPING_FILE):
    with open(mapping_file, newline="", encoding="utf-8-sig") as mapping_stream:
        return {
            row["Field"].strip()
            for row in csv.DictReader(mapping_stream)
            if row.get("Field", "").strip()
            and row.get("Source field", "").strip().startswith("extended_data.")
        }


def _get_columns(worksheet):
    columns = []
    for column in range(1, worksheet.max_column + 1):
        value = worksheet.cell(HEADER_ROW, column).value
        if value is None or str(value).strip() == "":
            continue
        columns.append((str(value).strip(), column))
    return columns


def _normalise_case_reference(value):
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def _is_blank(value):
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    return value != value


def _normalise_date(value):
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if not isinstance(value, str):
        return value
    value = value.strip()
    for date_format in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(value[:10], date_format).date().isoformat()
        except ValueError:
            continue
    return value


def _values_differ(left, right, field_name=None):
    if _is_blank(left) or _is_blank(right):
        return False
    if field_name == "LPA":
        horizon_value = str(left).strip().casefold()
        spreadsheet_value = str(right).strip().casefold()
        return spreadsheet_value not in horizon_value
    if field_name and (
        "date" in field_name.casefold() or field_name in DATE_FIELDS
    ):
        return _normalise_date(left) != _normalise_date(right)
    if isinstance(left, str) and isinstance(right, str):
        return left.strip() != right.strip()
    return left != right


def _read_cases(worksheet, columns):
    cases = {}
    order = []
    source_rows = {}
    key_column = next(column for header, column in columns if header == KEY_COL)

    for row in range(HEADER_ROW + 1, worksheet.max_row + 1):
        case_reference = _normalise_case_reference(
            worksheet.cell(row, key_column).value
        )
        if case_reference is None:
            continue

        if case_reference not in cases:
            cases[case_reference] = {
                index: worksheet.cell(row, column).value
                for index, (_, column) in enumerate(columns)
            }
            order.append(case_reference)
            source_rows[case_reference] = row
            continue

        for index, (_, column) in enumerate(columns):
            existing = cases[case_reference][index]
            incoming = worksheet.cell(row, column).value
            if existing in (None, "") and incoming not in (None, ""):
                cases[case_reference][index] = incoming

    return cases, order, source_rows


def _copy_cell_format(source_cell, target_cell):
    if source_cell.has_style:
        target_cell._style = copy(source_cell._style)
    target_cell.number_format = source_cell.number_format
    target_cell.font = copy(source_cell.font)
    target_cell.fill = copy(source_cell.fill)
    target_cell.border = copy(source_cell.border)
    target_cell.alignment = copy(source_cell.alignment)
    target_cell.protection = copy(source_cell.protection)


def _copy_column_format(source_worksheet, source_column, target_worksheet, target_column):
    source_letter = get_column_letter(source_column)
    target_letter = get_column_letter(target_column)
    source_dimension = source_worksheet.column_dimensions[source_letter]
    target_dimension = target_worksheet.column_dimensions[target_letter]
    target_dimension.width = source_dimension.width
    target_dimension.hidden = source_dimension.hidden
    target_dimension.bestFit = source_dimension.bestFit

    for row in range(1, HEADER_ROW + 2):
        _copy_cell_format(
            source_worksheet.cell(row, source_column),
            target_worksheet.cell(row, target_column),
        )


def _write_contradiction_summary(workbook, master_headers, contradiction_counts):
    if SUMMARY_SHEET_NAME in workbook.sheetnames:
        del workbook[SUMMARY_SHEET_NAME]

    summary_sheet = workbook.create_sheet(SUMMARY_SHEET_NAME)
    summary_sheet.append(["Master template field", "Contradiction count"])

    header_fill = PatternFill(
        fill_type="solid",
        start_color="C00000",
        end_color="C00000",
    )
    for cell in summary_sheet[1]:
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = copy(header_fill)
        cell.alignment = Alignment(horizontal="center", vertical="center")

    written_fields = set()
    for index, field_name in enumerate(master_headers):
        if field_name == KEY_COL or field_name in written_fields:
            continue
        count = sum(
            contradiction_counts.get(field_index, 0)
            for field_index, candidate_name in enumerate(master_headers)
            if candidate_name == field_name
        )
        if count == 0:
            continue
        summary_sheet.append([field_name, count])
        written_fields.add(field_name)

    summary_sheet.column_dimensions["A"].width = 48
    summary_sheet.column_dimensions["B"].width = 22
    summary_sheet.freeze_panes = "A2"
    summary_sheet.auto_filter.ref = f"A1:B{summary_sheet.max_row}"
    summary_sheet.sheet_view.showGridLines = False


def _output_column(source_column, source_is_key=False):
    if source_is_key:
        return 1
    return 2 + ((source_column - 2) * 2)


def combine_sources(
    horizon_file=HORIZON_FILE,
    spreadsheet_file=SPREADSHEET_FILE,
    master_template_file=MASTER_TEMPLATE_FILE,
    output_file=OUTPUT_FILE,
):
    # Write one output row per case with Horizon and spreadsheet values paired
    horizon_workbook = load_workbook(horizon_file, data_only=False)
    spreadsheet_workbook = load_workbook(spreadsheet_file, data_only=False)
    master_workbook = load_workbook(master_template_file, data_only=False)

    try:
        horizon_source = horizon_workbook[SHEET_NAME]
        spreadsheet_source = spreadsheet_workbook[SHEET_NAME]
        master_source = master_workbook[SHEET_NAME]
        horizon_columns = _get_columns(horizon_source)
        spreadsheet_columns = _get_columns(spreadsheet_source)
        master_columns = _get_columns(master_source)

        horizon_headers = [header for header, _ in horizon_columns]
        spreadsheet_headers = [header for header, _ in spreadsheet_columns]
        master_headers = [header for header, _ in master_columns]
        extended_data_fields = _get_extended_data_fields()
        horizon_indexes = {header: index for index, header in enumerate(horizon_headers)}
        spreadsheet_indexes = {header: index for index, header in enumerate(spreadsheet_headers)}
        if KEY_COL not in horizon_headers or KEY_COL not in spreadsheet_headers:
            raise ValueError(f"Both sheets must contain {KEY_COL!r}")

        missing_horizon_headers = [
            header for header in master_headers if header not in horizon_headers
        ]
        missing_spreadsheet_headers = [
            header for header in master_headers if header not in spreadsheet_headers
        ]
        if missing_horizon_headers or missing_spreadsheet_headers:
            raise ValueError(
                "Both sources must contain every legacy master column. Missing "
                f"from Horizon: {missing_horizon_headers}; missing from spreadsheet: "
                f"{missing_spreadsheet_headers}"
            )
        spreadsheet_only_headers = [
            header for header in spreadsheet_headers if header not in master_headers
        ]

        horizon_cases, horizon_order, horizon_rows = _read_cases(
            horizon_source, horizon_columns
        )
        spreadsheet_cases, spreadsheet_order, spreadsheet_rows = _read_cases(
            spreadsheet_source, spreadsheet_columns
        )

        confirmed_overrides = _load_confirmed_overrides()
        confirmed_cells = _apply_confirmed_overrides(
            spreadsheet_cases, spreadsheet_indexes, confirmed_overrides
        )

        output_workbook = load_workbook(master_template_file, data_only=False)
        output_sheet = output_workbook[SHEET_NAME]
        try:
            source_merges = list(output_sheet.merged_cells.ranges)
            for merged_range in source_merges:
                output_sheet.unmerge_cells(str(merged_range))
            if output_sheet.max_row > HEADER_ROW:
                output_sheet.delete_rows(HEADER_ROW + 1, output_sheet.max_row - HEADER_ROW)

            key_index = master_headers.index(KEY_COL)
            spreadsheet_key_index = spreadsheet_indexes[KEY_COL]
            output_headers = [KEY_COL]
            # Entries are (header, summary index, source index, is_horizon).
            output_sources = [(KEY_COL, key_index, spreadsheet_key_index, False)]
            comparison_pairs = []
            for index, header in enumerate(master_headers):
                if header != KEY_COL:
                    horizon_column = len(output_headers) + 1
                    output_headers.append(f"{header} (horizon)")
                    output_sources.append((header, index, horizon_indexes[header], True))
                    if header in extended_data_fields and header not in ALWAYS_COMPARE_FIELDS:
                        continue
                    spreadsheet_column = len(output_headers) + 1
                    output_headers.append(f"{header} (spreadsheet)")
                    output_sources.append((header, index, spreadsheet_indexes[header], False))
                    comparison_pairs.append((horizon_column, spreadsheet_column, index))
            for header in spreadsheet_only_headers:
                source_index = spreadsheet_headers.index(header)
                summary_index = len(master_headers)
                master_headers.append(header)
                horizon_column = len(output_headers) + 1
                output_headers.append(f"{header} (horizon)")
                output_sources.extend(
                    ((header, summary_index, None, True),)
                )
                spreadsheet_column = len(output_headers) + 1
                output_headers.append(f"{header} (spreadsheet)")
                output_sources.append((header, summary_index, source_index, False))
                comparison_pairs.append((horizon_column, spreadsheet_column, summary_index))

            output_columns_by_source_index = {}
            for output_column, (_, index, _, _) in enumerate(output_sources, start=1):
                output_columns_by_source_index.setdefault(index, []).append(output_column)

            for output_column, (_, _, source_index, is_horizon) in enumerate(
                output_sources, start=1
            ):
                if source_index is None:
                    output_sheet.cell(HEADER_ROW, output_column).value = output_headers[
                        output_column - 1
                    ]
                    continue
                source_worksheet = horizon_source if is_horizon else spreadsheet_source
                source_columns = horizon_columns if is_horizon else spreadsheet_columns
                source_column = source_columns[source_index][1]
                _copy_column_format(
                    source_worksheet,
                    source_column,
                    output_sheet,
                    output_column,
                )
                output_sheet.cell(HEADER_ROW, output_column).value = output_headers[
                    output_column - 1
                ]

            for merged_range in source_merges:
                if merged_range.min_row != 1 or merged_range.max_row != 1:
                    continue
                mapped_columns = [
                    output_column
                    for index, (_, source_column) in enumerate(master_columns)
                    if merged_range.min_col <= source_column <= merged_range.max_col
                    for output_column in output_columns_by_source_index.get(index, [])
                ]
                if not mapped_columns:
                    continue
                start_column = min(mapped_columns)
                end_column = max(mapped_columns)
                output_sheet.cell(1, start_column).value = master_source.cell(
                    1, merged_range.min_col
                ).value
                _copy_cell_format(
                    master_source.cell(1, merged_range.min_col),
                    output_sheet.cell(1, start_column),
                )
                output_sheet.merge_cells(
                    start_row=1,
                    start_column=start_column,
                    end_row=1,
                    end_column=end_column,
                )

            case_references = list(spreadsheet_order)
            case_references.extend(
                case_reference
                for case_reference in horizon_order
                if case_reference not in spreadsheet_cases
            )
            contradiction_counts = {}

            for output_row, case_reference in enumerate(
                case_references, start=HEADER_ROW + 1
            ):
                spreadsheet_row = spreadsheet_rows.get(case_reference)
                horizon_row = horizon_rows.get(case_reference)
                output_sheet.cell(output_row, 1).value = case_reference

                if spreadsheet_row is not None:
                    _copy_cell_format(
                        spreadsheet_source.cell(
                            spreadsheet_row, spreadsheet_columns[spreadsheet_key_index][1]
                        ),
                        output_sheet.cell(output_row, 1),
                    )

                confirmed_columns = set()
                for output_column, (_, index, source_index, is_horizon) in enumerate(
                    output_sources[1:], start=2
                ):
                    cases = horizon_cases if is_horizon else spreadsheet_cases
                    source_row = horizon_row if is_horizon else spreadsheet_row
                    source_worksheet = horizon_source if is_horizon else spreadsheet_source
                    source_columns = horizon_columns if is_horizon else spreadsheet_columns
                    output_sheet.cell(output_row, output_column).value = (
                        cases.get(case_reference, {}).get(source_index)
                        if source_index is not None else None
                    )
                    if source_row is not None and source_index is not None:
                        _copy_cell_format(
                            source_worksheet.cell(source_row, source_columns[source_index][1]),
                            output_sheet.cell(output_row, output_column),
                        )
                    if not is_horizon and (case_reference, index) in confirmed_cells:
                        output_sheet.cell(output_row, output_column).fill = copy(CONFIRMED_FILL)
                        confirmed_columns.add(output_column)

                for horizon_column, spreadsheet_column, field_index in comparison_pairs:
                    horizon_value = output_sheet.cell(
                        output_row, horizon_column
                    ).value
                    spreadsheet_value = output_sheet.cell(
                        output_row, spreadsheet_column
                    ).value
                    field_name = output_sources[horizon_column - 1][0]
                    if _values_differ(horizon_value, spreadsheet_value, field_name):
                        contradiction_counts[field_index] = (
                            contradiction_counts.get(field_index, 0) + 1
                        )
                        output_sheet.cell(
                            output_row, horizon_column
                        ).fill = copy(DIFFERENCE_FILL)
                        # a confirmed (blue) spreadsheet value keeps its blue fill even
                        # when it still disagrees with Horizon - the confirmation wins
                        if spreadsheet_column not in confirmed_columns:
                            output_sheet.cell(
                                output_row, spreadsheet_column
                            ).fill = copy(DIFFERENCE_FILL)

                source_row = spreadsheet_row or horizon_row
                if source_row is not None:
                    source_worksheet = (
                        spreadsheet_source if spreadsheet_row is not None else horizon_source
                    )
                    output_sheet.row_dimensions[output_row].height = source_worksheet.row_dimensions[
                        source_row
                    ].height

            output_sheet.freeze_panes = f"A{HEADER_ROW + 1}"
            output_sheet.auto_filter.ref = (
                f"A{HEADER_ROW}:{get_column_letter(len(output_headers))}"
                f"{HEADER_ROW + len(case_references)}"
            )
            _write_contradiction_summary(
                output_workbook,
                master_headers,
                contradiction_counts,
            )
            Path(output_file).parent.mkdir(parents=True, exist_ok=True)
            output_workbook.save(output_file)
        finally:
            output_workbook.close()
    finally:
        horizon_workbook.close()
        spreadsheet_workbook.close()
        master_workbook.close()


if __name__ == "__main__":
    combine_sources()
    print(f"Created {OUTPUT_FILE}")