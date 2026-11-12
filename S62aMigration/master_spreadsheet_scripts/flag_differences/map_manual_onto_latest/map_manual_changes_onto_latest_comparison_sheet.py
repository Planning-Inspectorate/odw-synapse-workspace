"""Map the coloured cells of the manual-changes draft sheet onto the latest comparison sheet.

The colours in the draft already mark the manual changes, so nothing is diffed. Coloured cells are
copied with their values, except red, white, pale yellow and bright yellow.
Cells are matched by
(case reference, occurrence, column header), never by
cell address, because the latest sheet has a different layout (header row, extra columns, row order).
Case references can repeat, so the Nth occurrence of a reference in the draft is matched to the Nth
occurrence in the latest sheet.

A change log records the cells copied, the value the latest cell held before, and any cell whose
case/column could not be found in the latest sheet.

Before mapping, the draft's row 2 (notes) and row 4 (duplicate header row) are inserted into the
latest sheet, and the draft's styling for rows 2-4 (fills, fonts, borders, merges, heights) is copied
across. Columns that only exist in the latest sheet are kept and take the style of the draft's last column.

Usage: python map_manual_changes_onto_latest_comparison_sheet.py
Outputs (next to this script):
  S62A_Horizon_vs_Spreadsheet_comparison_with_manual_changes.xlsx
  manual_changes_mapping_log.xlsx
"""

import colorsys
from collections import Counter
from copy import copy
from pathlib import Path

import openpyxl
from openpyxl.styles.colors import COLOR_INDEX
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter

HERE = Path(__file__).parent
SOURCE = HERE / "S62A_comparison_3rd_vs_5th_DIFF(MANUAL_CHANGES_SHEET)_draft2(latest).xlsx"
LATEST = HERE.parents[2] / "outputs" / "S62A_Horizon_vs_Spreadsheet_comparison.xlsx"
OUTPUT = HERE / "S62A_Horizon_vs_Spreadsheet_comparison_with_manual_changes.xlsx"
LOG = HERE / "manual_changes_mapping_log.xlsx"

SHEET = "Template"
DRAFT_HEADER_ROW, DRAFT_FIRST_ROW = 3, 5
# After add_draft_header_rows() the latest sheet has the same layout as the draft.
LATEST_HEADER_ROW, LATEST_FIRST_ROW = 3, 5
KEY_COL = 1


def norm(v):
    if v is None:
        return ""
    if isinstance(v, str):
        return v.replace("\xa0", " ").strip()
    return v


def fill_sig(cell):
    f = cell.fill
    if f is None or f.fill_type is None:
        return ""
    c = f.fgColor
    if c.type == "rgb":
        return str(c.rgb)
    if c.type == "theme":
        return f"theme{c.theme}/{round(c.tint or 0, 2)}"
    return f"indexed{c.indexed}"


# Default Office theme colours (index order used by Excel: lt1, dk1, lt2, dk2, accent1-6, ...)
THEME_RGB = ["FFFFFF", "000000", "E7E6E6", "44546A", "4472C4", "ED7D31", "A5A5A5", "FFC000", "5B9BD5", "70AD47"]


def fill_rgb(cell):
    """Resolve the cell's solid fill to an (r, g, b) tuple, or None if there is no fill."""
    f = cell.fill
    if f is None or f.fill_type != "solid":
        return None
    c = f.fgColor
    if c.type == "rgb" and isinstance(c.rgb, str):
        hex_ = c.rgb[-6:]
    elif c.type == "theme" and c.theme is not None and c.theme < len(THEME_RGB):
        hex_ = THEME_RGB[c.theme]
    elif c.type == "indexed" and c.indexed is not None and c.indexed < len(COLOR_INDEX):
        hex_ = COLOR_INDEX[c.indexed][-6:]
    else:
        return None
    r, g, b = (int(hex_[i:i + 2], 16) / 255 for i in (0, 2, 4))
    tint = (c.tint or 0) if c.type == "theme" else 0
    if tint:
        h, l, s = colorsys.rgb_to_hls(r, g, b)
        l = l * (1 + tint) if tint < 0 else l * (1 - tint) + tint
        r, g, b = colorsys.hls_to_rgb(h, l, s)
    return r, g, b


def is_red(rgb):
    """Any shade of red, including light pinks and dark reds."""
    h, s, v = colorsys.rgb_to_hsv(*rgb)
    return (h <= 0.04 or h >= 0.94) and s >= 0.12 and v >= 0.2


def is_coloured_non_red(cell):
    rgb = fill_rgb(cell)
    if rgb is None:
        return False
    if min(rgb) >= 0.99:  # white is not a highlight colour
        return False
    if any(
        all(abs(value - expected) < 1 / 510 for value, expected in zip(rgb, excluded))
        for excluded in ((1, 242 / 255, 204 / 255), (1, 1, 0))
    ):
        return False
    return not is_red(rgb)


def index_rows(ws, first_row):
    """Map (case reference, occurrence) -> row number."""
    seen = Counter()
    rows = {}
    for r in range(first_row, ws.max_row + 1):
        ref = str(norm(ws.cell(r, KEY_COL).value))
        if not ref:
            continue
        seen[ref] += 1
        rows[(ref, seen[ref])] = r
    return rows


def index_cols(ws, header_row):
    cols = {}
    for c in range(1, ws.max_column + 1):
        h = norm(ws.cell(header_row, c).value)
        if h:
            cols.setdefault(h, c)
    return cols


def copy_style(src, dst):
    dst.font = copy(src.font)
    dst.fill = copy(src.fill)
    dst.border = copy(src.border)
    dst.alignment = copy(src.alignment)
    dst.number_format = src.number_format
    dst.protection = copy(src.protection)


def has_draft_header_rows(ws):
    """True if rows 3 and 4 are both header rows (draft layout: notes row 2, header row 3, duplicate header row 4)."""
    return all(norm(ws.cell(r, KEY_COL).value) == "Case reference" for r in (3, 4))


def add_draft_header_rows(draft_ws, latest_ws):
    """Insert the draft's notes row (2) and duplicate header row (4) and copy rows 2-4 styling."""
    old_heights = {r: d.height for r, d in latest_ws.row_dimensions.items() if d.height is not None}
    latest_ws.insert_rows(2)
    latest_ws.insert_rows(4)
    for r in sorted(old_heights, reverse=True):
        if r >= 2:
            latest_ws.row_dimensions[r + (1 if r == 2 else 2)].height = old_heights[r]
    for r in (2, 3, 4):
        latest_ws.row_dimensions[r].height = draft_ws.row_dimensions[r].height

    draft_cols = index_cols(draft_ws, DRAFT_HEADER_ROW)
    last_draft_col = draft_ws.max_column
    for c in range(1, latest_ws.max_column + 1):
        header = norm(latest_ws.cell(3, c).value)
        dc = draft_cols.get(header, last_draft_col)
        for r in (2, 3, 4):
            copy_style(draft_ws.cell(r, dc), latest_ws.cell(r, c))
        if header in draft_cols:
            latest_ws.cell(2, c).value = draft_ws.cell(2, dc).value
        latest_ws.cell(4, c).value = latest_ws.cell(3, c).value

    for m in draft_ws.merged_cells.ranges:
        if 2 <= m.min_row <= 4:
            assert draft_cols.get(norm(latest_ws.cell(3, m.min_col).value)) == m.min_col, m
            latest_ws.merge_cells(str(m))

    latest_ws.freeze_panes = "A4"
    latest_ws.auto_filter.ref = f"A3:{get_column_letter(latest_ws.max_column)}{latest_ws.max_row}"


def main():
    source_ws = openpyxl.load_workbook(SOURCE)[SHEET]
    latest_wb = openpyxl.load_workbook(LATEST)
    latest_ws = latest_wb[SHEET]
    # A latest sheet that already carries the draft's notes row and duplicate header row must not get them twice.
    if not has_draft_header_rows(latest_ws):
        add_draft_header_rows(source_ws, latest_ws)

    source_rows = index_rows(source_ws, DRAFT_FIRST_ROW)
    latest_rows = index_rows(latest_ws, LATEST_FIRST_ROW)
    source_cols = index_cols(source_ws, DRAFT_HEADER_ROW)
    latest_cols = index_cols(latest_ws, LATEST_HEADER_ROW)

    # Copy only the colours accepted as manual changes.
    log = []
    for key, sr in source_rows.items():
        lr = latest_rows.get(key)
        for header, sc in source_cols.items():
            s_cell = source_ws.cell(sr, sc)
            if not is_coloured_non_red(s_cell):
                continue
            lc = latest_cols.get(header)
            if lr is None or lc is None:
                log.append((key[0], key[1], header, "Coloured cell", fill_sig(s_cell), s_cell.value, "", "NOT APPLIED - case/column not in latest sheet"))
                continue
            l_cell = latest_ws.cell(lr, lc)
            latest_before = l_cell.value
            unchanged = norm(latest_before) == norm(s_cell.value) and fill_sig(l_cell) == fill_sig(s_cell)
            l_cell.value = s_cell.value
            l_cell.fill = copy(s_cell.fill)
            log.append((key[0], key[1], header, "Coloured cell", fill_sig(s_cell), s_cell.value, latest_before,
                        "Already matched" if unchanged else "Applied"))

    latest_wb.save(OUTPUT)

    wb = openpyxl.Workbook()
    summary = wb.active
    summary.title = "Summary"
    counts = Counter((r[3], r[7].split(" (")[0].split(" -")[0]) for r in log)
    summary.append(["Change type", "Result", "Count"])
    for (ctype, res), n in sorted(counts.items()):
        summary.append([ctype, res, n])
    detail = wb.create_sheet("Mapping log")
    detail.append(["Case reference", "Occurrence", "Column", "Change type", "Fill colour", "Value copied", "Latest before merge", "Status"])
    for r in log:
        detail.append(list(r))
    for sheet in (summary, detail):
        for c in sheet[1]:
            c.font = Font(bold=True, color="FFFFFF")
            c.fill = PatternFill("solid", fgColor="305496")
        for i in range(1, sheet.max_column + 1):
            sheet.column_dimensions[get_column_letter(i)].width = 32
    detail.freeze_panes = "A2"
    detail.auto_filter.ref = detail.dimensions
    wb.save(LOG)

    print(f"Wrote {OUTPUT.name} and {LOG.name}")
    for (ctype, res), n in sorted(counts.items()):
        print(f"{ctype}: {res} = {n}")


if __name__ == "__main__":
    main()
