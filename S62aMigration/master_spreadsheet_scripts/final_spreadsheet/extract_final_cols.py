from copy import copy
from pathlib import Path
import re

from openpyxl import load_workbook
from openpyxl.styles.colors import COLOR_INDEX
from openpyxl.utils import get_column_letter


BASE_DIR = Path(
	"/Users/nisalihalwathura/PINS/ODW-Service/odw-synapse-workspace/S62aMigration"
)
COMPARISON_FILE = (
	BASE_DIR
	/ "csv_and_xlsx_files/S62A_Horizon_vs_Spreadsheet_comparison_coloured_sheet.xlsx"
)
MASTER_TEMPLATE_FILE = (
	BASE_DIR / "csv_and_xlsx_files/MASTER LEGACY cases S62A .xlsx"
)
OUTPUT_FILE = BASE_DIR / "outputs/MASTER LEGACY cases S62A - final.xlsx"

SHEET_NAME = "Template"
FINAL_SHEET_NAME = "Final selected"
COMPARISON_HEADER_ROW = 3
COMPARISON_COLOUR_ROW = 3
COMPARISON_FIRST_DATA_ROW = 4
TEMPLATE_HEADER_ROW = 2
TEMPLATE_FIRST_DATA_ROW = 3
KEY_COLUMN = "Case reference"


def _is_blank(value):
	return value is None or (isinstance(value, str) and not value.strip())


def _headers(sheet, row):
	return {
		str(sheet.cell(row, column).value).strip(): column
		for column in range(1, sheet.max_column + 1)
		if sheet.cell(row, column).value not in (None, "")
	}


def _rgb(cell):
	fill = cell.fill
	colour = fill.fgColor
	if fill.fill_type != "solid":
		return None
	if colour.type == "rgb" and colour.rgb:
		value = colour.rgb[-6:]
	elif colour.type == "indexed" and colour.indexed is not None:
		value = COLOR_INDEX[colour.indexed][-6:]
	else:
		return None
	try:
		return tuple(int(value[index:index + 2], 16) for index in (0, 2, 4))
	except ValueError:
		return None


def _colour_kind(cell):
	"""Classify the decision fill used in Row 3."""
	colour = cell.fill.fgColor
	if (
		cell.fill.fill_type == "solid"
		and colour.type == "theme"
		and colour.theme in (0, 1)
		and colour.tint is not None
		and -0.2 <= colour.tint <= -0.1
	):
		return "grey"
	components = _rgb(cell)
	if components is None:
		return "other"
	red, green, blue = components
	brightness = sum(components) / 3
	spread = max(components) - min(components)
	if spread <= 18 and brightness < 245:
		return "grey"
	if green > red and green > blue:
		return "green" if brightness < 180 else "light_green"
	return "other"


def _case_reference_column(sheet):
	headers = _headers(sheet, COMPARISON_HEADER_ROW)
	if KEY_COLUMN in headers:
		return headers[KEY_COLUMN]
	for header, column in headers.items():
		if header.casefold().startswith(f"{KEY_COLUMN.casefold()} ("):
			return column
	raise KeyError(f"Missing {KEY_COLUMN!r} in comparison row {COMPARISON_HEADER_ROW}")


def _case_rows(sheet):
	key_column = _case_reference_column(sheet)
	rows = {}
	for row in range(COMPARISON_FIRST_DATA_ROW, sheet.max_row + 1):
		case_reference = sheet.cell(row, key_column).value
		if not _is_blank(case_reference):
			rows.setdefault(str(case_reference).strip(), []).append(row)
	return rows


def _select_sources(comparison_sheet):
	source_headers = _headers(comparison_sheet, COMPARISON_HEADER_ROW)
	groups = {}
	for header, column in source_headers.items():
		if header.casefold().startswith(KEY_COLUMN.casefold()):
			continue
		match = re.match(r"^(.*) \((horizon|spreadsheet)\)$", header, re.IGNORECASE)
		if not match:
			continue
		field = match.group(1).strip()
		source = match.group(2).casefold()
		groups.setdefault(field, {})[source] = column

	selections = []
	for field, sources in groups.items():
		kinds = {
			source: _colour_kind(comparison_sheet.cell(COMPARISON_COLOUR_ROW, column))
			for source, column in sources.items()
		}
		available = [source for source, kind in kinds.items() if kind != "grey"]
		if not available:
			continue

		green_sources = [source for source in available if kinds[source] == "green"]
		if green_sources:
			primary_source = green_sources[0]
			fallback_source = (
				"horizon" if primary_source == "spreadsheet" else "spreadsheet"
			)
			fallback = sources.get(fallback_source) if kinds.get(fallback_source) == "light_green" else None
			selections.append({
				"header": field,
				"primary": sources[primary_source],
				"fallback": fallback,
			})
		else:
			for source in available:
				selections.append({
					"header": f"{field} ({source})",
					"primary": sources[source],
					"fallback": None,
				})
	return selections


def _first_value(sheet, rows, column):
	if column is None:
		return None
	for row in rows:
		value = sheet.cell(row, column).value
		if not _is_blank(value):
			return value
	return None


def _copy_template_header(source_sheet, target_sheet, source_column, target_column):
	source = source_sheet.cell(TEMPLATE_HEADER_ROW, source_column)
	target = target_sheet.cell(TEMPLATE_HEADER_ROW, target_column)
	target.value = source.value
	target._style = copy(source._style)
	target.number_format = source.number_format
	target.alignment = copy(source.alignment)
	target.border = copy(source.border)
	target.fill = copy(source.fill)
	target.font = copy(source.font)


def _copy_style(source, target):
	target._style = copy(source._style)
	target.number_format = source.number_format
	target.font = copy(source.font)
	target.fill = copy(source.fill)
	target.border = copy(source.border)
	target.alignment = copy(source.alignment)
	target.protection = copy(source.protection)


def _merged_anchor(sheet, row, column):
	for merged_range in sheet.merged_cells.ranges:
		if (
			merged_range.min_row <= row <= merged_range.max_row
			and merged_range.min_col <= column <= merged_range.max_col
		):
			return merged_range.min_row, merged_range.min_col
	return row, column


def _merge_repeated_row_one(sheet):
	start_column = 1
	while start_column <= sheet.max_column:
		label = sheet.cell(1, start_column).value
		end_column = start_column
		while (
			end_column + 1 <= sheet.max_column
			and sheet.cell(1, end_column + 1).value == label
		):
			end_column += 1
		if label not in (None, "") and end_column > start_column:
			top_left = sheet.cell(1, start_column)
			top_left.alignment = copy(top_left.alignment)
			top_left.alignment = top_left.alignment.copy(
				horizontal="center",
				vertical="center",
			)
			sheet.merge_cells(
				start_row=1,
				start_column=start_column,
				end_row=1,
				end_column=end_column,
			)
		else:
			end_column = start_column
		start_column = end_column + 1


def extract_final_columns():
	comparison_workbook = load_workbook(COMPARISON_FILE, data_only=True)
	comparison_styles_workbook = load_workbook(COMPARISON_FILE, data_only=False)
	template_workbook = load_workbook(MASTER_TEMPLATE_FILE, data_only=False)
	try:
		comparison_sheet = comparison_workbook[SHEET_NAME]
		comparison_styles_sheet = comparison_styles_workbook[SHEET_NAME]
		template_sheet = template_workbook[SHEET_NAME]
		template_headers = _headers(template_sheet, TEMPLATE_HEADER_ROW)
		selections = _select_sources(comparison_styles_sheet)
		case_rows = _case_rows(comparison_sheet)

		if FINAL_SHEET_NAME in comparison_styles_workbook.sheetnames:
			del comparison_styles_workbook[FINAL_SHEET_NAME]
		final_sheet = comparison_styles_workbook.create_sheet(FINAL_SHEET_NAME)

		# Keep the legacy template header style for the case-reference column.
		_copy_template_header(
			template_sheet,
			final_sheet,
			template_headers[KEY_COLUMN],
			1,
		)
		final_sheet.cell(TEMPLATE_HEADER_ROW, 1).value = KEY_COLUMN
		row_one, key_anchor_column = _merged_anchor(
			comparison_styles_sheet, 1, _case_reference_column(comparison_styles_sheet)
		)
		final_sheet.cell(1, 1).value = comparison_styles_sheet.cell(
			row_one, key_anchor_column
		).value
		_copy_style(
			comparison_styles_sheet.cell(row_one, key_anchor_column),
			final_sheet.cell(1, 1),
		)
		final_sheet.column_dimensions["A"].width = template_sheet.column_dimensions[
			get_column_letter(template_headers[KEY_COLUMN])
		].width

		# Build one output column per selected field/source.
		output_columns = {}
		for output_column, selection in enumerate(selections, start=2):
			output_columns[selection["header"]] = output_column
			row_one, anchor_column = _merged_anchor(
				comparison_styles_sheet, 1, selection["primary"]
			)
			final_sheet.cell(1, output_column).value = comparison_styles_sheet.cell(
				row_one, anchor_column
			).value
			_copy_style(
				comparison_styles_sheet.cell(row_one, anchor_column),
				final_sheet.cell(1, output_column),
			)
			final_sheet.cell(TEMPLATE_HEADER_ROW, output_column).value = selection["header"]
			_copy_style(
				comparison_styles_sheet.cell(COMPARISON_COLOUR_ROW, selection["primary"]),
				final_sheet.cell(TEMPLATE_HEADER_ROW, output_column),
			)
			final_sheet.column_dimensions[get_column_letter(output_column)].width = (
				comparison_styles_sheet.column_dimensions[
					get_column_letter(selection["primary"])
				].width
			)
		_merge_repeated_row_one(final_sheet)

		normal_data_row = (
			TEMPLATE_FIRST_DATA_ROW + 1
			if template_sheet.max_row > TEMPLATE_FIRST_DATA_ROW
			else TEMPLATE_FIRST_DATA_ROW
		)
		normal_styles = [
			copy(template_sheet.cell(normal_data_row, column)._style)
			for column in range(1, template_sheet.max_column + 1)
		]

		output_row = TEMPLATE_FIRST_DATA_ROW
		for case_reference, source_rows in case_rows.items():
			final_sheet.cell(output_row, 1).value = case_reference
			for output_column, selection in enumerate(selections, start=2):
				value = _first_value(comparison_sheet, source_rows, selection["primary"])
				if _is_blank(value):
					value = _first_value(comparison_sheet, source_rows, selection["fallback"])
				final_sheet.cell(output_row, output_column).value = value
			output_row += 1

		final_sheet.freeze_panes = f"A{TEMPLATE_FIRST_DATA_ROW}"
		final_sheet.auto_filter.ref = (
			f"A{TEMPLATE_HEADER_ROW}:{get_column_letter(template_sheet.max_column)}"
			f"{output_row - 1}"
		)
		OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
		comparison_styles_workbook.save(OUTPUT_FILE)
		print(f"Wrote {output_row - TEMPLATE_FIRST_DATA_ROW} cases to {OUTPUT_FILE}")
		print(f"Retained {len(selections)} final columns; comparison Template preserved")
	finally:
		comparison_workbook.close()
		comparison_styles_workbook.close()
		template_workbook.close()


if __name__ == "__main__":
	extract_final_columns()
