"""
Analyze Excel spreadsheet to find caseReferences with inconsistent cell coloring.

For each caseReference that appears multiple times:
- Check each column for color inconsistencies
- Flag columns where some rows have BLUE color and other rows have NO COLOR (blank)
- Report the caseReference + column header for review

Usage:
    python analyze_color_mismatches.py
"""

import openpyxl
from openpyxl.styles import PatternFill, Font
from pathlib import Path
from collections import defaultdict

# File paths
INPUT_FILE = Path("/Users/nisalihalwathura/PINS/ODW-Service/odw-synapse-workspace/S62aMigration/outputs/MASTER LEGACY cases S62A - final.xlsx")
OUTPUT_FILE = Path("/Users/nisalihalwathura/PINS/ODW-Service/odw-synapse-workspace/S62aMigration/outputs/Color_Mismatch_Report.xlsx")

def is_blue_color(color_obj):
    """Check if a color is some shade of blue (including cyan, navy, light blue, etc).
    
    Handles both RGB and theme colors. For theme colors, checks if it's likely to be
    a blue color based on common Excel color schemes.
    """
    if not color_obj:
        return False
    
    # Handle theme colors (Excel theme-based colors)
    if hasattr(color_obj, 'type') and color_obj.type == 'theme':
        theme = getattr(color_obj, 'theme', None)
        # In Excel color schemes, themes 3-7 are accent colors
        # Themes 3 and 7 are commonly used for blue highlights
        # Theme 4 can also be blue-ish depending on scheme
        if theme in [3, 4, 7]:  # Common blue theme colors
            return True
        return False
    
    # Handle RGB colors
    if hasattr(color_obj, 'rgb') and color_obj.rgb:
        color_str = str(color_obj.rgb).upper()
    else:
        color_str = str(color_obj).upper()
    
    # Handle ARGB format (8 characters like 0000B0F0 or FFC7CE)
    # Remove leading zeros or alpha channel
    if len(color_str) >= 6:
        # If starts with '00' followed by 6 hex chars, it's ARGB with alpha=00
        if color_str.startswith('00') and len(color_str) == 8:
            color_str = color_str[2:]
        # If it's FFRRGGBB format
        elif color_str.startswith('FF') and len(color_str) == 8:
            color_str = color_str[2:]
    
    # Now should be 6 characters RRGGBB
    if len(color_str) == 6:
        try:
            r = int(color_str[0:2], 16)
            g = int(color_str[2:4], 16)
            b = int(color_str[4:6], 16)
            
            # Blue colors: B component is significantly higher than R and G
            # This catches: dark blue, light blue, cyan, navy, royal blue, etc.
            return b > r and b > g and (b - max(r, g)) > 20
        except ValueError:
            return False
    
    return False

def has_color(cell):
    """Check if cell has any fill color (not blank/white)."""
    if not cell.fill or not cell.fill.start_color:
        return False
    
    color = cell.fill.start_color
    if hasattr(color, 'rgb') and color.rgb:
        return str(color.rgb).upper() not in ('00000000', 'FFFFFFFF', '00FFFFFF', 'FFFFFFFF')
    
    return False

def main():
    # Load workbook
    if not INPUT_FILE.exists():
        print(f"❌ File not found: {INPUT_FILE}")
        return
    
    print(f"📂 Loading: {INPUT_FILE}")
    wb = openpyxl.load_workbook(INPUT_FILE)
    ws = wb['Template']
    
    # Find caseReference column and header row
    case_ref_col = None
    header_row = None
    
    for row_check in [1, 2, 3]:
        for col_idx in range(1, ws.max_column + 1):
            cell = ws.cell(row_check, col_idx)
            if cell.value and 'case' in str(cell.value).lower() and 'reference' in str(cell.value).lower():
                case_ref_col = col_idx
                header_row = row_check
                break
        if case_ref_col:
            break
    
    if not case_ref_col:
        print("❌ Could not find caseReference column")
        return
    
    print(f"✅ Found caseReference column at: {openpyxl.utils.get_column_letter(case_ref_col)} (row {header_row})")
    
    # Get all headers
    headers = {}
    for col_idx in range(1, ws.max_column + 1):
        header_cell = ws.cell(header_row, col_idx)
        headers[col_idx] = header_cell.value or f"Col{col_idx}"
    
    # Read all data grouped by caseReference
    case_rows = defaultdict(list)
    
    for row_idx in range(header_row + 1, ws.max_row + 1):
        case_ref_cell = ws.cell(row_idx, case_ref_col)
        case_ref = case_ref_cell.value
        
        if not case_ref or str(case_ref).strip() == '':
            continue
        
        case_ref = str(case_ref).strip()
        case_rows[case_ref].append(row_idx)
    
    # Find cases with multiple occurrences
    multi_occurrence_cases = {k: v for k, v in case_rows.items() if len(v) > 1}
    
    print(f"\n📊 Data Analysis:")
    print(f"  Total unique caseReferences: {len(case_rows)}")
    print(f"  CaseReferences with multiple rows: {len(multi_occurrence_cases)}")
    
    # Find columns with color inconsistencies (blue vs blank)
    mismatches = {}  # {case_ref: {col_idx: [rows_data]}}
    
    for case_ref, row_indices in multi_occurrence_cases.items():
        mismatches[case_ref] = {}
        
        # Check each column
        for col_idx in range(1, ws.max_column + 1):
            colors_in_column = []
            
            # Get color status for each row in this case
            for row_idx in row_indices:
                cell = ws.cell(row_idx, col_idx)
                
                # Determine color status: blue, colored (non-blue), or blank
                color_obj = cell.fill.start_color if cell.fill and cell.fill.start_color else None
                
                if is_blue_color(color_obj):
                    color_status = 'blue'
                elif has_color(cell):
                    color_status = 'colored'
                else:
                    color_status = 'blank'
                
                colors_in_column.append({
                    'row': row_idx,
                    'value': cell.value,
                    'status': color_status
                })
            
            # Check if there's a mismatch: blue and blank in same column
            statuses = set(c['status'] for c in colors_in_column)
            has_blue = 'blue' in statuses
            has_blank = 'blank' in statuses
            
            # Flag if there's a mix of blue and blank (or blue and other colors)
            if has_blue and has_blank:
                mismatches[case_ref][col_idx] = colors_in_column
    
    # Filter out cases with no mismatches
    mismatches = {k: v for k, v in mismatches.items() if v}
    
    print(f"  CaseReferences with color inconsistencies: {len(mismatches)}")
    
    # Create output workbook
    output_wb = openpyxl.Workbook()
    
    # Remove default sheet
    if 'Sheet' in output_wb.sheetnames:
        output_wb.remove(output_wb['Sheet'])
    
    # Create summary sheet
    summary_ws = output_wb.create_sheet("Summary", 0)
    summary_ws['A1'] = "CaseReference"
    summary_ws['B1'] = "Column Header"
    summary_ws['C1'] = "Column #"
    summary_ws['D1'] = "Issue Description"
    summary_ws['E1'] = "Total Rows for Case"
    
    summary_row = 2
    
    for case_ref in sorted(mismatches.keys()):
        mismatch_cols = mismatches[case_ref]
        num_rows = len(multi_occurrence_cases[case_ref])
        
        for col_idx in sorted(mismatch_cols.keys()):
            colors_info = mismatch_cols[col_idx]
            col_header = headers[col_idx]
            
            # Count blue vs blank
            blue_count = sum(1 for c in colors_info if c['status'] == 'blue')
            blank_count = sum(1 for c in colors_info if c['status'] == 'blank')
            
            issue = f"Mixed coloring: {blue_count} blue cell(s), {blank_count} blank cell(s)"
            
            summary_ws[f'A{summary_row}'] = case_ref
            summary_ws[f'B{summary_row}'] = col_header
            summary_ws[f'C{summary_row}'] = col_idx
            summary_ws[f'D{summary_row}'] = issue
            summary_ws[f'E{summary_row}'] = num_rows
            
            summary_row += 1
    
    # Adjust column widths
    summary_ws.column_dimensions['A'].width = 20
    summary_ws.column_dimensions['B'].width = 40
    summary_ws.column_dimensions['C'].width = 10
    summary_ws.column_dimensions['D'].width = 40
    summary_ws.column_dimensions['E'].width = 15
    
    # Create detailed sheet showing all rows for each case+column mismatch
    detail_ws = output_wb.create_sheet("Details", 1)
    detail_ws['A1'] = "CaseReference"
    detail_ws['B1'] = "Column Header"
    detail_ws['C1'] = "Row #"
    detail_ws['D1'] = "Value"
    detail_ws['E1'] = "Color Status"
    
    detail_row = 2
    
    for case_ref in sorted(mismatches.keys()):
        mismatch_cols = mismatches[case_ref]
        
        for col_idx in sorted(mismatch_cols.keys()):
            colors_info = mismatch_cols[col_idx]
            col_header = headers[col_idx]
            
            for color_info in sorted(colors_info, key=lambda x: x['row']):
                detail_ws[f'A{detail_row}'] = case_ref
                detail_ws[f'B{detail_row}'] = col_header
                detail_ws[f'C{detail_row}'] = color_info['row']
                detail_ws[f'D{detail_row}'] = color_info['value']
                detail_ws[f'E{detail_row}'] = color_info['status'].upper()
                
                # Highlight the status column for easy scanning
                if color_info['status'] == 'blue':
                    detail_ws[f'E{detail_row}'].fill = PatternFill(
                        start_color="ADD8E6", end_color="ADD8E6", fill_type="solid"
                    )
                elif color_info['status'] == 'blank':
                    detail_ws[f'E{detail_row}'].fill = PatternFill(
                        start_color="FFFF00", end_color="FFFF00", fill_type="solid"
                    )
                
                detail_row += 1
    
    # Adjust column widths
    detail_ws.column_dimensions['A'].width = 20
    detail_ws.column_dimensions['B'].width = 40
    detail_ws.column_dimensions['C'].width = 10
    detail_ws.column_dimensions['D'].width = 30
    detail_ws.column_dimensions['E'].width = 15
    
    # Create a sheet specifically for BLUE cells with BLANK DATA
    blue_blank_ws = output_wb.create_sheet("Blue Cells with Blank Data", 2)
    blue_blank_ws['A1'] = "CaseReference"
    blue_blank_ws['B1'] = "Column Header"
    blue_blank_ws['C1'] = "Row #"
    blue_blank_ws['D1'] = "Value"
    blue_blank_ws['E1'] = "Data Status"
    blue_blank_ws['F1'] = "Issue"
    
    blue_blank_row = 2
    
    for case_ref in sorted(mismatches.keys()):
        mismatch_cols = mismatches[case_ref]
        
        for col_idx in sorted(mismatch_cols.keys()):
            colors_info = mismatch_cols[col_idx]
            col_header = headers[col_idx]
            
            # Find blue cells with blank data
            blue_blank_cells = [c for c in colors_info if c['status'] == 'blue' and (c['value'] is None or str(c['value']).strip() == '')]
            
            # Only include if there are blue cells with blank data
            if blue_blank_cells:
                for color_info in sorted(blue_blank_cells, key=lambda x: x['row']):
                    blue_blank_ws[f'A{blue_blank_row}'] = case_ref
                    blue_blank_ws[f'B{blue_blank_row}'] = col_header
                    blue_blank_ws[f'C{blue_blank_row}'] = color_info['row']
                    blue_blank_ws[f'D{blue_blank_row}'] = "[EMPTY]" if color_info['value'] is None else f"[WHITESPACE]: '{color_info['value']}'"
                    blue_blank_ws[f'E{blue_blank_row}'] = "BLANK"
                    blue_blank_ws[f'F{blue_blank_row}'] = "Blue highlighted cell with no data - possible incomplete data entry"
                    
                    # Highlight row in red to draw attention
                    for col_letter in ['A', 'B', 'C', 'D', 'E', 'F']:
                        blue_blank_ws[f'{col_letter}{blue_blank_row}'].fill = PatternFill(
                            start_color="FF6B6B", end_color="FF6B6B", fill_type="solid"
                        )
                        # Make text white for better contrast
                        blue_blank_ws[f'{col_letter}{blue_blank_row}'].font = Font(color="FFFFFF", bold=True)
                    
                    blue_blank_row += 1
    
    # Adjust column widths
    blue_blank_ws.column_dimensions['A'].width = 20
    blue_blank_ws.column_dimensions['B'].width = 40
    blue_blank_ws.column_dimensions['C'].width = 10
    blue_blank_ws.column_dimensions['D'].width = 30
    blue_blank_ws.column_dimensions['E'].width = 15
    blue_blank_ws.column_dimensions['F'].width = 50
    
    # Save output
    output_wb.save(OUTPUT_FILE)
    print(f"\n✅ Report generated: {OUTPUT_FILE}")
    
    print(f"\n📋 Summary of Inconsistencies:")
    print(f"  - 'Summary' sheet: All cases + columns with blue/blank color mismatches")
    print(f"  - 'Details' sheet: All rows for each identified mismatch (blue highlighted, blank highlighted yellow)")
    print(f"  - 'Blue Cells with Blank Data' sheet: ⚠️ PRIORITY - Blue highlighted cells that contain no data (red highlighted)")
    
    total_issues = sum(len(v) for v in mismatches.values())
    print(f"\nTotal column mismatches found: {total_issues}")
    
    # Count blue cells with blank data
    blue_blank_count = 0
    for case_ref in mismatches.keys():
        for col_idx in mismatches[case_ref].keys():
            colors_info = mismatches[case_ref][col_idx]
            blue_blank_count += sum(1 for c in colors_info if c['status'] == 'blue' and (c['value'] is None or str(c['value']).strip() == ''))
    
    print(f"Blue cells with blank data found: {blue_blank_count}")
    
    # Show sample
    if mismatches:
        print(f"\nSample of issues (first 5):")
        count = 0
        for case_ref in sorted(mismatches.keys()):
            for col_idx in sorted(mismatches[case_ref].keys()):
                if count >= 5:
                    break
                col_header = headers[col_idx]
                colors_info = mismatches[case_ref][col_idx]
                blue_count = sum(1 for c in colors_info if c['status'] == 'blue')
                blank_count = sum(1 for c in colors_info if c['status'] == 'blank')
                print(f"  {case_ref} | {col_header} | {blue_count} blue, {blank_count} blank")
                count += 1
            if count >= 5:
                break

if __name__ == "__main__":
    main()
