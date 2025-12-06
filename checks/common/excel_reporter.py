# Excel Reporter for Data Validation Pipeline
# Adapted from src/end2end.py:536-680 with optimizations

import sys
from pathlib import Path
import pandas as pd
import xlwings as xw
from PIL import ImageColor

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

try:
    from upath import UPath
except ImportError:
    UPath = Path


#>>> Helper class for Excel styling <<<#
class XS:
    def __init__(self, ws):
        self.ws = ws

    def make_cell(self, pos, value):
        self.ws.range(pos).value = value

    def apply_styles(self, pos, value=None, font=None, color=None, align=None):
        rng = self.ws.range(pos)
        if value is not None:
            rng.value = value
        if font:
            for k, v in font.items():
                setattr(rng.font, k, v)
        if color:
            rng.color = color
        if align:
            rng.api.HorizontalAlignment = -4152 if align == 'right' else -4131

    def write_dataframe(self, df, pos, index=True):
        self.ws.range(pos).value = df if not index else df.reset_index()


#>>> Excel reporter for validation results <<<#
class ExcelReporter:

    #>>> Initialize reporter with workbook path <<<#
    def __init__(self, workbook_path: str):
        self.workbook_path = UPath(workbook_path)
        self.app = None
        self.wb = None
        self.ns = -1
        self.cx, self.cy = None, None

    def __enter__(self):
        try:
            self.workbook_path.unlink(True)
        except PermissionError:
            xw.Book(self.workbook_path).close()
        self.app = xw.App(visible=True, add_book=False)
        self.wb = self.app.books.add()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.wb:
            self.wb.save(str(self.workbook_path))

    #>>> Create summary sheet with overview <<<#
    def create_summary_sheet(self, title: str, headers: list, data_rows: list):
        ws = self.wb.sheets[0]
        ws.name = 'SUMMARY'
        ws.range('A1').value = title
        ws.range('A1').font.bold = True
        ws.range('A1').font.size = 14

        # Write headers
        header_range = f'A3:{chr(64 + len(headers))}3'
        ws.range('A3').value = headers
        ws.range(header_range).font.bold = True
        ws.range(header_range).color = (200, 200, 200)

        # Write data rows
        row = 4
        for data_row in data_rows:
            ws.range(f'A{row}').value = data_row
            row += 1

        ws.autofit()
        self.ns += 1

    #>>> Create detailed sheet for specific data <<<#
    def create_detail_sheet(self, sheet_name: str, sections: list):
        wb = self.wb
        sheet_name = sheet_name.upper()[:31]  # Excel limit

        try:
            ws = wb.sheets.add(sheet_name, after=wb.sheets[self.ns])
        except ValueError:
            ws = wb.sheets[sheet_name]
        finally:
            ws.clear()

        xs = XS(ws)
        row = 1

        for section in sections:
            # Section header
            xs.make_cell(pos=f'A{row}', value=section['title'])
            xs.apply_styles(f'A{row}:D{row}', font={'bold': True}, color=(190, 190, 190))
            row += 2

            # Section data
            if 'dataframe' in section:
                df = section['dataframe']
                xs.write_dataframe(df, f'A{row}', index=section.get('index', False))
                row += len(df) + 3
            elif 'rows' in section:
                for data_row in section['rows']:
                    ws.range(f'A{row}').value = data_row
                    row += 1
                row += 2

        ws.autofit()
        self.ns += 1

    #>>> Format cell value for display <<<#
    def _format_cell_value(self, value) -> str:
        if pd.isna(value):
            return ''
        elif isinstance(value, (int, float)):
            if isinstance(value, float) and value.is_integer():
                return str(int(value))
            return str(value)
        else:
            str_val = str(value)
            return str_val[:50] + '...' if len(str_val) > 50 else str_val

    #>>> Apply conditional coloring to range based on comparison <<<#
    def apply_comparison_colors(self, ws, start_row: int, end_row: int,
                                col_idx: int, comparison_col_idx: int):
        get_rgb = ImageColor.getrgb
        for row in range(start_row, end_row + 1):
            cell1 = ws[row, col_idx]
            cell2 = ws[row, comparison_col_idx]

            if cell1.value == cell2.value:
                cell1.font.color = get_rgb('green')
                cell2.font.color = get_rgb('green')
            else:
                cell1.font.color = get_rgb('red')
                cell2.font.color = get_rgb('red')


#>>> Create comparison report from results dictionary <<<#
def create_comparison_report(comparison_results: dict, output_path: str):
    with ExcelReporter(output_path) as reporter:
        reporter.create_comparison_report(comparison_results)
