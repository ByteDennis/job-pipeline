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
    def create_summary_sheet(self, title: str, headers: list, data_rows: list, color_by_match_rate: bool = False):
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

            # Apply color coding based on match rate if requested
            if color_by_match_rate and len(data_row) >= 6:
                # Assumes last column contains match rate as string like "95.0%"
                match_rate_str = data_row[-1]
                try:
                    match_rate = float(match_rate_str.rstrip('%'))
                    num_cols = len(headers)
                    row_range = f'A{row}:{chr(64 + num_cols)}{row}'
                    if match_rate >= 95:
                        ws.range(row_range).color = (200, 255, 200)  # Light green
                    else:
                        ws.range(row_range).color = (255, 200, 200)  # Light red
                except (ValueError, AttributeError):
                    pass

            row += 1

        # Right-align numeric columns (skip first column which is usually table name)
        if data_rows:
            num_cols = len(headers)
            data_range = f'B4:{chr(64 + num_cols)}{row - 1}'
            ws.range(data_range).api.HorizontalAlignment = -4152  # xlRight

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
                xs.write_dataframe(df, f'A{row}', index=False)
                row += len(df) + 3
            elif 'rows' in section:
                for data_row in section['rows']:
                    ws.range(f'A{row}').value = data_row
                    row += 1
                row += 2
            elif 'comparison' in section:
                # Handle PCDS vs AWS comparison sections
                comp = section['comparison']
                self._render_comparison_section(xs, ws, row, comp)
                row = comp['next_row']

        ws.autofit()
        self.ns += 1

    #>>> Create column comparison sheet (vintage-based transposed format) <<<#
    def create_column_comparison_sheet(self, sheet_name: str, sections: list):
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
            # Vintage header
            xs.make_cell(pos=f'A{row}', value='Vintage: ')
            xs.apply_styles(pos=f'B{row}', value=section['vintage'], align='right')
            ws.range(f'B{row}:D{row}').merge()
            xs.apply_styles(f'A{row}:D{row}', font={'bold': True}, color=(190, 190, 190))
            row += 2

            # PCDS Statistics Header
            xs.make_cell(pos=f'A{row}', value='PCDS: ')
            xs.apply_styles(pos=f'B{row}', value=section['pcds_label'], align='right')
            ws.range(f'B{row}:D{row}').merge()
            xs.apply_styles(f'A{row}:D{row}', font={'bold': True}, color=(240, 240, 240))
            row += 1

            # PCDS Statistics Data (transposed: stats as rows, columns as columns)
            pcds_df = section['pcds_df']
            pcds_start_row = row
            xs.write_dataframe(pcds_df, f'B{row}', index=True)

            # AWS Statistics Header
            row += len(pcds_df) + 2
            xs.make_cell(pos=f'A{row}', value='AWS: ')
            xs.apply_styles(pos=f'B{row}', value=section['aws_label'], align='right')
            ws.range(f'B{row}:D{row}').merge()
            xs.apply_styles(f'A{row}:D{row}', font={'bold': True}, color=(240, 240, 240))
            row += 1

            # AWS Statistics Data (transposed: stats as rows, columns as columns)
            aws_df = section['aws_df']
            aws_start_row = row
            xs.write_dataframe(aws_df, f'B{row}', index=True)
            row += len(aws_df) + 3

            # Highlight differences in mismatched columns
            self._highlight_differences(
                ws,
                nx=section['num_mismatched'],
                ny=len(pcds_df) - 1,  # -1 to exclude header row
                pcds_start_row=pcds_start_row,
                aws_start_row=aws_start_row
            )

        ws.autofit()
        self.ns += 1

    #>>> Render comparison section (PCDS vs AWS) <<<#
    def _render_comparison_section(self, xs, ws, start_row: int, comp_data: dict):
        """Render a comparison section with PCDS and AWS data side by side

        Args:
            comp_data: dict with keys:
                - pcds_label: str (e.g., 'PCDS: table_name')
                - aws_label: str (e.g., 'AWS: table_name')
                - pcds_df: DataFrame (transposed stats)
                - aws_df: DataFrame (transposed stats)
                - mismatched_columns: list of column names
                - next_row: int (updated by this function)
        """
        row = start_row

        # PCDS header
        xs.make_cell(pos=f'A{row}', value='PCDS: ')
        xs.apply_styles(pos=f'B{row}', value=comp_data['pcds_label'], align='right')
        ws.range(f'B{row}:D{row}').merge()
        xs.apply_styles(f'A{row}:D{row}', font={'bold': True}, color=(240, 240, 240))
        row += 1

        # Write PCDS data
        pcds_df = comp_data['pcds_df']
        pcds_start_row = row
        xs.write_dataframe(pcds_df, f'B{row}', index=True)
        row += len(pcds_df) + 2

        # AWS header
        xs.make_cell(pos=f'A{row}', value='AWS: ')
        xs.apply_styles(pos=f'B{row}', value=comp_data['aws_label'], align='right')
        ws.range(f'B{row}:D{row}').merge()
        xs.apply_styles(f'A{row}:D{row}', font={'bold': True}, color=(240, 240, 240))
        row += 1

        # Write AWS data
        aws_df = comp_data['aws_df']
        aws_start_row = row
        xs.write_dataframe(aws_df, f'B{row}', index=True)
        row += len(aws_df) + 3

        # Highlight differences in mismatched columns
        self._highlight_comparison_differences(
            ws,
            pcds_start_row=pcds_start_row + 1,  # +1 to skip header row
            aws_start_row=aws_start_row + 1,
            num_stat_rows=len(pcds_df) - 1,  # -1 to exclude header
            mismatched_columns=comp_data.get('mismatched_columns', []),
            all_columns=list(pcds_df.columns)
        )

        comp_data['next_row'] = row

    #>>> Highlight differences between PCDS and AWS <<<#
    def _highlight_differences(self, ws, nx: int, ny: int, pcds_start_row: int, aws_start_row: int):
        """Highlight differences between PCDS and AWS statistics for mismatched columns

        Args:
            ws: Excel worksheet
            nx: Number of mismatched columns to highlight
            ny: Number of statistic rows (excluding header)
            pcds_start_row: Row where PCDS stats start (1-indexed, includes header)
            aws_start_row: Row where AWS stats start (1-indexed, includes header)
        """
        get_rgb = ImageColor.getrgb

        # Start from row after header (stats start at +1)
        pcds_data_row = pcds_start_row + 1
        aws_data_row = aws_start_row + 1

        # Start from column B (column index 2)
        start_col = 2

        for i in range(ny):  # For each stat row
            for j in range(nx):  # For each mismatched column
                pcds_cell = ws[pcds_data_row + i, start_col + j]
                aws_cell = ws[aws_data_row + i, start_col + j]

                # Try to format as numbers
                try:
                    pcds_cell.number_format = '0.00'
                    aws_cell.number_format = '0.00'
                except:
                    pass

                # Compare and color
                if pcds_cell.value == aws_cell.value:
                    pcds_cell.font.color = get_rgb('green')
                    aws_cell.font.color = get_rgb('green')
                else:
                    pcds_cell.font.color = get_rgb('red')
                    aws_cell.font.color = get_rgb('red')

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
