"""Tests for utils_xlsx.py."""
import pytest
import os
import tempfile
from unittest.mock import Mock, patch, MagicMock

# Test for create_summary_sheet()
@patch('utils_xlsx.xlwings')
def test_create_summary_sheet_basic(mock_xlwings):
    """Test creating summary sheet with basic data."""
    # Mock Excel objects
    mock_wb = Mock()
    mock_sheet = Mock()
    mock_range = Mock()

    mock_wb.sheets.add.return_value = mock_sheet
    mock_sheet.range.return_value = mock_range
    mock_range.api.Font.Size = 11

    from utils_xlsx import ExcelReporter

    # Create temporary file
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
        temp_path = f.name

    try:
        reporter = ExcelReporter(temp_path)
        reporter.wb = mock_wb  # Inject mock

        title = 'Meta Check Summary'
        headers = ['Table', 'PCDS Access', 'AWS Access', 'PCDS Rows', 'AWS Rows', 'Match']
        data_rows = [
            ['customer.account', '✓', '✓', 1000, 1000, '✓'],
            ['customer.order', '✓', '✓', 500, 500, '✓']
        ]

        reporter.create_summary_sheet(title, headers, data_rows)

        # Verify sheet created
        mock_wb.sheets.add.assert_called_once_with('SUMMARY')

        # Verify title written
        assert mock_sheet.range.call_count > 0

    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)


@patch('utils_xlsx.xlwings')
def test_create_summary_sheet_right_alignment(mock_xlwings):
    """Test right-alignment applied to numeric columns."""
    mock_wb = Mock()
    mock_sheet = Mock()
    mock_range = Mock()

    mock_wb.sheets.add.return_value = mock_sheet
    mock_sheet.range.return_value = mock_range

    # Track alignment calls
    alignment_calls = []

    def track_alignment(value):
        alignment_calls.append(value)

    type(mock_range.api).HorizontalAlignment = property(None, track_alignment)

    from utils_xlsx import ExcelReporter

    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
        temp_path = f.name

    try:
        reporter = ExcelReporter(temp_path)
        reporter.wb = mock_wb

        headers = ['Table', 'Count', 'Match']
        data_rows = [
            ['customer.account', 1000, '✓'],
            ['customer.order', 500, '✓']
        ]

        reporter.create_summary_sheet('Summary', headers, data_rows)

        # Verify right-alignment applied (xlRight = -4152)
        assert -4152 in alignment_calls

    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)


@patch('utils_xlsx.xlwings')
def test_create_summary_sheet_empty_data(mock_xlwings):
    """Test creating summary sheet with empty data rows."""
    mock_wb = Mock()
    mock_sheet = Mock()
    mock_range = Mock()

    mock_wb.sheets.add.return_value = mock_sheet
    mock_sheet.range.return_value = mock_range

    from utils_xlsx import ExcelReporter

    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
        temp_path = f.name

    try:
        reporter = ExcelReporter(temp_path)
        reporter.wb = mock_wb

        headers = ['Table', 'Count']
        data_rows = []  # Empty data

        reporter.create_summary_sheet('Summary', headers, data_rows)

        # Should still create sheet with headers
        mock_wb.sheets.add.assert_called_once()

    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)
