"""Tests for utils_date.py."""
import pytest
import pandas as pd
from datetime import datetime

# Test for parse_date_to_std()
def test_parse_date_to_std_with_datetime():
    """Test parsing datetime objects."""
    from utils_date import parse_date_to_std

    # Test datetime object
    dt = datetime(2024, 1, 15)
    result = parse_date_to_std(dt)
    assert result == '2024-01-15'


def test_parse_date_to_std_with_string_formats():
    """Test parsing various string date formats."""
    from utils_date import parse_date_to_std

    test_cases = [
        ('2024-01-15', '2024-01-15'),
        ('20240115', '2024-01-15'),
        ('2024/01/15', '2024-01-15'),
        ('01/15/2024', '2024-01-15'),
        ('Jan 15, 2024', '2024-01-15')
    ]

    for input_date, expected in test_cases:
        result = parse_date_to_std(input_date)
        assert result == expected, f"Failed for {input_date}"


def test_parse_date_to_std_with_nat():
    """Test parsing pd.NaT returns None."""
    from utils_date import parse_date_to_std

    result = parse_date_to_std(pd.NaT)
    assert result is None


def test_parse_date_to_std_with_invalid_format():
    """Test parsing invalid date format returns None."""
    from utils_date import parse_date_to_std

    result = parse_date_to_std('invalid_date')
    assert result is None


def test_parse_date_to_std_with_timestamp():
    """Test parsing pandas Timestamp."""
    from utils_date import parse_date_to_std

    ts = pd.Timestamp('2024-01-15')
    result = parse_date_to_std(ts)
    assert result == '2024-01-15'
