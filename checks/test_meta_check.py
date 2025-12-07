"""Tests for meta check modules (Parts 1-3)."""
import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock

# Test for Part 1: meta_check_pcds.py - check_crosswalk()
def test_check_crosswalk_with_comparable_columns():
    """Test crosswalk classification with comparable columns."""
    # Setup
    table_name = "customer.account"
    columns_df = pd.DataFrame({
        'column_name': ['ACCT_ID', 'ACCT_NAME', 'SSN', 'BALANCE', 'EXTRA_COL'],
        'data_type': ['NUMBER', 'VARCHAR2', 'VARCHAR2', 'NUMBER', 'VARCHAR2']
    })

    crosswalk_df = pd.DataFrame({
        'pcds_column': ['acct_id', 'acct_name', 'ssn'],
        'aws_column': ['account_id', 'account_name', 'ssn_token'],
        'pcds_type': ['NUMBER', 'VARCHAR2', 'VARCHAR2'],
        'aws_type': ['bigint', 'string', 'string'],
        'category': ['comparable', 'comparable', 'tokenized'],
        'col_map_name': ['account', 'account', 'account']
    })

    col_map_name = 'account'

    # Import function (mock if needed)
    from meta_check_pcds import check_crosswalk

    # Execute
    result = check_crosswalk(table_name, columns_df, crosswalk_df, col_map_name)

    # Verify
    assert 'comparable' in result
    assert 'tokenized' in result
    assert 'pcds_only' in result
    assert 'unmapped' in result

    assert 'ACCT_ID' in result['comparable']
    assert 'ACCT_NAME' in result['comparable']
    assert 'SSN' in result['tokenized']
    assert 'EXTRA_COL' in result['unmapped']  # Not in crosswalk
    assert 'BALANCE' in result['unmapped']  # Not in crosswalk


def test_check_crosswalk_empty_crosswalk():
    """Test crosswalk with empty crosswalk file (all columns should be unmapped)."""
    table_name = "customer.account"
    columns_df = pd.DataFrame({
        'column_name': ['ACCT_ID', 'ACCT_NAME'],
        'data_type': ['NUMBER', 'VARCHAR2']
    })

    crosswalk_df = pd.DataFrame({
        'pcds_column': [],
        'aws_column': [],
        'pcds_type': [],
        'aws_type': [],
        'category': [],
        'col_map_name': []
    })

    col_map_name = 'account'

    from meta_check_pcds import check_crosswalk

    result = check_crosswalk(table_name, columns_df, crosswalk_df, col_map_name)

    # All columns should be unmapped
    assert len(result['comparable']) == 0
    assert len(result['tokenized']) == 0
    assert len(result['unmapped']) == 2


# Test for Part 2: meta_check_aws.py - get_row_counts()
@patch('meta_check_aws.proc_aws')
def test_get_row_counts_with_mixed_date_formats(mock_proc_aws):
    """Test row counts with mixed date formats in result."""
    # Mock database response
    mock_proc_aws.return_value = pd.DataFrame({
        'acct_date': ['2024-01-01', '20240102', '2024-01-03'],
        'cnt': [100, 150, 200]
    })

    from meta_check_aws import get_row_counts

    result = get_row_counts('customer_db', 'account', 'acct_date')

    # Verify date_std column added with standardized dates
    assert 'date_std' in result.columns
    assert len(result) == 3
    assert result['cnt'].sum() == 450

    # Check dates parsed correctly
    expected_dates = ['2024-01-01', '2024-01-02', '2024-01-03']
    assert all(d in result['date_std'].values for d in expected_dates)


# Test for Part 3: compare_report.py - prepare_table_sections()
def test_prepare_table_sections_with_complete_data():
    """Test section preparation with complete PCDS and AWS results."""
    pcds_result = {
        'table': 'customer.account',
        'accessible': True,
        'row_counts': [
            {'date_std': '2024-01-01', 'cnt': 100},
            {'date_std': '2024-01-02', 'cnt': 150}
        ],
        'crosswalk': {
            'comparable': ['ACCT_ID', 'ACCT_NAME'],
            'tokenized': ['SSN'],
            'pcds_only': ['EXTRA_COL'],
            'aws_only': [],
            'unmapped': []
        },
        'vintages': [
            {'vintage': 'M202401', 'start_date': '2024-01-01', 'end_date': '2024-01-31'}
        ]
    }

    aws_result = {
        'table': 'customer_db.account',
        'accessible': True,
        'row_counts': [
            {'date_std': '2024-01-01', 'cnt': 100},
            {'date_std': '2024-01-02', 'cnt': 150}
        ],
        'crosswalk': {
            'comparable': ['account_id', 'account_name'],
            'tokenized': ['ssn_token'],
            'pcds_only': [],
            'aws_only': [],
            'unmapped': []
        },
        'vintages': [
            {'vintage': 'M202401', 'start_date': '2024-01-01', 'end_date': '2024-01-31'}
        ]
    }

    from compare_report import prepare_table_sections

    sections = prepare_table_sections(pcds_result, aws_result)

    # Verify sections created
    assert len(sections) >= 3  # Table info, row counts, crosswalk summary
    assert sections[0]['title'] == 'Table Information'
    assert any('Row Counts' in s['title'] for s in sections)
    assert any('Crosswalk' in s['title'] for s in sections)


def test_prepare_table_sections_with_missing_data():
    """Test section preparation with missing row counts and crosswalk."""
    pcds_result = {
        'table': 'customer.account',
        'accessible': False,
        'row_counts': None,
        'crosswalk': None,
        'vintages': None
    }

    aws_result = {
        'table': 'customer_db.account',
        'accessible': False,
        'row_counts': None,
        'crosswalk': None,
        'vintages': None
    }

    from compare_report import prepare_table_sections

    sections = prepare_table_sections(pcds_result, aws_result)

    # Should only have table info section
    assert len(sections) == 1
    assert sections[0]['title'] == 'Table Information'
