"""Tests for hash check modules (Parts 7-9)."""
import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock

# Test for Part 7: hash_check_pcds.py - compute_vintage_hash()
@patch('hash_check_pcds.proc_pcds')
@patch('hash_check_pcds.build_oracle_hash_expr')
def test_compute_vintage_hash_normal_mode(mock_build_hash, mock_proc_pcds):
    """Test hash computation in normal mode (not debug)."""
    # Setup
    mock_build_hash.return_value = {
        'hash_expr': "RAWTOHEX(STANDARD_HASH(normalized_cols, 'SHA256'))",
        'concat_expr': "col1 || '|' || col2",
        'debug_select': ''
    }

    mock_proc_pcds.return_value = pd.DataFrame([
        {'acct_id': 1001, 'hash_value': 'ABC123DEF456'},
        {'acct_id': 1002, 'hash_value': 'DEF789GHI012'},
        {'acct_id': 1003, 'hash_value': 'ABC123DEF456'}  # Duplicate hash
    ])

    from hash_check_pcds import compute_vintage_hash

    columns_with_types = {'ACCT_ID': 'NUMBER', 'BALANCE': 'NUMBER'}
    key_columns = ['acct_id']
    vintage = {
        'vintage': 'M202401',
        'start_date': '2024-01-01',
        'end_date': '2024-01-31',
        'where_clause': "acct_date >= DATE '2024-01-01'"
    }

    args = ('customer', 'customer.account', columns_with_types, key_columns, vintage, False)
    result = compute_vintage_hash(args)

    # Verify
    assert result is not None
    assert result['total_rows'] == 3
    assert result['unique_hashes'] == 2  # Two unique hashes (one duplicate)
    assert len(result['hashes']) == 3


@patch('hash_check_pcds.proc_pcds')
@patch('hash_check_pcds.build_oracle_hash_expr')
def test_compute_vintage_hash_debug_mode(mock_build_hash, mock_proc_pcds):
    """Test hash computation in debug mode (shows normalized values)."""
    mock_build_hash.return_value = {
        'hash_expr': "RAWTOHEX(STANDARD_HASH(normalized_cols, 'SHA256'))",
        'concat_expr': "col1 || '|' || col2",
        'debug_select': 'normalized_acct_id, normalized_balance, __concat_string, __hash_hex'
    }

    mock_proc_pcds.return_value = pd.DataFrame([
        {
            'acct_id': 1001,
            'ACCT_ID': '1001.000',
            'BALANCE': '500.000',
            '__concat_string': '1001.000|500.000',
            '__hash_hex': 'ABC123DEF456'
        }
    ])

    from hash_check_pcds import compute_vintage_hash

    columns_with_types = {'ACCT_ID': 'NUMBER', 'BALANCE': 'NUMBER'}
    key_columns = ['acct_id']
    vintage = {'vintage': 'M202401', 'where_clause': '1=1'}

    args = ('customer', 'customer.account', columns_with_types, key_columns, vintage, True)
    result = compute_vintage_hash(args)

    # Debug mode returns raw DataFrame records
    assert isinstance(result, list)
    assert len(result) == 1
    assert '__concat_string' in result[0]
    assert '__hash_hex' in result[0]


# Test for Part 8: hash_check_aws.py - compute_table_hashes()
@patch('hash_check_aws.compute_vintage_hash')
def test_compute_table_hashes_parallel_execution(mock_compute_vintage):
    """Test parallel hash computation across vintages."""
    # Mock responses for each vintage
    vintage_results = [
        {'total_rows': 100, 'unique_hashes': 100, 'hashes': []},
        {'total_rows': 150, 'unique_hashes': 150, 'hashes': []},
        {'total_rows': 200, 'unique_hashes': 200, 'hashes': []}
    ]

    mock_compute_vintage.side_effect = vintage_results

    from hash_check_aws import compute_table_hashes

    columns_with_types = {'account_id': 'bigint', 'balance': 'double'}
    key_columns = ['account_id']
    vintages = [
        {'vintage': 'M202401', 'where_clause': "year_month = '2024-01'"},
        {'vintage': 'M202402', 'where_clause': "year_month = '2024-02'"},
        {'vintage': 'M202403', 'where_clause': "year_month = '2024-03'"}
    ]

    result = compute_table_hashes(
        'customer_db',
        'account',
        columns_with_types,
        key_columns,
        vintages,
        max_workers=3,
        debug=False
    )

    # Verify
    assert len(result) == 3
    assert all('total_rows' in v['hash_data'] for v in result)
    assert mock_compute_vintage.call_count == 3


# Test for Part 9: hash_check_compare.py - compare_vintage_hashes()
def test_compare_vintage_hashes_all_matched():
    """Test hash comparison with all rows matched."""
    pcds_hashes = {
        'hashes': [
            {'acct_id': 1001, 'hash_value': 'ABC123'},
            {'acct_id': 1002, 'hash_value': 'DEF456'},
            {'acct_id': 1003, 'hash_value': 'GHI789'}
        ]
    }

    aws_hashes = {
        'hashes': [
            {'acct_id': 1001, 'hash_value': 'ABC123'},
            {'acct_id': 1002, 'hash_value': 'DEF456'},
            {'acct_id': 1003, 'hash_value': 'GHI789'}
        ]
    }

    key_columns = ['acct_id']

    from hash_check_compare import compare_vintage_hashes

    result = compare_vintage_hashes(pcds_hashes, aws_hashes, key_columns)

    # Verify all matched
    assert result['pcds_rows'] == 3
    assert result['aws_rows'] == 3
    assert result['matched_rows'] == 3
    assert result['pcds_only_rows'] == 0
    assert result['aws_only_rows'] == 0
    assert result['hash_mismatch_rows'] == 0


def test_compare_vintage_hashes_with_mismatches():
    """Test hash comparison with mismatches and missing rows."""
    pcds_hashes = {
        'hashes': [
            {'acct_id': 1001, 'hash_value': 'ABC123'},
            {'acct_id': 1002, 'hash_value': 'DEF456'},  # Hash mismatch
            {'acct_id': 1003, 'hash_value': 'GHI789'}   # PCDS only
        ]
    }

    aws_hashes = {
        'hashes': [
            {'acct_id': 1001, 'hash_value': 'ABC123'},
            {'acct_id': 1002, 'hash_value': 'DIFFERENT'},  # Hash mismatch
            {'acct_id': 1004, 'hash_value': 'JKL012'}       # AWS only
        ]
    }

    key_columns = ['acct_id']

    from hash_check_compare import compare_vintage_hashes

    result = compare_vintage_hashes(pcds_hashes, aws_hashes, key_columns)

    # Verify
    assert result['pcds_rows'] == 3
    assert result['aws_rows'] == 3
    assert result['matched_rows'] == 1  # Only 1001 matched
    assert result['pcds_only_rows'] == 1  # 1003
    assert result['aws_only_rows'] == 1  # 1004
    assert result['hash_mismatch_rows'] == 1  # 1002
    assert len(result['sample_mismatches']) == 1
    assert result['sample_mismatches'][0]['acct_id'] == 1002


def test_compare_vintage_hashes_empty_results():
    """Test hash comparison with empty results."""
    pcds_hashes = {'hashes': []}
    aws_hashes = {'hashes': []}
    key_columns = ['acct_id']

    from hash_check_compare import compare_vintage_hashes

    result = compare_vintage_hashes(pcds_hashes, aws_hashes, key_columns)

    # Verify
    assert result['pcds_rows'] == 0
    assert result['aws_rows'] == 0
    assert result['matched_rows'] == 0
