"""Tests for column check modules (Parts 4-6)."""
import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from concurrent.futures import ThreadPoolExecutor

# Test for Part 4: column_check_pcds.py - get_column_stats()
@patch('column_check_pcds.proc_pcds')
@patch('column_check_pcds.build_column_sql')
def test_get_column_stats_continuous_variable(mock_build_sql, mock_proc_pcds):
    """Test column stats for continuous numeric variable."""
    # Setup
    mock_build_sql.return_value = "SELECT COUNT(*) AS count, MIN(balance) AS min FROM customer.account"

    mock_proc_pcds.return_value = pd.DataFrame([{
        'col_name': 'BALANCE',
        'col_type': 'NUMBER',
        'col_category': 'continuous',
        'count': 1000,
        'distinct': 850,
        'min': 0.0,
        'max': 9999.99,
        'avg': 1234.56,
        'std': 567.89,
        'missing': 0,
        'freq_top10': ''
    }])

    from column_check_pcds import get_column_stats

    args = ('customer', 'customer.account', 'BALANCE', 'NUMBER', 'acct_date >= DATE \'2024-01-01\'')
    result = get_column_stats(args)

    # Verify
    assert result is not None
    assert result['col_name'] == 'BALANCE'
    assert result['col_category'] == 'continuous'
    assert result['count'] == 1000
    assert result['distinct'] == 850
    assert result['freq_top10'] == ''  # No frequency for continuous


@patch('column_check_pcds.proc_pcds')
@patch('column_check_pcds.build_column_sql')
def test_get_column_stats_categorical_variable(mock_build_sql, mock_proc_pcds):
    """Test column stats for categorical string variable."""
    mock_build_sql.return_value = "SELECT col_freq FROM ..."

    mock_proc_pcds.return_value = pd.DataFrame([{
        'col_name': 'STATUS',
        'col_type': 'VARCHAR2',
        'col_category': 'categorical',
        'count': 1000,
        'distinct': 3,
        'min': None,
        'max': None,
        'avg': None,
        'std': None,
        'missing': 10,
        'freq_top10': 'ACTIVE:500,INACTIVE:300,PENDING:200'
    }])

    from column_check_pcds import get_column_stats

    args = ('customer', 'customer.account', 'STATUS', 'VARCHAR2', '1=1')
    result = get_column_stats(args)

    assert result['col_category'] == 'categorical'
    assert result['distinct'] == 3
    assert 'ACTIVE' in result['freq_top10']


# Test for Part 5: column_check_aws.py - get_vintage_stats()
@patch('column_check_aws.get_column_stats')
def test_get_vintage_stats_parallel_execution(mock_get_stats):
    """Test parallel execution with ThreadPoolExecutor."""
    # Mock column stats returning different results for each column
    def mock_stats_response(args):
        database, table, col_name, col_type, where = args
        return {
            'col_name': col_name,
            'count': 1000,
            'distinct': 500,
            'col_category': 'continuous' if 'AMOUNT' in col_name else 'categorical'
        }

    mock_get_stats.side_effect = mock_stats_response

    from column_check_aws import get_vintage_stats

    columns_with_types = {
        'account_id': 'bigint',
        'amount': 'double',
        'status': 'string'
    }

    vintage = {
        'vintage': 'M202401',
        'where_clause': 'year_month = \'2024-01\''
    }

    result = get_vintage_stats(
        'customer_db',
        'account',
        columns_with_types,
        vintage,
        max_workers=3
    )

    # Verify all columns processed
    assert 'account_id' in result
    assert 'amount' in result
    assert 'status' in result
    assert len(result) == 3


# Test for Part 6: column_check_compare.py - analyze_column_quality()
def test_analyze_column_quality_identifies_mismatches():
    """Test identification of mismatched columns across vintages."""
    pcds_result = {
        'table': 'customer.account',
        'columns': ['ACCT_ID', 'BALANCE', 'STATUS'],
        'vintage_stats': [
            {
                'vintage': 'M202401',
                'stats': {
                    'ACCT_ID': {'count': 1000, 'distinct': 1000},
                    'BALANCE': {'count': 1000, 'distinct': 850},
                    'STATUS': {'count': 1000, 'distinct': 3}
                }
            },
            {
                'vintage': 'M202402',
                'stats': {
                    'ACCT_ID': {'count': 1100, 'distinct': 1100},
                    'BALANCE': {'count': 1100, 'distinct': 920},  # Mismatch on count!
                    'STATUS': {'count': 1100, 'distinct': 3}
                }
            }
        ]
    }

    aws_result = {
        'table': 'customer_db.account',
        'columns': ['account_id', 'balance', 'status'],
        'vintage_stats': [
            {
                'vintage': 'M202401',
                'stats': {
                    'ACCT_ID': {'count': 1000, 'distinct': 1000},
                    'BALANCE': {'count': 1000, 'distinct': 850},
                    'STATUS': {'count': 1000, 'distinct': 3}
                }
            },
            {
                'vintage': 'M202402',
                'stats': {
                    'ACCT_ID': {'count': 1100, 'distinct': 1100},
                    'BALANCE': {'count': 1050, 'distinct': 920},  # COUNT MISMATCH!
                    'STATUS': {'count': 1100, 'distinct': 3}
                }
            }
        ]
    }

    from column_check_compare import analyze_column_quality

    result = analyze_column_quality(pcds_result, aws_result)

    # Verify
    assert 'BALANCE' in result['mismatched_columns']  # Failed in vintage M202402
    assert 'ACCT_ID' in result['clean_columns']  # Matched in all vintages
    assert 'STATUS' in result['clean_columns']

    # Verify top key columns selected from clean columns only
    assert result['top_key_columns'] == ['ACCT_ID']  # Highest distinct count
    assert 'BALANCE' not in result['top_key_columns']  # Excluded (mismatched)


def test_analyze_column_quality_top_3_key_columns():
    """Test selection of top 3 key columns by distinct count."""
    pcds_result = {
        'table': 'customer.account',
        'columns': ['COL_A', 'COL_B', 'COL_C', 'COL_D', 'COL_E'],
        'vintage_stats': [
            {
                'vintage': 'M202401',
                'stats': {
                    'COL_A': {'count': 1000, 'distinct': 1000},  # Highest
                    'COL_B': {'count': 1000, 'distinct': 500},   # 2nd
                    'COL_C': {'count': 1000, 'distinct': 300},   # 3rd
                    'COL_D': {'count': 1000, 'distinct': 100},   # 4th
                    'COL_E': {'count': 1000, 'distinct': 10}     # 5th
                }
            }
        ]
    }

    aws_result = {
        'table': 'customer_db.account',
        'columns': ['COL_A', 'COL_B', 'COL_C', 'COL_D', 'COL_E'],
        'vintage_stats': [
            {
                'vintage': 'M202401',
                'stats': {
                    'COL_A': {'count': 1000, 'distinct': 1000},
                    'COL_B': {'count': 1000, 'distinct': 500},
                    'COL_C': {'count': 1000, 'distinct': 300},
                    'COL_D': {'count': 1000, 'distinct': 100},
                    'COL_E': {'count': 1000, 'distinct': 10}
                }
            }
        ]
    }

    from column_check_compare import analyze_column_quality

    result = analyze_column_quality(pcds_result, aws_result)

    # Verify top 3 by distinct count
    assert result['top_key_columns'] == ['COL_A', 'COL_B', 'COL_C']
    assert len(result['top_key_columns']) == 3
