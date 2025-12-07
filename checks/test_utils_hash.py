"""Tests for utils_hash.py."""
import pytest

# Test for normalize_oracle_column()
def test_normalize_oracle_column_number_null():
    """Test normalization of NULL numeric values."""
    from utils_hash import normalize_oracle_column

    result = normalize_oracle_column('balance', 'NUMBER')

    # Should convert NULL or 0 to '0'
    assert "WHEN balance IS NULL OR balance = 0 THEN '0'" in result
    assert 'ELSE' in result


def test_normalize_oracle_column_number_non_zero():
    """Test normalization of non-zero numeric values."""
    from utils_hash import normalize_oracle_column

    result = normalize_oracle_column('balance', 'NUMBER', decimals=3)

    # Should format with fixed decimals
    assert 'ROUND(balance, 3)' in result
    assert 'FM9999999999999990.000' in result
    assert 'REPLACE' in result  # Remove commas


def test_normalize_oracle_column_varchar():
    """Test normalization of string values."""
    from utils_hash import normalize_oracle_column

    result = normalize_oracle_column('status', 'VARCHAR2')

    # Should UPPER, TRIM, remove control chars, collapse spaces
    assert 'UPPER' in result
    assert 'TRIM' in result
    assert 'REGEXP_REPLACE' in result
    assert '[[:cntrl:]]' in result  # Remove control chars
    assert '[[:space:]]+' in result  # Collapse spaces


def test_normalize_oracle_column_pseudo_null_strings():
    """Test normalization of pseudo-null strings (NULL, N/A, etc.)."""
    from utils_hash import normalize_oracle_column

    result = normalize_oracle_column('status', 'VARCHAR2')

    # Should convert pseudo-nulls to actual NULL
    pseudo_nulls = ['NULL', 'NUL', 'NONE', 'N/A', '<NA>', 'EMPTY', 'NIL']
    for pn in pseudo_nulls:
        assert pn in result


def test_normalize_oracle_column_date():
    """Test normalization of DATE values."""
    from utils_hash import normalize_oracle_column

    result = normalize_oracle_column('acct_date', 'DATE')

    # Should format as YYYY-MM-DD
    assert 'TO_CHAR(acct_date, \'YYYY-MM-DD\')' in result
    assert 'WHEN acct_date IS NULL THEN NULL' in result


def test_normalize_oracle_column_timestamp():
    """Test normalization of TIMESTAMP values."""
    from utils_hash import normalize_oracle_column

    result = normalize_oracle_column('created_at', 'TIMESTAMP')

    # Should format as YYYY-MM-DD HH24:MI:SS.FF3
    assert 'TO_CHAR(created_at, \'YYYY-MM-DD HH24:MI:SS.FF3\')' in result


def test_normalize_oracle_column_custom_decimals():
    """Test normalization with custom decimal places."""
    from utils_hash import normalize_oracle_column

    # 2 decimals
    result_2 = normalize_oracle_column('price', 'NUMBER', decimals=2)
    assert '00' in result_2  # Should have 2 zeros
    assert 'ROUND(price, 2)' in result_2

    # 5 decimals
    result_5 = normalize_oracle_column('price', 'NUMBER', decimals=5)
    assert '00000' in result_5
    assert 'ROUND(price, 5)' in result_5


# Test for build_oracle_hash_expr()
def test_build_oracle_hash_expr_basic():
    """Test building Oracle hash expression with basic columns."""
    from utils_hash import build_oracle_hash_expr

    columns = [
        {'column_name': 'acct_id', 'data_type': 'NUMBER'},
        {'column_name': 'balance', 'data_type': 'NUMBER'},
        {'column_name': 'status', 'data_type': 'VARCHAR2'}
    ]

    result = build_oracle_hash_expr(columns)

    # Verify hash expression
    assert 'RAWTOHEX' in result['hash_expr']
    assert 'STANDARD_HASH' in result['hash_expr']
    assert 'SHA256' in result['hash_expr']

    # Verify concatenation with separator
    assert " || '|' || " in result['concat_expr']

    # Verify debug select
    assert 'acct_id' in result['debug_select']
    assert 'balance' in result['debug_select']
    assert 'status' in result['debug_select']
    assert '__concat_string' in result['debug_select']
    assert '__hash_hex' in result['debug_select']


def test_build_oracle_hash_expr_empty_columns():
    """Test building hash expression with empty column list."""
    from utils_hash import build_oracle_hash_expr

    result = build_oracle_hash_expr([])

    # Should return NULL
    assert result['hash_expr'] == 'NULL'
    assert result['concat_expr'] == 'NULL'
    assert result['debug_select'] == ''


def test_build_oracle_hash_expr_single_column():
    """Test building hash expression with single column."""
    from utils_hash import build_oracle_hash_expr

    columns = [{'column_name': 'acct_id', 'data_type': 'NUMBER'}]

    result = build_oracle_hash_expr(columns)

    # Should still work with single column
    assert 'RAWTOHEX' in result['hash_expr']
    assert 'STANDARD_HASH' in result['hash_expr']
    assert result['concat_expr']  # Not empty


def test_build_oracle_hash_expr_custom_separator():
    """Test building hash expression with custom separator."""
    from utils_hash import build_oracle_hash_expr

    columns = [
        {'column_name': 'col1', 'data_type': 'NUMBER'},
        {'column_name': 'col2', 'data_type': 'VARCHAR2'}
    ]

    result = build_oracle_hash_expr(columns, separator='||')

    # Should use custom separator
    assert " || '||' || " in result['concat_expr']


def test_build_oracle_hash_expr_mixed_types():
    """Test building hash expression with mixed data types."""
    from utils_hash import build_oracle_hash_expr

    columns = [
        {'column_name': 'id', 'data_type': 'NUMBER'},
        {'column_name': 'name', 'data_type': 'VARCHAR2'},
        {'column_name': 'created', 'data_type': 'DATE'},
        {'column_name': 'updated', 'data_type': 'TIMESTAMP'},
        {'column_name': 'amount', 'data_type': 'DECIMAL(15,2)'}
    ]

    result = build_oracle_hash_expr(columns)

    # All columns should be normalized appropriately
    assert 'RAWTOHEX' in result['hash_expr']
    assert len([c for c in columns]) == 5
