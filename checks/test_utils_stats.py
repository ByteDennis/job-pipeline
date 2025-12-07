"""Tests for utils_stats.py."""
import pytest

# Test for is_numeric_type()
def test_is_numeric_type_oracle_numeric():
    """Test numeric type detection for Oracle types."""
    from utils_stats import is_numeric_type

    oracle_numeric = ['NUMBER', 'NUMERIC', 'INTEGER', 'INT', 'FLOAT', 'DOUBLE', 'DECIMAL']

    for data_type in oracle_numeric:
        assert is_numeric_type(data_type, is_oracle=True) is True


def test_is_numeric_type_oracle_non_numeric():
    """Test non-numeric type detection for Oracle types."""
    from utils_stats import is_numeric_type

    oracle_non_numeric = ['VARCHAR2', 'VARCHAR', 'CHAR', 'DATE', 'TIMESTAMP', 'CLOB', 'BLOB']

    for data_type in oracle_non_numeric:
        assert is_numeric_type(data_type, is_oracle=True) is False


def test_is_numeric_type_athena_numeric():
    """Test numeric type detection for Athena types."""
    from utils_stats import is_numeric_type

    athena_numeric = ['int', 'bigint', 'double', 'float', 'decimal', 'smallint', 'tinyint']

    for data_type in athena_numeric:
        assert is_numeric_type(data_type, is_oracle=False) is True


def test_is_numeric_type_athena_non_numeric():
    """Test non-numeric type detection for Athena types."""
    from utils_stats import is_numeric_type

    athena_non_numeric = ['string', 'varchar', 'char', 'date', 'timestamp', 'boolean', 'array', 'struct']

    for data_type in athena_non_numeric:
        assert is_numeric_type(data_type, is_oracle=False) is False


def test_is_numeric_type_case_insensitive():
    """Test type detection is case insensitive."""
    from utils_stats import is_numeric_type

    # Oracle
    assert is_numeric_type('number', is_oracle=True) is True
    assert is_numeric_type('NUMBER', is_oracle=True) is True
    assert is_numeric_type('Number', is_oracle=True) is True

    # Athena
    assert is_numeric_type('BIGINT', is_oracle=False) is True
    assert is_numeric_type('bigint', is_oracle=False) is True
    assert is_numeric_type('BigInt', is_oracle=False) is True


def test_is_numeric_type_complex_oracle():
    """Test complex Oracle types (e.g., NUMBER(10,2))."""
    from utils_stats import is_numeric_type

    complex_numeric = ['NUMBER(10,2)', 'DECIMAL(15,3)', 'FLOAT(126)']

    for data_type in complex_numeric:
        assert is_numeric_type(data_type, is_oracle=True) is True


def test_is_numeric_type_unknown_defaults_to_categorical():
    """Test unknown types default to categorical (non-numeric)."""
    from utils_stats import is_numeric_type

    unknown_types = ['UNKNOWN_TYPE', 'CUSTOM_TYPE', 'WEIRD_TYPE']

    for data_type in unknown_types:
        assert is_numeric_type(data_type, is_oracle=True) is False
        assert is_numeric_type(data_type, is_oracle=False) is False
