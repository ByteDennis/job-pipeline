"""
SQL Normalization Functions for Hash Comparison

This module provides robust normalization functions for both Oracle (PCDS) and AWS Athena
to ensure consistent hash computation across platforms.

Key principles:
- Trim whitespace
- Convert to uppercase for string comparison
- Handle NULL values consistently
- Normalize dates to ISO format
- Normalize numbers to consistent precision
- Handle special characters
"""

from typing import Dict, List


def normalize_oracle_column(column_name: str, data_type: str) -> str:
    """
    Generate Oracle SQL expression to normalize a column for hash computation

    Args:
        column_name: Name of the column
        data_type: Oracle data type (e.g., 'VARCHAR2', 'NUMBER', 'DATE', 'TIMESTAMP')

    Returns:
        str: Oracle SQL expression for normalized column
    """
    data_type_upper = data_type.upper()

    # String types
    if any(t in data_type_upper for t in ['VARCHAR', 'CHAR', 'CLOB', 'NVARCHAR', 'NCHAR']):
        return f"COALESCE(UPPER(TRIM({column_name})), '')"

    # Numeric types
    elif any(t in data_type_upper for t in ['NUMBER', 'NUMERIC', 'INTEGER', 'INT', 'FLOAT', 'DOUBLE']):
        # Convert to string with fixed precision to avoid floating point differences
        return f"COALESCE(TO_CHAR({column_name}, 'FM999999999999999.999999'), '0')"

    # Date types
    elif 'DATE' in data_type_upper and 'TIMESTAMP' not in data_type_upper:
        # Normalize to ISO format: YYYY-MM-DD
        return f"COALESCE(TO_CHAR({column_name}, 'YYYY-MM-DD'), '')"

    # Timestamp types
    elif 'TIMESTAMP' in data_type_upper:
        # Normalize to ISO format with milliseconds: YYYY-MM-DD HH24:MI:SS.FF3
        return f"COALESCE(TO_CHAR({column_name}, 'YYYY-MM-DD HH24:MI:SS.FF3'), '')"

    # Binary types - convert to hex
    elif any(t in data_type_upper for t in ['BLOB', 'RAW', 'LONG RAW']):
        return f"COALESCE(RAWTOHEX({column_name}), '')"

    # Default: convert to string
    else:
        return f"COALESCE(UPPER(TRIM(TO_CHAR({column_name}))), '')"


def normalize_athena_column(column_name: str, data_type: str) -> str:
    """
    Generate Athena SQL expression to normalize a column for hash computation

    Args:
        column_name: Name of the column
        data_type: Athena data type (e.g., 'string', 'bigint', 'double', 'timestamp', 'date')

    Returns:
        str: Athena SQL expression for normalized column
    """
    data_type_lower = data_type.lower()

    # String types
    if any(t in data_type_lower for t in ['string', 'varchar', 'char']):
        return f"COALESCE(UPPER(TRIM({column_name})), '')"

    # Integer types
    elif any(t in data_type_lower for t in ['bigint', 'int', 'integer', 'smallint', 'tinyint']):
        return f"COALESCE(CAST({column_name} AS VARCHAR), '0')"

    # Float/Double types
    elif any(t in data_type_lower for t in ['double', 'float', 'real', 'decimal']):
        # Format with fixed precision
        return f"COALESCE(FORMAT('%0.6f', {column_name}), '0.000000')"

    # Date type
    elif data_type_lower == 'date':
        # Normalize to ISO format: YYYY-MM-DD
        return f"COALESCE(DATE_FORMAT({column_name}, '%Y-%m-%d'), '')"

    # Timestamp type
    elif 'timestamp' in data_type_lower:
        # Normalize to ISO format with milliseconds
        return f"COALESCE(DATE_FORMAT({column_name}, '%Y-%m-%d %H:%i:%s.%f'), '')"

    # Boolean type
    elif 'boolean' in data_type_lower:
        return f"COALESCE(CAST({column_name} AS VARCHAR), 'false')"

    # Binary types - convert to hex
    elif any(t in data_type_lower for t in ['binary', 'varbinary']):
        return f"COALESCE(TO_HEX({column_name}), '')"

    # Array/Map/Struct - convert to JSON string
    elif any(t in data_type_lower for t in ['array', 'map', 'struct', 'row']):
        return f"COALESCE(CAST({column_name} AS VARCHAR), '')"

    # Default: convert to string
    else:
        return f"COALESCE(UPPER(TRIM(CAST({column_name} AS VARCHAR))), '')"


def build_oracle_hash_expr(columns: List[Dict[str, str]], separator: str = '|') -> str:
    """
    Build complete Oracle hash expression for multiple columns

    Args:
        columns: List of dicts with 'column_name' and 'data_type'
        separator: String separator between columns (default: '|')

    Returns:
        str: Complete Oracle MD5 hash expression

    Example:
        >>> columns = [
        ...     {'column_name': 'ID', 'data_type': 'NUMBER'},
        ...     {'column_name': 'NAME', 'data_type': 'VARCHAR2'}
        ... ]
        >>> build_oracle_hash_expr(columns)
    """
    if not columns:
        return "NULL"

    normalized_exprs = [
        normalize_oracle_column(col['column_name'], col['data_type'])
        for col in columns
    ]

    # Concatenate with separator
    concat_expr = f" || '{separator}' || ".join(normalized_exprs)

    # Wrap in STANDARD_HASH or DBMS_CRYPTO.HASH (depending on Oracle version)
    # Using DBMS_OBFUSCATION_TOOLKIT.MD5 for compatibility
    # Or STANDARD_HASH (Oracle 12c+)
    hash_expr = f"STANDARD_HASH({concat_expr}, 'MD5')"

    return hash_expr


def build_athena_hash_expr(columns: List[Dict[str, str]], separator: str = '|') -> str:
    """
    Build complete Athena hash expression for multiple columns

    Args:
        columns: List of dicts with 'column_name' and 'data_type'
        separator: String separator between columns (default: '|')

    Returns:
        str: Complete Athena MD5 hash expression

    Example:
        >>> columns = [
        ...     {'column_name': 'id', 'data_type': 'bigint'},
        ...     {'column_name': 'name', 'data_type': 'string'}
        ... ]
        >>> build_athena_hash_expr(columns)
    """
    if not columns:
        return "NULL"

    normalized_exprs = [
        normalize_athena_column(col['column_name'], col['data_type'])
        for col in columns
    ]

    # Concatenate with separator
    concat_expr = f" || '{separator}' || ".join(normalized_exprs)

    # Athena MD5: to_hex(md5(to_utf8(string)))
    hash_expr = f"TO_HEX(MD5(TO_UTF8({concat_expr})))"

    return hash_expr


def test_normalization():
    """
    Test normalization functions with various data types
    """
    print("Testing Oracle Normalization:")
    print("=" * 60)

    oracle_tests = [
        ('CUSTOMER_ID', 'NUMBER'),
        ('CUSTOMER_NAME', 'VARCHAR2(100)'),
        ('CREATE_DATE', 'DATE'),
        ('UPDATE_TS', 'TIMESTAMP'),
        ('AMOUNT', 'NUMBER(10,2)'),
    ]

    for col, dtype in oracle_tests:
        expr = normalize_oracle_column(col, dtype)
        print(f"{col:20s} ({dtype:20s}) => {expr}")

    print("\n" + "=" * 60)
    print("Testing Athena Normalization:")
    print("=" * 60)

    athena_tests = [
        ('customer_id', 'bigint'),
        ('customer_name', 'string'),
        ('create_date', 'date'),
        ('update_ts', 'timestamp'),
        ('amount', 'double'),
    ]

    for col, dtype in athena_tests:
        expr = normalize_athena_column(col, dtype)
        print(f"{col:20s} ({dtype:20s}) => {expr}")

    print("\n" + "=" * 60)
    print("Testing Hash Expression Builders:")
    print("=" * 60)

    columns = [
        {'column_name': 'CUSTOMER_ID', 'data_type': 'NUMBER'},
        {'column_name': 'CUSTOMER_NAME', 'data_type': 'VARCHAR2'},
        {'column_name': 'CREATE_DATE', 'data_type': 'DATE'}
    ]

    oracle_hash = build_oracle_hash_expr(columns)
    print(f"\nOracle Hash Expression:\n{oracle_hash}\n")

    columns_athena = [
        {'column_name': 'customer_id', 'data_type': 'bigint'},
        {'column_name': 'customer_name', 'data_type': 'string'},
        {'column_name': 'create_date', 'data_type': 'date'}
    ]

    athena_hash = build_athena_hash_expr(columns_athena)
    print(f"Athena Hash Expression:\n{athena_hash}\n")


if __name__ == "__main__":
    test_normalization()
