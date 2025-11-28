"""
Tests for Oracle and Athena hash normalization functions
"""
import pytest
import unittest
from typing import List, Dict


# Copy the functions here or import them from your module
def normalize_oracle_column(column_name: str, data_type: str) -> str:
    """Generate Oracle SQL expression to normalize a column for hash computation"""
    data_type_upper = data_type.upper()

    if any(t in data_type_upper for t in ['VARCHAR', 'CHAR', 'CLOB', 'NVARCHAR', 'NCHAR']):
        return f"COALESCE(UPPER(TRIM({column_name})), '')"
    elif any(t in data_type_upper for t in ['NUMBER', 'NUMERIC', 'INTEGER', 'INT', 'FLOAT', 'DOUBLE']):
        return f"COALESCE(TO_CHAR({column_name}, 'FM999999999999999.999999'), '0')"
    elif 'DATE' in data_type_upper and 'TIMESTAMP' not in data_type_upper:
        return f"COALESCE(TO_CHAR({column_name}, 'YYYY-MM-DD'), '')"
    elif 'TIMESTAMP' in data_type_upper:
        return f"COALESCE(TO_CHAR({column_name}, 'YYYY-MM-DD HH24:MI:SS.FF3'), '')"
    elif any(t in data_type_upper for t in ['BLOB', 'RAW', 'LONG RAW']):
        return f"COALESCE(RAWTOHEX({column_name}), '')"
    else:
        return f"COALESCE(UPPER(TRIM(TO_CHAR({column_name}))), '')"


def normalize_athena_column(column_name: str, data_type: str) -> str:
    """Generate Athena SQL expression to normalize a column for hash computation"""
    data_type_lower = data_type.lower()

    if any(t in data_type_lower for t in ['string', 'varchar', 'char']):
        return f"COALESCE(UPPER(TRIM({column_name})), '')"
    elif any(t in data_type_lower for t in ['bigint', 'int', 'integer', 'smallint', 'tinyint']):
        return f"COALESCE(CAST({column_name} AS VARCHAR), '0')"
    elif any(t in data_type_lower for t in ['double', 'float', 'real', 'decimal']):
        return f"COALESCE(FORMAT('%0.6f', {column_name}), '0.000000')"
    elif data_type_lower == 'date':
        return f"COALESCE(DATE_FORMAT({column_name}, '%Y-%m-%d'), '')"
    elif 'timestamp' in data_type_lower:
        return f"COALESCE(DATE_FORMAT({column_name}, '%Y-%m-%d %H:%i:%s.%f'), '')"
    elif 'boolean' in data_type_lower:
        return f"COALESCE(CAST({column_name} AS VARCHAR), 'false')"
    elif any(t in data_type_lower for t in ['binary', 'varbinary']):
        return f"COALESCE(TO_HEX({column_name}), '')"
    elif any(t in data_type_lower for t in ['array', 'map', 'struct', 'row']):
        return f"COALESCE(CAST({column_name} AS VARCHAR), '')"
    else:
        return f"COALESCE(UPPER(TRIM(CAST({column_name} AS VARCHAR))), '')"


def build_oracle_hash_expr(columns: List[Dict[str, str]], separator: str = '|') -> str:
    """Build complete Oracle hash expression for multiple columns"""
    if not columns:
        return "NULL"

    normalized_exprs = [
        normalize_oracle_column(col['column_name'], col['data_type'])
        for col in columns
    ]
    concat_expr = f" || '{separator}' || ".join(normalized_exprs)
    return f"STANDARD_HASH({concat_expr}, 'MD5')"


def build_athena_hash_expr(columns: List[Dict[str, str]], separator: str = '|') -> str:
    """Build complete Athena hash expression for multiple columns"""
    if not columns:
        return "NULL"

    normalized_exprs = [
        normalize_athena_column(col['column_name'], col['data_type'])
        for col in columns
    ]
    concat_expr = f" || '{separator}' || ".join(normalized_exprs)
    return f"TO_HEX(MD5(TO_UTF8({concat_expr})))"


class TestOracleNormalization(unittest.TestCase):
    """Test Oracle column normalization"""

    @pytest.mark.parametrize("column_name,data_type,expected", [
        # String types
        ("name", "VARCHAR2(100)", "COALESCE(UPPER(TRIM(name)), '')"),
        ("code", "CHAR(10)", "COALESCE(UPPER(TRIM(code)), '')"),
        ("desc", "CLOB", "COALESCE(UPPER(TRIM(desc)), '')"),
        ("text", "NVARCHAR2(50)", "COALESCE(UPPER(TRIM(text)), '')"),

        # Numeric types
        ("id", "NUMBER", "COALESCE(TO_CHAR(id, 'FM999999999999999.999999'), '0')"),
        ("amount", "NUMBER(10,2)", "COALESCE(TO_CHAR(amount, 'FM999999999999999.999999'), '0')"),
        ("count", "INTEGER", "COALESCE(TO_CHAR(count, 'FM999999999999999.999999'), '0')"),
        ("price", "FLOAT", "COALESCE(TO_CHAR(price, 'FM999999999999999.999999'), '0')"),

        # Date/Timestamp types
        ("birth_date", "DATE", "COALESCE(TO_CHAR(birth_date, 'YYYY-MM-DD'), '')"),
        ("created_at", "TIMESTAMP", "COALESCE(TO_CHAR(created_at, 'YYYY-MM-DD HH24:MI:SS.FF3'), '')"),
        ("updated_at", "TIMESTAMP(6)", "COALESCE(TO_CHAR(updated_at, 'YYYY-MM-DD HH24:MI:SS.FF3'), '')"),

        # Binary types
        ("file_data", "BLOB", "COALESCE(RAWTOHEX(file_data), '')"),
        ("hash", "RAW(16)", "COALESCE(RAWTOHEX(hash), '')"),

        # Default case
        ("other", "UNKNOWN_TYPE", "COALESCE(UPPER(TRIM(TO_CHAR(other))), '')"),
    ])
    def test_normalize_oracle_column(self, column_name, data_type, expected):
        """Test Oracle column normalization with various data types"""
        result = normalize_oracle_column(column_name, data_type)
        assert result == expected, f"Failed for {data_type}: got {result}"


class TestAthenaNormalization(unittest.TestCase):
    """Test Athena column normalization"""

    @pytest.mark.parametrize("column_name,data_type,expected", [
        # String types
        ("name", "string", "COALESCE(UPPER(TRIM(name)), '')"),
        ("code", "varchar(100)", "COALESCE(UPPER(TRIM(code)), '')"),
        ("desc", "char(50)", "COALESCE(UPPER(TRIM(desc)), '')"),

        # Integer types
        ("id", "bigint", "COALESCE(CAST(id AS VARCHAR), '0')"),
        ("count", "int", "COALESCE(CAST(count AS VARCHAR), '0')"),
        ("flag", "tinyint", "COALESCE(CAST(flag AS VARCHAR), '0')"),

        # Float types
        ("amount", "double", "COALESCE(FORMAT('%0.6f', amount), '0.000000')"),
        ("price", "float", "COALESCE(FORMAT('%0.6f', price), '0.000000')"),
        ("rate", "decimal(10,2)", "COALESCE(FORMAT('%0.6f', rate), '0.000000')"),

        # Date/Timestamp types
        ("birth_date", "date", "COALESCE(DATE_FORMAT(birth_date, '%Y-%m-%d'), '')"),
        ("created_at", "timestamp", "COALESCE(DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s.%f'), '')"),

        # Boolean type
        ("is_active", "boolean", "COALESCE(CAST(is_active AS VARCHAR), 'false')"),

        # Binary types
        ("file_data", "binary", "COALESCE(TO_HEX(file_data), '')"),
        ("hash", "varbinary", "COALESCE(TO_HEX(hash), '')"),

        # Complex types
        ("tags", "array<string>", "COALESCE(CAST(tags AS VARCHAR), '')"),
        ("metadata", "map<string,string>", "COALESCE(CAST(metadata AS VARCHAR), '')"),
        ("record", "struct<id:int,name:string>", "COALESCE(CAST(record AS VARCHAR), '')"),

        # Default case
        ("other", "unknown_type", "COALESCE(UPPER(TRIM(CAST(other AS VARCHAR))), '')"),
    ])
    def test_normalize_athena_column(self, column_name, data_type, expected):
        """Test Athena column normalization with various data types"""
        result = normalize_athena_column(column_name, data_type)
        assert result == expected, f"Failed for {data_type}: got {result}"


class TestOracleHashBuilder(unittest.TestCase):
    """Test Oracle hash expression builder"""

    @pytest.mark.parametrize("columns,separator,expected_contains", [
        # Empty columns
        ([], '|', "NULL"),

        # Single column
        (
            [{'column_name': 'id', 'data_type': 'NUMBER'}],
            '|',
            ["STANDARD_HASH", "COALESCE(TO_CHAR(id", "'MD5')"]
        ),

        # Multiple columns with default separator
        (
            [
                {'column_name': 'id', 'data_type': 'NUMBER'},
                {'column_name': 'name', 'data_type': 'VARCHAR2'}
            ],
            '|',
            ["STANDARD_HASH", "||", "'|'", "COALESCE(TO_CHAR(id", "COALESCE(UPPER(TRIM(name))"]
        ),

        # Multiple columns with custom separator
        (
            [
                {'column_name': 'id', 'data_type': 'NUMBER'},
                {'column_name': 'date', 'data_type': 'DATE'},
                {'column_name': 'amount', 'data_type': 'NUMBER'}
            ],
            '~',
            ["STANDARD_HASH", "'~'", "COALESCE(TO_CHAR(id", "COALESCE(TO_CHAR(date, 'YYYY-MM-DD')"]
        ),
    ])
    def test_build_oracle_hash_expr(self, columns, separator, expected_contains):
        """Test Oracle hash expression building"""
        result = build_oracle_hash_expr(columns, separator)

        if isinstance(expected_contains, list):
            for expected in expected_contains:
                assert expected in result, f"Expected '{expected}' in result: {result}"
        else:
            assert result == expected_contains


class TestAthenaHashBuilder(unittest.TestCase):
    """Test Athena hash expression builder"""

    @pytest.mark.parametrize("columns,separator,expected_contains", [
        # Empty columns
        ([], '|', "NULL"),

        # Single column
        (
            [{'column_name': 'id', 'data_type': 'bigint'}],
            '|',
            ["TO_HEX(MD5(TO_UTF8", "COALESCE(CAST(id AS VARCHAR)"]
        ),

        # Multiple columns with default separator
        (
            [
                {'column_name': 'id', 'data_type': 'bigint'},
                {'column_name': 'name', 'data_type': 'string'}
            ],
            '|',
            ["TO_HEX(MD5(TO_UTF8", "||", "'|'", "COALESCE(CAST(id AS VARCHAR)", "COALESCE(UPPER(TRIM(name))"]
        ),

        # Multiple columns with custom separator
        (
            [
                {'column_name': 'id', 'data_type': 'bigint'},
                {'column_name': 'date', 'data_type': 'date'},
                {'column_name': 'amount', 'data_type': 'double'}
            ],
            '~',
            ["TO_HEX(MD5(TO_UTF8", "'~'", "COALESCE(CAST(id AS VARCHAR)", "COALESCE(DATE_FORMAT(date"]
        ),
    ])
    def test_build_athena_hash_expr(self, columns, separator, expected_contains):
        """Test Athena hash expression building"""
        result = build_athena_hash_expr(columns, separator)

        if isinstance(expected_contains, list):
            for expected in expected_contains:
                assert expected in result, f"Expected '{expected}' in result: {result}"
        else:
            assert result == expected_contains


class TestCrossplatformConsistency(unittest.TestCase):
    """Test that Oracle and Athena produce consistent structures"""

    def test_both_handle_empty_columns(self):
        """Both functions should return NULL for empty columns"""
        oracle_result = build_oracle_hash_expr([])
        athena_result = build_athena_hash_expr([])
        assert oracle_result == "NULL"
        assert athena_result == "NULL"

    def test_both_use_coalesce(self):
        """Both functions should use COALESCE for NULL handling"""
        oracle_result = normalize_oracle_column("col", "VARCHAR2")
        athena_result = normalize_athena_column("col", "string")
        assert "COALESCE" in oracle_result
        assert "COALESCE" in athena_result

    def test_separator_consistency(self):
        """Both functions should respect custom separators"""
        columns = [
            {'column_name': 'a', 'data_type': 'NUMBER'},
            {'column_name': 'b', 'data_type': 'VARCHAR2'}
        ]
        oracle_result = build_oracle_hash_expr(columns, separator='#')
        athena_columns = [
            {'column_name': 'a', 'data_type': 'bigint'},
            {'column_name': 'b', 'data_type': 'string'}
        ]
        athena_result = build_athena_hash_expr(athena_columns, separator='#')
        assert "'#'" in oracle_result
        assert "'#'" in athena_result


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
