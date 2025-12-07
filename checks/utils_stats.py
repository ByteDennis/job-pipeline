"""Statistical computation utilities for column analysis with categorical/continuous differentiation."""
from typing import Dict

#>>> Determine if data type is numeric (continuous) vs categorical <<<#
def is_numeric_type(data_type, is_oracle=True):
    data_type = data_type.upper() if is_oracle else data_type.lower()
    if is_oracle:
        return any(t in data_type for t in ['NUMBER', 'FLOAT', 'BINARY_FLOAT', 'BINARY_DOUBLE'])
    else:
        return any(t in data_type for t in ['int', 'double', 'decimal', 'float', 'bigint', 'tinyint', 'smallint'])

#>>> Build SQL for continuous column (Oracle) <<<#
def build_continuous_sql_oracle(table_name, col_name, col_type, where_clause):
    return f"""
SELECT '{col_name}' AS col_name, '{col_type}' AS col_type, 'continuous' AS col_category,
       COUNT({col_name}) AS col_count, COUNT(DISTINCT {col_name}) AS col_distinct,
       TO_CHAR(MAX({col_name})) AS col_max, TO_CHAR(MIN({col_name})) AS col_min,
       AVG({col_name}) AS col_avg, STDDEV({col_name}) AS col_std,
       SUM({col_name}) AS col_sum, SUM({col_name} * {col_name}) AS col_sum_sq,
       '' AS col_freq, COUNT(*) - COUNT({col_name}) AS col_missing
FROM {table_name} WHERE {where_clause}""".strip()

#>>> Build SQL for categorical column (Oracle) <<<#
def build_categorical_sql_oracle(table_name, col_name, col_type, where_clause):
    col_ref = f'TRUNC({col_name})' if 'TIMESTAMP' in col_type.upper() else col_name
    return f"""
WITH FreqTable_RAW AS (
    SELECT {col_ref} AS p_col, COUNT(*) AS value_freq
    FROM {table_name} WHERE {where_clause} GROUP BY {col_ref}
), FreqTable AS (
    SELECT p_col, value_freq, ROW_NUMBER() OVER (ORDER BY value_freq DESC, p_col ASC) AS rn
    FROM FreqTable_RAW
), TopFreq AS (
    SELECT LISTAGG(TO_CHAR(NVL(p_col, 'NULL')) || '(' || value_freq || ')', '; ')
           WITHIN GROUP (ORDER BY value_freq DESC) AS freq_str
    FROM FreqTable WHERE rn <= 10
)
SELECT '{col_name}' AS col_name, '{col_type}' AS col_type, 'categorical' AS col_category,
       SUM(value_freq) AS col_count, COUNT(value_freq) AS col_distinct,
       TO_CHAR(MAX(value_freq)) AS col_max, TO_CHAR(MIN(value_freq)) AS col_min,
       AVG(value_freq) AS col_avg, STDDEV(value_freq) AS col_std,
       SUM(value_freq) AS col_sum, SUM(value_freq * value_freq) AS col_sum_sq,
       t.freq_str AS col_freq,
       NVL((SELECT value_freq FROM FreqTable WHERE p_col IS NULL), 0) AS col_missing
FROM FreqTable CROSS JOIN TopFreq t
GROUP BY t.freq_str""".strip()

#>>> Build SQL for continuous column (Athena) <<<#
def build_continuous_sql_athena(table_name, col_name, col_type, where_clause):
    return f"""
SELECT '{col_name}' AS col_name, '{col_type}' AS col_type, 'continuous' AS col_category,
       COUNT({col_name}) AS col_count, APPROX_DISTINCT({col_name}) AS col_distinct,
       CAST(MAX({col_name}) AS VARCHAR) AS col_max, CAST(MIN({col_name}) AS VARCHAR) AS col_min,
       AVG(CAST({col_name} AS DOUBLE)) AS col_avg, STDDEV_SAMP(CAST({col_name} AS DOUBLE)) AS col_std,
       SUM(CAST({col_name} AS DOUBLE)) AS col_sum,
       SUM(CAST({col_name} AS DOUBLE) * CAST({col_name} AS DOUBLE)) AS col_sum_sq,
       '' AS col_freq, COUNT(*) - COUNT({col_name}) AS col_missing
FROM {table_name} WHERE {where_clause}""".strip()

#>>> Build SQL for categorical column (Athena) <<<#
def build_categorical_sql_athena(table_name, col_name, col_type, where_clause):
    return f"""
WITH FreqTable_RAW AS (
    SELECT {col_name} AS p_col, COUNT(*) AS value_freq
    FROM {table_name} WHERE {where_clause} GROUP BY {col_name}
), FreqTable AS (
    SELECT p_col, value_freq, ROW_NUMBER() OVER (ORDER BY value_freq DESC, p_col ASC) AS rn
    FROM FreqTable_RAW
)
SELECT '{col_name}' AS col_name, '{col_type}' AS col_type, 'categorical' AS col_category,
       SUM(value_freq) AS col_count, COUNT(value_freq) AS col_distinct,
       CAST(MAX(value_freq) AS VARCHAR) AS col_max, CAST(MIN(value_freq) AS VARCHAR) AS col_min,
       AVG(CAST(value_freq AS DOUBLE)) AS col_avg, STDDEV_SAMP(CAST(value_freq AS DOUBLE)) AS col_std,
       SUM(value_freq) AS col_sum, SUM(value_freq * value_freq) AS col_sum_sq,
       ARRAY_JOIN(ARRAY_AGG(COALESCE(CAST(p_col AS VARCHAR), 'NULL') || '(' || CAST(value_freq AS VARCHAR) || ')'), '; ') AS col_freq,
       COALESCE((SELECT value_freq FROM FreqTable WHERE p_col IS NULL), 0) AS col_missing
FROM FreqTable WHERE rn <= 10""".strip()

#>>> Build SQL for single column based on type <<<#
def build_column_sql(table_name, col_name, col_type, where_clause, is_oracle=True):
    is_continuous = is_numeric_type(col_type, is_oracle)
    if is_oracle:
        return build_continuous_sql_oracle(table_name, col_name, col_type, where_clause) if is_continuous \
            else build_categorical_sql_oracle(table_name, col_name, col_type, where_clause)
    else:
        return build_continuous_sql_athena(table_name, col_name, col_type, where_clause) if is_continuous \
            else build_categorical_sql_athena(table_name, col_name, col_type, where_clause)

#>>> Parse single row statistics result <<<#
def parse_stats_row(row):
    return {
        'col_name': row.get('col_name'), 'col_type': row.get('col_type'), 'col_category': row.get('col_category'),
        'count': row.get('col_count'), 'distinct': row.get('col_distinct'),
        'max': row.get('col_max'), 'min': row.get('col_min'),
        'avg': row.get('col_avg'), 'std': row.get('col_std'),
        'sum': row.get('col_sum'), 'sum_sq': row.get('col_sum_sq'),
        'freq_top10': row.get('col_freq'), 'missing': row.get('col_missing')
    }
