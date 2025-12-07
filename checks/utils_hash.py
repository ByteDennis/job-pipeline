"""Hash utilities for data validation pipeline - normalization and hash expression builders."""
from typing import List, Dict

#>>> Normalize Oracle column value to standardized format <<<#
def normalize_oracle_column(column_name: str, data_type: str, decimals: int = 3) -> str:
    dt = data_type.upper()
    dec_zeros = '0' * decimals
    num_mask = f"FM9999999999999990.{dec_zeros}"

    text_sanitizer = (
        f"REGEXP_REPLACE("
        f"REGEXP_REPLACE(UPPER(TRIM({column_name})), '[[:cntrl:]]', ''),"
        f"'[[:space:]]+', ' ')"
    )
    pseudo_null_to_null = (
        f"CASE WHEN {text_sanitizer} IN ('', 'NULL','NUL','NONE','N/A','<NA>','EMPTY','NIL','.') "
        f"THEN NULL ELSE {text_sanitizer} END"
    )

    if any(t in dt for t in ['NUMBER','NUMERIC','INTEGER','INT','FLOAT','DOUBLE','DECIMAL']):
        return (
            f"CASE WHEN {column_name} IS NULL OR {column_name} = 0 THEN '0' "
            f"ELSE REPLACE(TO_CHAR(ROUND({column_name}, {decimals}), '{num_mask}'), ',', '.') END"
        )

    if any(t in dt for t in ['VARCHAR','CHAR','RAW', 'VARCHAR2']):
        return pseudo_null_to_null

    if 'DATE' in dt and 'TIMESTAMP' not in dt:
        return f"CASE WHEN {column_name} IS NULL THEN NULL ELSE TO_CHAR({column_name}, 'YYYY-MM-DD') END"

    if 'TIMESTAMP' in dt:
        return f"CASE WHEN {column_name} IS NULL THEN NULL ELSE TO_CHAR({column_name}, 'YYYY-MM-DD HH24:MI:SS.FF3') END"

    return pseudo_null_to_null


#>>> Normalize Athena column value to standardized format <<<#
def normalize_athena_column(column_name: str, data_type: str, decimals: int = 3) -> str:
    dt = data_type.lower()
    printf = f"%.{decimals}f"

    base = f"upper(trim({column_name}))"
    rm_ctrl = f"regexp_replace({base}, '\\\\p{{Cntrl}}', '')"
    collapse = f"regexp_replace({rm_ctrl}, '\\\\s+', ' ')"
    pseudo_null_to_null = (
        f"CASE WHEN {collapse} IN ('', 'NULL','NUL','NONE','N/A','<NA>','EMPTY','NIL','.') "
        f"THEN NULL ELSE {collapse} END"
    )

    if any(t in dt for t in ['bigint','int','integer','double','float', 'decimal']):
        return (
            f"CASE WHEN {column_name} IS NULL OR CAST({column_name} AS DOUBLE) = 0 THEN '0' "
            f"ELSE FORMAT('{printf}', CAST({column_name} AS DOUBLE)) END"
        )

    if any(t in dt for t in ['string','varchar','char']):
        return pseudo_null_to_null

    if dt == 'date':
        return f"CASE WHEN {column_name} IS NULL THEN NULL ELSE DATE_FORMAT({column_name}, '%Y-%m-%d') END"

    if 'timestamp' in dt:
        return f"CASE WHEN {column_name} IS NULL THEN NULL ELSE format_datetime({column_name}, 'yyyy-MM-dd HH:mm:ss.SSS') END"

    return pseudo_null_to_null


#>>> Build Oracle SHA-256 hash expression <<<#
def build_oracle_hash_expr(columns: List[Dict[str, str]], separator: str = '|') -> Dict[str, str]:
    if not columns:
        return {"hash_expr": "NULL", "concat_expr": "NULL", "debug_select": ""}

    normalized_exprs = [
        normalize_oracle_column(col['column_name'], col['data_type'])
        for col in columns
    ]

    concat_expr = f" || '{separator}' || ".join(normalized_exprs)
    hash_expr = f"RAWTOHEX(STANDARD_HASH({concat_expr}, 'SHA256'))"

    debug_pairs = [
        f'{expr} AS "{col["column_name"]}"'
        for expr, col in zip(normalized_exprs, columns)
    ]
    debug_pairs.append(f'{concat_expr} AS "__concat_string"')
    debug_pairs.append(f'{hash_expr} AS "__hash_hex"')
    debug_select_list = ",\n       ".join(debug_pairs)

    return {
        "hash_expr": hash_expr,
        "concat_expr": concat_expr,
        "debug_select": debug_select_list
    }


#>>> Build Athena SHA-256 hash expression <<<#
def build_athena_hash_expr(columns: List[Dict[str, str]], separator: str = '|', decimals: int = 3, null_sentinel: str = '') -> Dict[str, str]:
    if not columns:
        return {"hash_expr": "NULL", "concat_expr": "NULL", "debug_select": ""}

    norm_exprs = [normalize_athena_column(c['column_name'], c['data_type'], decimals) for c in columns]

    items = [f"COALESCE({e}, '{null_sentinel}')" for e in norm_exprs]
    concat_expr = f"array_join(array[{', '.join(items)}], '{separator}')"

    hash_expr = f"to_hex(sha256(to_utf8({concat_expr})))"

    debug_pairs = [f'{e} AS "{c["column_name"]}"' for e, c in zip(norm_exprs, columns)]
    debug_pairs.append(f'{concat_expr} AS "__concat_string"')
    debug_pairs.append(f'{hash_expr} AS "__hash_hex"')
    debug_select_list = ",\n       ".join(debug_pairs)

    return {
        "hash_expr": hash_expr,
        "concat_expr": concat_expr,
        "debug_select": debug_select_list
    }
