"""Part 1: PCDS Meta Check - Check table accessibility, row counts, crosswalk mappings, and generate vintages then upload to S3."""
if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv('input_pcds')

import os
from upath import UPath
import pandas as pd
from operator import itemgetter
from loguru import logger

import constant
import utils_config as C
from utils_s3 import S3Manager
from utils_date import parse_date_to_std, generate_vintages


#>>> Check if table is accessible <<<#
def check_accessible(conn, table_name):
    try:
        sql = f"SELECT 1 FROM {table_name} WHERE ROWNUM = 1"
        C.proc_pcds(sql, service_name=conn)
        return True
    except Exception as e:
        logger.error(f"Table {table_name} not accessible: {e}")
        return False

#>>> Get row counts grouped by raw date variable <<<#
def get_row_counts(conn, table_name, date_var, where_clause=None):
    where = "" if C.is_missing(where_clause) else f"WHERE {where_clause}"
    sql = f"SELECT {date_var}, COUNT(*) as cnt FROM {table_name} {where} GROUP BY {date_var}"
    df = C.proc_pcds(sql, service_name=conn)
    df[date_var] = df[date_var].apply(parse_date_to_std)
    return df

#>>> Get table columns with types <<<#
def get_columns(service_name, table_name):
    sql = f"""
    SELECT column_name, data_type
    FROM all_tab_columns
    WHERE table_name = UPPER('{table_name}')
    ORDER BY column_id
    """
    return C.proc_pcds(sql, service_name=service_name)

#>>> Check crosswalk completeness <<<#
def check_crosswalk(table_name, columns_df, crosswalk_df, col_map_name):
    actual_cols = set(columns_df['COLUMN_NAME'].str.upper())
    mapped = crosswalk_df[crosswalk_df['col_map'] == col_map_name.lower()]

    comparable, tokenized, pcds_only = [], [], []
    for _, row in mapped.iterrows():
        pcds_col = str(row.get("pcds_col", "")).strip().upper()
        if pcds_col in actual_cols:
            if row['is_tokenized']:
                tokenized.append(pcds_col)
            elif pd.notna(row['aws_col']):
                comparable.append(pcds_col)
            else:
                pcds_only.append(pcds_col)

    unmapped = list(actual_cols - set(comparable) - set(tokenized) - set(pcds_only))
    return {
        'table': table_name,
        'comparable': comparable, 
        'tokenized': tokenized, 
        'pcds_only': pcds_only, 
        'unmapped': unmapped
    }

#>>> Generate vintages from row count data <<<#
def get_vintages(row_counts_df, date_var, partition_type):
    var = date_var._var
    if row_counts_df.empty or row_counts_df[var].isna().all():
        return []
    min_date = row_counts_df[var].min()
    max_date = row_counts_df[var].max()
    return generate_vintages(min_date, max_date, partition_type, date_var)

#>>> Main execution <<<#
def main():
    run_name, category, config_path = C.get_env('RUN_NAME', 'CATEGORY', 'META_STEP')

    cfg = C.load_config(config_path)
    step_name = cfg.output.step_name.format(p='pcds')
    output_folder = cfg.output.disk.format(name=run_name)
    C.add_logger(output_folder, name=step_name)
    logger.info(f"Starting PCDS meta check: {run_name} | {category}")

    tables_df = C.read_input_tables(cfg.table)
    crosswalk_df = C.load_column_mappings(cfg.column_maps, category)

    # Filter for enabled tables only
    enabled_tables = tables_df[tables_df['enabled']].copy()
    logger.info(f"Going to validate {len(enabled_tables)} tables out of {len(tables_df)} total")

    # Filter crosswalk for only enabled table mappings
    filtered_crosswalk = crosswalk_df[
        crosswalk_df["col_map"].str.lower().isin(
            (
                enabled_tables["col_map"]
                if "col_map" in enabled_tables.columns
                else enabled_tables["pcds_tbl"].str.extract(r"([^.]+)$", expand=False)
            ).str.lower().unique()
        )
    ].copy()

    # Upload filtered config files to S3 for other machines to use
    s3_bucket = cfg.output.s3.format(name=run_name)
    s3 = S3Manager(s3_bucket)
    s3.write_df(enabled_tables, 'input_tables.csv')
    s3.write_df(filtered_crosswalk, 'crosswalk.csv')
    logger.info(f"Uploaded input_tables, crosswalk document to S3 {s3.base}")

    results = []

    for _, table in enabled_tables.iterrows():
        service_name, table_name = table['pcds_tbl'].split('.')
        fetch = itemgetter('pcds_var', 'pcds_where', 'partition', 'col_map', 'start_dt', 'end_dt')
        date_var, where_clause, partition_type, col_map_name, start_dt, end_dt = fetch(table)
        logger.info(f"Processing {table_name}")
        result = {
            'table': table_name.upper(),
            'service': service_name,
            'accessible': check_accessible(service_name, table_name),
            'row_counts': None,
            'crosswalk': None,
            'column_types': None,
            'date_var': None,
            'where_clause': '',
            'vintages': []
        }

        if result['accessible']:
            columns_df = get_columns(service_name, table_name)
            result['crosswalk'] = check_crosswalk(table_name, columns_df, filtered_crosswalk, col_map_name)

            column_types = dict(zip(columns_df['COLUMN_NAME'].str.upper(), columns_df['DATA_TYPE']))
            result['column_types'] = column_types

            if date_var and not C.is_missing(date_var):
                date_var = C.DateParser(date_var, column_types[date_var])
                date_var.get_fmt(table_name, service_name)
                where_clause = date_var.merge_where(start_dt, end_dt, where_clause)
                row_counts_df = date_var.get_cnt(table_name, where_clause, service_name=service_name)
                result['where_clause'] = where_clause
                result['row_counts'] = row_counts_df.to_dict('records')
                result['vintages'] = get_vintages(row_counts_df, date_var, partition_type)
                result['date_var'] = date_var.to_json()

        results.append(result)

    local_path = os.path.join(output_folder, f'{step_name}.json')
    s3.write_json(results, UPath(local_path))
    logger.info(f"Saved local copy to {local_path}")

    s3_path = s3.write_json(results, f'{step_name}.json')
    logger.info(f"Uploaded PCDS meta check results to {s3_path}")

    return results

if __name__ == '__main__':
    main()