"""Part 2: AWS Meta Check - Download PCDS results from S3, check AWS tables, and upload results back to S3."""
if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv('input_pcds')

import os
from upath import UPath
import pandas as pd
from loguru import logger
from operator import itemgetter

import constant
import utils_config as C
from utils_s3 import S3Manager
from utils_date import parse_date_to_std, generate_vintages

#>>> Check if table is accessible <<<#
def check_accessible(database, table_name):
    try:
        sql = f"SELECT 1 FROM {table_name} LIMIT 1"
        C.proc_aws(sql, data_base=database)
        return True
    except Exception as e:
        logger.error(f"Table {table_name} not accessible: {e}")
        return False

#>>> Get row counts grouped by raw date variable <<<#
def get_row_counts(database, table_name, date_var, where_clause=None):
    where = f"WHERE {where_clause}" if where_clause else ""
    sql = f"SELECT {date_var}, COUNT(*) as cnt FROM {table_name} {where} GROUP BY {date_var}"
    df = C.proc_aws(sql, data_base=database)
    df[date_var] = df[date_var].apply(parse_date_to_std)
    return df

#>>> Get table columns with types <<<#
def get_columns(database, table_name):
    sql = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = LOWER('{database}')
    AND table_name = LOWER('{table_name}')
    ORDER BY ordinal_position
    """    
    return C.proc_aws(sql, data_base=database)

#>>> Check crosswalk completeness <<<#
def check_crosswalk(table_name, columns_df, crosswalk_df, col_map_name):
    actual_cols = set(columns_df.iloc[:, 0].str.lower())
    mapped = crosswalk_df[crosswalk_df['col_map'] == col_map_name.lower()]

    comparable, tokenized, aws_only = [], [], []
    for _, row in mapped.iterrows():
        aws_col = row['aws_col'].lower() if pd.notna(row['aws_col']) else None
        if aws_col and aws_col in actual_cols:
            if row['is_tokenized']:
                tokenized.append(aws_col)
            elif pd.notna(row['pcds_col']):
                comparable.append(aws_col)
            else:
                aws_only.append(aws_col)

    unmapped = list(actual_cols - set(comparable) - set(tokenized) - set(aws_only))
    return {
        'table': table_name,
        'comparable': comparable, 
        'tokenized': tokenized, 
        'aws_only': aws_only, 
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
    step_name = cfg.output.step_name.format(p='aws')
    output_folder = cfg.output.disk.format(name=run_name)
    C.add_logger(output_folder, name=step_name)
    logger.info(f"Starting AWS meta check: {run_name} | {category}")

    s3_bucket = cfg.output.s3.format(name=run_name)
    s3 = S3Manager(s3_bucket)

    tables_df = s3.read_df('input_tables.csv')
    crosswalk_df = s3.read_df('crosswalk.csv')
    logger.info(f"Downloaded from S3: {len(tables_df)} tables to validate, {len(crosswalk_df)} crosswalk mappings")

    results = []

    for _, table in tables_df.iterrows():
        data_base, table_name = table['aws_tbl'].split('.')
        fetch = itemgetter('aws_tbl', 'aws_var', 'aws_where', 'partition', 'col_map', 'start_dt', 'end_dt')
        aws_tbl, date_var, where_clause, partition_type, col_map_name, start_dt, end_dt = fetch(table)
        logger.info(f"Processing {table_name}")
        result = {
            'table': aws_tbl,
            'database': data_base,
            'accessible': check_accessible(data_base, aws_tbl),
            'row_counts': None,
            'crosswalk': None,
            'column_types': None,
            'date_var': None,
            'where_clause': '',
            'vintages': []
        }

        if result['accessible']:
            columns_df = get_columns(data_base, table_name)
            result['crosswalk'] = check_crosswalk(table_name, columns_df, crosswalk_df, col_map_name)

            column_types = dict(zip(columns_df.iloc[:, 0].str.lower(), columns_df.iloc[:, 1]))
            result['column_types'] = column_types

            if date_var and not pd.isna(date_var):
                date_var = C.DateParser(date_var, column_types[date_var])
                date_var.get_fmt(aws_tbl, data_base=data_base)
                where_clause = date_var.merge_where(start_dt, end_dt, where_clause)
                row_counts_df = date_var.get_cnt(aws_tbl, where_clause, data_base=data_base)
                result['where_clause'] = where_clause
                result['row_counts'] = row_counts_df.to_dict('records')
                result['vintages'] = get_vintages(row_counts_df, date_var, partition_type)
                result['date_var'] = date_var.to_json()

        results.append(result)

    local_path = os.path.join(output_folder, f'{step_name}.json')
    s3.write_json(results, UPath(local_path))
    logger.info(f"Saved local copy to {local_path}")

    s3_path = s3.write_json(results, f'{step_name}.json')
    logger.info(f"Uploaded AWS meta check results to {s3_path}")

    return results

if __name__ == '__main__':
    main()