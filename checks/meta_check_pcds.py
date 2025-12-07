"""Part 1: PCDS Meta Check - Check table accessibility, row counts, crosswalk mappings, and generate vintages then upload to S3."""
import os
import json
import pandas as pd
from loguru import logger
from utils_config import load_env, get_config, get_input_tables, get_column_mappings, proc_pcds
from utils_s3 import S3Manager
from utils_date import parse_date_to_std, detect_date_format, generate_vintages

#>>> Setup logger to output folder <<<#
def add_logger(folder):
    os.makedirs(folder, exist_ok=True)
    logger.remove()
    if os.path.exists(fpath := os.path.join(folder, 'events.log')):
        os.remove(fpath)
    logger.add(fpath, level='INFO', format='{time:YY-MM-DD HH:mm:ss} | {level} | {message}', mode='w')

#>>> Check if table is accessible <<<#
def check_accessible(conn, table_name):
    try:
        sql = f"SELECT 1 FROM {table_name} WHERE ROWNUM = 1"
        proc_pcds(sql, service_name=conn)
        return True
    except Exception as e:
        logger.error(f"Table {table_name} not accessible: {e}")
        return False

#>>> Get row counts grouped by raw date variable <<<#
def get_row_counts(conn, table_name, date_var, where_clause=None):
    where = f"WHERE {where_clause}" if where_clause else ""
    sql = f"SELECT {date_var}, COUNT(*) as cnt FROM {table_name} {where} GROUP BY {date_var}"
    df = proc_pcds(sql, service_name=conn)
    df['date_std'] = df.iloc[:, 0].apply(parse_date_to_std)
    return df

#>>> Get table columns with types <<<#
def get_columns(conn, table_name):
    svc, tbl = table_name.split('.')
    sql = f"""
    SELECT column_name, data_type
    FROM all_tab_columns
    WHERE owner = UPPER('{svc}') AND table_name = UPPER('{tbl}')
    ORDER BY column_id
    """
    return proc_pcds(sql, service_name=conn)

#>>> Check crosswalk completeness <<<#
def check_crosswalk(table_name, columns_df, crosswalk_df, col_map_name):
    actual_cols = set(columns_df['COLUMN_NAME'].str.upper())
    mapped = crosswalk_df[crosswalk_df['col_map'] == col_map_name.lower()]

    comparable, tokenized, pcds_only = [], [], []
    for _, row in mapped.iterrows():
        pcds_col = row['pcds_col'].upper()
        if pcds_col in actual_cols:
            if row['is_tokenized']:
                tokenized.append(pcds_col)
            elif pd.notna(row['aws_col']):
                comparable.append(pcds_col)
            else:
                pcds_only.append(pcds_col)

    unmapped = list(actual_cols - set(comparable) - set(tokenized) - set(pcds_only))
    return {'comparable': comparable, 'tokenized': tokenized, 'pcds_only': pcds_only, 'unmapped': unmapped}

#>>> Generate vintages from row count data <<<#
def get_vintages(row_counts_df, date_var, partition_type, db_type='oracle'):
    if row_counts_df.empty or row_counts_df['date_std'].isna().all():
        return []

    min_date = row_counts_df['date_std'].min()
    max_date = row_counts_df['date_std'].max()

    var_type, var_format = detect_date_format(row_counts_df.iloc[:, 0].tolist())

    return generate_vintages(min_date, max_date, partition_type, date_var, var_type, var_format, db_type)

#>>> Main execution <<<#
def main():
    env = load_env('input_pcds')
    run_name = env['RUN_NAME']
    category = env['CATEGORY']
    s3_bucket = env.get('S3_BUCKET', os.environ.get('S3_BUCKET'))

    output_folder = f'output/{run_name}'
    add_logger(output_folder)
    logger.info(f"Starting PCDS meta check: {run_name} / {category}")

    config = get_config(category)
    tables_df = get_input_tables(category)
    crosswalk_df = get_column_mappings(category)

    # Filter for enabled tables only
    enabled_tables = tables_df[tables_df['enabled']].copy()
    logger.info(f"Found {len(enabled_tables)} enabled tables out of {len(tables_df)} total")

    # Filter crosswalk for only enabled table mappings
    enabled_col_maps = enabled_tables['col_map'].unique() if 'col_map' in enabled_tables.columns else enabled_tables['pcds_tbl'].str.split('.').str[-1].unique()
    filtered_crosswalk = crosswalk_df[crosswalk_df['col_map'].isin(enabled_col_maps)].copy()
    logger.info(f"Filtered crosswalk: {len(filtered_crosswalk)} mappings for {len(enabled_col_maps)} tables")

    # Upload filtered config files to S3 for other machines to use
    s3 = S3Manager(s3_bucket, run_name)
    s3.upload_csv(enabled_tables, '', 'input_tables.csv')
    s3.upload_csv(filtered_crosswalk, '', 'crosswalk.csv')
    logger.info("Uploaded filtered config files to S3 root")

    results = []

    for _, table in enabled_tables.iterrows():
        table_name = table['pcds_tbl']
        svc = table['pcds_svc']
        date_var = table.get('pcds_var')
        where_clause = table.get('pcds_where')
        partition_type = table.get('partition', 'month')
        col_map_name = table.get('col_map', table_name.split('.')[-1])

        logger.info(f"Processing {table_name}")

        result = {
            'table': table_name,
            'accessible': check_accessible(svc, table_name),
            'row_counts': None,
            'crosswalk': None,
            'vintages': []
        }

        if result['accessible']:
            columns_df = get_columns(svc, table_name)
            result['crosswalk'] = check_crosswalk(table_name, columns_df, filtered_crosswalk, col_map_name)

            column_types = dict(zip(columns_df['COLUMN_NAME'].str.upper(), columns_df['DATA_TYPE']))
            result['column_types'] = column_types

            if date_var and not pd.isna(date_var):
                row_counts_df = get_row_counts(svc, table_name, date_var, where_clause)
                result['row_counts'] = row_counts_df.to_dict('records')
                result['vintages'] = get_vintages(row_counts_df, date_var, partition_type)

        results.append(result)

    local_path = os.path.join(output_folder, f'pcds_{category}_meta.json')
    with open(local_path, 'w') as f:
        json.dump(results, f, indent=2)
    logger.info(f"Saved local copy to {local_path}")

    s3_path = s3.upload_json(results, 'meta_check', f'pcds_{category}_meta.json')
    logger.info(f"Uploaded PCDS meta check results to {s3_path}")

    return results

if __name__ == '__main__':
    main()
