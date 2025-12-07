"""Part 2: AWS Meta Check - Download PCDS results from S3, check AWS tables, and upload results back to S3."""
import os
import json
import pandas as pd
from loguru import logger
from utils_config import load_env, get_config, get_input_tables, get_column_mappings, proc_aws
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
def check_accessible(database, table_name):
    try:
        sql = f"SELECT 1 FROM {table_name} LIMIT 1"
        proc_aws(sql, data_base=database)
        return True
    except Exception as e:
        logger.error(f"Table {table_name} not accessible: {e}")
        return False

#>>> Get row counts grouped by raw date variable <<<#
def get_row_counts(database, table_name, date_var, where_clause=None):
    where = f"WHERE {where_clause}" if where_clause else ""
    sql = f"SELECT {date_var}, COUNT(*) as cnt FROM {table_name} {where} GROUP BY {date_var}"
    df = proc_aws(sql, data_base=database)
    df['date_std'] = df.iloc[:, 0].apply(parse_date_to_std)
    return df

#>>> Get table columns with types <<<#
def get_columns(database, table_name):
    sql = f"DESCRIBE {table_name}"
    return proc_aws(sql, data_base=database)

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
    return {'comparable': comparable, 'tokenized': tokenized, 'aws_only': aws_only, 'unmapped': unmapped}

#>>> Generate vintages from row count data <<<#
def get_vintages(row_counts_df, date_var, partition_type, db_type='athena'):
    if row_counts_df.empty or row_counts_df['date_std'].isna().all():
        return []

    min_date = row_counts_df['date_std'].min()
    max_date = row_counts_df['date_std'].max()

    var_type, var_format = detect_date_format(row_counts_df.iloc[:, 0].tolist())

    return generate_vintages(min_date, max_date, partition_type, date_var, var_type, var_format, db_type)

#>>> Main execution <<<#
def main():
    env = load_env('input_aws')
    run_name = env['RUN_NAME']
    category = env['CATEGORY']
    s3_bucket = env.get('S3_BUCKET', os.environ.get('S3_BUCKET'))

    output_folder = f'output/{run_name}'
    add_logger(output_folder)
    logger.info(f"Starting AWS meta check: {run_name} / {category}")

    s3 = S3Manager(s3_bucket, run_name)

    # Download PCDS results and config files from S3 (uploaded by meta_check_pcds.py)
    pcds_results = s3.download_json('meta_check', f'pcds_{category}_meta.json')
    logger.info(f"Downloaded PCDS results: {len(pcds_results)} tables")

    tables_df = s3.download_csv('', 'input_tables.csv')
    crosswalk_df = s3.download_csv('', 'crosswalk.csv')
    logger.info(f"Downloaded config from S3: {len(tables_df)} tables, {len(crosswalk_df)} crosswalk mappings")

    results = []

    for _, table in tables_df.iterrows():
        table_name = table['aws_tbl']
        database = table_name.split('.')[0]
        date_var = table.get('aws_var')
        where_clause = table.get('aws_where')
        partition_type = table.get('partition', 'month')
        col_map_name = table.get('col_map', table['pcds_tbl'].split('.')[-1])

        logger.info(f"Processing {table_name}")

        result = {
            'table': table_name,
            'accessible': check_accessible(database, table_name),
            'row_counts': None,
            'crosswalk': None,
            'vintages': []
        }

        if result['accessible']:
            columns_df = get_columns(database, table_name)
            result['crosswalk'] = check_crosswalk(table_name, columns_df, crosswalk_df, col_map_name)

            column_types = dict(zip(columns_df.iloc[:, 0].str.lower(), columns_df.iloc[:, 1]))
            result['column_types'] = column_types

            if date_var and not pd.isna(date_var):
                row_counts_df = get_row_counts(database, table_name, date_var, where_clause)
                result['row_counts'] = row_counts_df.to_dict('records')
                result['vintages'] = get_vintages(row_counts_df, date_var, partition_type)

        results.append(result)

    local_path = os.path.join(output_folder, f'aws_{category}_meta.json')
    with open(local_path, 'w') as f:
        json.dump(results, f, indent=2)
    logger.info(f"Saved local copy to {local_path}")

    s3_path = s3.upload_json(results, 'meta_check', f'aws_{category}_meta.json')
    logger.info(f"Uploaded AWS meta check results to {s3_path}")

    return results

if __name__ == '__main__':
    main()
