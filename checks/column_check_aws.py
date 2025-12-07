"""Part 2: AWS Column Check - Get column statistics (categorical/continuous) per vintage, with parallel execution support."""
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from loguru import logger
from utils_config import load_env, proc_aws
from utils_s3 import S3Manager
from utils_stats import build_column_sql, parse_stats_row

#>>> Setup logger to output folder <<<#
def add_logger(folder):
    os.makedirs(folder, exist_ok=True)
    logger.remove()
    if os.path.exists(fpath := os.path.join(folder, 'events.log')):
        os.remove(fpath)
    logger.add(fpath, level='INFO', format='{time:YY-MM-DD HH:mm:ss} | {level} | {message}', mode='w')

#>>> Get statistics for single column (worker function) <<<#
def get_column_stats(args):
    database, table_name, col_name, col_type, where_clause = args
    try:
        sql = build_column_sql(f'{database}.{table_name}', col_name, col_type, where_clause, is_oracle=False)
        df = proc_aws(sql, data_base=database)
        if not df.empty:
            return parse_stats_row(df.iloc[0].to_dict())
        return None
    except Exception as e:
        logger.error(f"Error getting stats for {col_name}: {e}")
        return None

#>>> Get statistics for all columns in a vintage <<<#
def get_vintage_stats(database, table_name, columns_with_types, vintage, max_workers=1):
    where_clause = vintage.get('where_clause', '1=1')

    args_list = [(database, table_name, col, typ, where_clause) for col, typ in columns_with_types.items()]

    if max_workers == 1:
        return {col: get_column_stats(args) for col, args in zip(columns_with_types.keys(), args_list)}

    all_stats = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_col = {executor.submit(get_column_stats, args): col for col, args in zip(columns_with_types.keys(), args_list)}

        for future in as_completed(future_to_col):
            col_name = future_to_col[future]
            try:
                stats = future.result()
                all_stats[col_name] = stats
            except Exception as e:
                logger.error(f"Worker failed for {col_name}: {e}")
                all_stats[col_name] = None

    return all_stats

#>>> Main execution <<<#
def main(max_workers=1):
    env = load_env('input_aws')
    run_name = env['RUN_NAME']
    category = env['CATEGORY']
    s3_bucket = env.get('S3_BUCKET', os.environ.get('S3_BUCKET'))

    output_folder = f'output/{run_name}'
    add_logger(output_folder)
    logger.info(f"Starting AWS column check: {run_name} / {category} (workers={max_workers})")

    s3 = S3Manager(s3_bucket, run_name)
    consolidated = s3.download_json('', f'{category}_meta_check.json')

    validated_tables = consolidated.get('validated_tables', [])
    logger.info(f"Processing {len(validated_tables)} validated tables")

    results = []

    for table_info in validated_tables:
        aws_table = table_info['aws_table']
        database = table_info['aws_database']
        tbl = aws_table.split('.')[1] if '.' in aws_table else aws_table
        comparable = table_info.get('comparable_columns_aws', [])
        aws_types = table_info.get('aws_column_types', {})
        validated_vintages = table_info.get('validated_vintages', [])

        if not comparable:
            logger.warning(f"No comparable columns for {aws_table}")
            continue

        columns_with_types = {col: aws_types.get(col, 'string') for col in comparable}
        logger.info(f"Processing {aws_table}: {len(comparable)} columns, {len(validated_vintages)} vintages")

        table_result = {'table': aws_table, 'columns': comparable, 'vintage_stats': []}

        for vintage in validated_vintages:
            logger.info(f"  Vintage {vintage['vintage']}: {len(comparable)} columns")
            where_clause = vintage.get('aws_where_clause', '1=1')

            vintage_obj = {'where_clause': where_clause}
            stats = get_vintage_stats(database, tbl, columns_with_types, vintage_obj, max_workers)

            table_result['vintage_stats'].append({
                'vintage': vintage['vintage'],
                'start_date': vintage['start_date'],
                'end_date': vintage['end_date'],
                'stats': stats
            })

        results.append(table_result)

    local_path = os.path.join(output_folder, f'aws_{category}_column_stats.json')
    with open(local_path, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    logger.info(f"Saved local copy to {local_path}")

    s3_path = s3.upload_json(results, 'column_check', f'aws_{category}_column_stats.json')
    logger.info(f"Uploaded to {s3_path}")

    return results

if __name__ == '__main__':
    import sys
    workers = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    main(max_workers=workers)
