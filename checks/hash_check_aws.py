"""Part 2: AWS Hash Check - Compute row hashes per vintage with parallel execution and debug mode."""
if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv('input_pcds')

import os
from upath import UPath
from concurrent.futures import ThreadPoolExecutor, as_completed
from loguru import logger

import constant
import utils_config as C
from utils_s3 import S3Manager
from utils_hash import build_athena_hash_expr

#>>> Compute hash for single vintage (worker function) <<<#
def compute_vintage_hash(args):
    database, table_name, columns_with_types, key_columns, vintage, debug = args
    try:
        col_specs = [{'column_name': col, 'data_type': typ} for col, typ in columns_with_types.items()]
        hash_result = build_athena_hash_expr(col_specs)

        where_clause = vintage.get('where_clause', '1=1')
        key_select = ', '.join(key_columns) if key_columns else 'row_number() OVER () AS row_id'

        if debug:
            sql = f"SELECT {key_select}, {hash_result['debug_select']} FROM {database}.{table_name} WHERE {where_clause}"
        else:
            sql = f"SELECT {key_select}, {hash_result['hash_expr']} AS hash_value FROM {database}.{table_name} WHERE {where_clause}"

        df = C.proc_aws(sql, data_base=database)

        if debug:
            return df.to_dict('records')
        else:
            return {
                'total_rows': len(df),
                'unique_hashes': df['hash_value'].nunique() if not df.empty else 0,
                'hashes': df.to_dict('records') if not df.empty else []
            }
    except Exception as e:
        logger.error(f"Error computing hash for vintage {vintage.get('vintage')}: {e}")
        return None

#>>> Compute hashes for all vintages of a table <<<#
def compute_table_hashes(database, table_name, columns_with_types, key_columns, vintages, max_workers=1, debug=False):
    args_list = [(database, table_name, columns_with_types, key_columns, v, debug) for v in vintages]

    if max_workers == 1:
        return [compute_vintage_hash(args) for args in args_list]

    all_results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_vintage = {executor.submit(compute_vintage_hash, args): v for args, v in zip(args_list, vintages)}

        for future in as_completed(future_to_vintage):
            vintage = future_to_vintage[future]
            try:
                result = future.result()
                all_results.append(result)
            except Exception as e:
                logger.error(f"Worker failed for vintage {vintage.get('vintage')}: {e}")
                all_results.append(None)

    return all_results

#>>> Main execution <<<#
def main(max_workers=1, debug=False):
    run_name, category, config_path = C.get_env('RUN_NAME', 'CATEGORY', 'HASH_STEP')

    cfg = C.load_config(config_path)
    step_name = cfg.output.step_name.format(p='aws')
    suffix = '_debug' if debug else ''
    output_folder = cfg.output.disk.format(name=run_name)
    C.add_logger(output_folder, name=f'{step_name}{suffix}')
    logger.info(f"Starting AWS hash check: {run_name} | {category} (workers={max_workers}, debug={debug})")

    s3_bucket = cfg.output.s3.format(name=run_name)
    s3 = S3Manager(s3_bucket)

    consolidated = s3.read_json(f'{cfg.output.summary.format(s="column")}.json')
    validated_tables = consolidated.get('validated_tables', [])
    logger.info(f"Processing {len(validated_tables)} validated tables")

    results = []

    for table_info in validated_tables:
        aws_table = table_info['aws_table']
        database = table_info['aws_database']
        tbl = aws_table.split('.')[1] if '.' in aws_table else aws_table
        clean_columns = table_info.get('clean_columns', [])
        key_columns = table_info.get('key_columns', [])
        aws_types = table_info.get('clean_aws_column_types', {})
        validated_vintages = table_info.get('validated_vintages', [])

        if not clean_columns:
            logger.warning(f"No clean columns for {aws_table}")
            continue

        columns_with_types = {col: aws_types.get(col, 'string') for col in clean_columns}
        logger.info(f"Processing {aws_table}: {len(clean_columns)} clean columns, key: {key_columns}")

        table_result = {
            'table': aws_table,
            'clean_columns': clean_columns,
            'key_columns': key_columns,
            'mismatched_columns': table_info.get('mismatched_columns', []),
            'vintage_hashes': []
        }

        vintage_objs = [{'where_clause': v.get('aws_where_clause', '1=1')} for v in validated_vintages]
        hash_results = compute_table_hashes(database, tbl, columns_with_types, key_columns, vintage_objs, max_workers, debug)

        for vintage, hash_data in zip(validated_vintages, hash_results):
            logger.info(f"  Vintage {vintage['vintage']}: {hash_data.get('total_rows', 'N/A') if not debug else len(hash_data)} rows")
            table_result['vintage_hashes'].append({
                'vintage': vintage['vintage'],
                'start_date': vintage['start_date'],
                'end_date': vintage['end_date'],
                'hash_data': hash_data
            })

        results.append(table_result)

    local_path = os.path.join(output_folder, f'{step_name}{suffix}.json')
    s3.write_json(results, UPath(local_path))
    logger.info(f"Saved local copy to {local_path}")

    s3_path = s3.write_json(results, f'{step_name}{suffix}.json')
    logger.info(f"Uploaded AWS hash check results to {s3_path}")

    return results

if __name__ == '__main__':
    import sys
    workers = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    debug_mode = sys.argv[2].lower() == 'true' if len(sys.argv) > 2 else False
    main(max_workers=workers, debug=debug_mode)
