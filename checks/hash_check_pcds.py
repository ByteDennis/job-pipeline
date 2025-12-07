"""Part 1: PCDS Hash Check - Compute row hashes per vintage with parallel execution and debug mode."""
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from loguru import logger
from utils_config import load_env, proc_pcds
from utils_s3 import S3Manager
from utils_hash import build_oracle_hash_expr

#>>> Setup logger to output folder <<<#
def add_logger(folder):
    os.makedirs(folder, exist_ok=True)
    logger.remove()
    if os.path.exists(fpath := os.path.join(folder, 'events.log')):
        os.remove(fpath)
    logger.add(fpath, level='INFO', format='{time:YY-MM-DD HH:mm:ss} | {level} | {message}', mode='w')

#>>> Compute hash for single vintage (worker function) <<<#
def compute_vintage_hash(args):
    svc, table_name, columns_with_types, key_columns, vintage, debug = args
    try:
        col_specs = [{'column_name': col, 'data_type': typ} for col, typ in columns_with_types.items()]
        hash_result = build_oracle_hash_expr(col_specs)

        where_clause = vintage.get('where_clause', '1=1')
        key_select = ', '.join(key_columns) if key_columns else 'ROWNUM AS row_id'

        if debug:
            sql = f"SELECT {key_select}, {hash_result['debug_select']} FROM {table_name} WHERE {where_clause}"
        else:
            sql = f"SELECT {key_select}, {hash_result['hash_expr']} AS hash_value FROM {table_name} WHERE {where_clause}"

        df = proc_pcds(sql, service_name=svc)

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
def compute_table_hashes(svc, table_name, columns_with_types, key_columns, vintages, max_workers=1, debug=False):
    args_list = [(svc, table_name, columns_with_types, key_columns, v, debug) for v in vintages]

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
    env = load_env('input_pcds')
    run_name = env['RUN_NAME']
    category = env['CATEGORY']
    s3_bucket = env.get('S3_BUCKET', os.environ.get('S3_BUCKET'))

    output_folder = f'output/{run_name}'
    add_logger(output_folder)
    logger.info(f"Starting PCDS hash check: {run_name} / {category} (workers={max_workers}, debug={debug})")

    s3 = S3Manager(s3_bucket, run_name)
    consolidated = s3.download_json('', f'{category}_column_check.json')

    validated_tables = consolidated.get('validated_tables', [])
    logger.info(f"Processing {len(validated_tables)} validated tables")

    results = []

    for table_info in validated_tables:
        pcds_table = table_info['pcds_table']
        svc = table_info['pcds_svc']
        clean_columns = table_info.get('clean_columns', [])
        key_columns = table_info.get('key_columns', [])
        pcds_types = table_info.get('clean_pcds_column_types', {})
        validated_vintages = table_info.get('validated_vintages', [])

        if not clean_columns:
            logger.warning(f"No clean columns for {pcds_table}")
            continue

        columns_with_types = {col: pcds_types.get(col, 'VARCHAR2') for col in clean_columns}
        logger.info(f"Processing {pcds_table}: {len(clean_columns)} clean columns, key: {key_columns}")

        table_result = {
            'table': pcds_table,
            'clean_columns': clean_columns,
            'key_columns': key_columns,
            'mismatched_columns': table_info.get('mismatched_columns', []),
            'vintage_hashes': []
        }

        vintage_objs = [{'where_clause': v.get('pcds_where_clause', '1=1')} for v in validated_vintages]
        hash_results = compute_table_hashes(svc, pcds_table, columns_with_types, key_columns, vintage_objs, max_workers, debug)

        for vintage, hash_data in zip(validated_vintages, hash_results):
            logger.info(f"  Vintage {vintage['vintage']}: {hash_data.get('total_rows', 'N/A') if not debug else len(hash_data)} rows")
            table_result['vintage_hashes'].append({
                'vintage': vintage['vintage'],
                'start_date': vintage['start_date'],
                'end_date': vintage['end_date'],
                'hash_data': hash_data
            })

        results.append(table_result)

    suffix = '_debug' if debug else ''
    local_path = os.path.join(output_folder, f'pcds_{category}_hash{suffix}.json')
    with open(local_path, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    logger.info(f"Saved local copy to {local_path}")

    s3_path = s3.upload_json(results, 'hash_check', f'pcds_{category}_hash{suffix}.json')
    logger.info(f"Uploaded to {s3_path}")

    return results

if __name__ == '__main__':
    import sys
    workers = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    debug_mode = sys.argv[2].lower() == 'true' if len(sys.argv) > 2 else False
    main(max_workers=workers, debug=debug_mode)
