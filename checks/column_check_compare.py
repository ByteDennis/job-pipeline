"""Part 3: Column Check Compare - Download PCDS and AWS column stats, compare, generate Excel report."""
import os
import json
import numpy as np
import pandas as pd
from loguru import logger
from utils_config import load_env
from utils_s3 import S3Manager
from utils_xlsx import ExcelReporter
from utils_date import parse_date_value
from datetime import datetime

#>>> Statistics Comparison Schema Configuration <<<#
STAT_COMPARISON_SCHEMA = {
    'count': {'type': 'exact', 'description': 'Row count must match exactly'},
    'distinct': {'type': 'exact', 'description': 'Distinct count must match exactly'},
    'min': {'type': 'flexible_string', 'try_date': True, 'try_numeric': True, 'description': 'Min value with type inference'},
    'max': {'type': 'flexible_string', 'try_date': True, 'try_numeric': True, 'description': 'Max value with type inference'},
    'avg': {'type': 'numeric_tolerance', 'atol': 1e-6, 'rtol': 1e-6, 'description': 'Average with numeric tolerance'},
    'std': {'type': 'numeric_tolerance', 'atol': 1e-6, 'rtol': 1e-6, 'description': 'Std deviation with numeric tolerance'},
    'missing': {'type': 'exact_with_zero_nan', 'description': 'Missing count (PCDS 0 = AWS NaN)'},
    'freq_top10': {'type': 'frequency_list', 'description': 'Top 10 frequency distribution'}
}

#>>> Comparator Functions <<<#
def compare_exact(val1, val2, **kwargs):
    """Exact match with NaN handling"""
    if pd.isna(val1) and pd.isna(val2):
        return True
    if pd.isna(val1) or pd.isna(val2):
        return False
    return val1 == val2

def compare_numeric_tolerance(val1, val2, atol=1e-6, rtol=1e-6, **kwargs):
    """Numeric comparison with configurable tolerance"""
    if pd.isna(val1) and pd.isna(val2):
        return True
    if pd.isna(val1) or pd.isna(val2):
        return False
    try:
        return np.isclose(float(val1), float(val2), atol=atol, rtol=rtol)
    except (ValueError, TypeError):
        return False

def compare_exact_with_zero_nan(val1, val2, **kwargs):
    """Exact match where 0 in PCDS equals NaN in AWS"""
    if pd.isna(val1) and pd.isna(val2):
        return True
    if val1 == 0 and pd.isna(val2):
        return True
    if pd.isna(val1) or pd.isna(val2):
        return False
    return val1 == val2

def compare_flexible_string(val1, val2, try_date=False, try_numeric=False, **kwargs):
    """String comparison with date/numeric type inference"""
    if pd.isna(val1) and pd.isna(val2):
        return True
    if pd.isna(val1) or pd.isna(val2):
        return False

    # Try date comparison
    if try_date:
        try:
            dat1 = parse_date_value(val1, in_pcds=True)
            dat2 = parse_date_value(val2, in_pcds=False)
            return dat1 == dat2
        except (ValueError, TypeError):
            pass

    # Try numeric comparison
    if try_numeric:
        try:
            num1, num2 = float(val1), float(val2)
            return np.isclose(num1, num2, atol=1e-6, rtol=1e-6)
        except (ValueError, TypeError):
            pass

    # Fall back to string comparison
    return str(val1) == str(val2)

def compare_frequency_list(val1, val2, **kwargs):
    """Compare frequency distribution lists element by element"""
    if pd.isna(val1) and pd.isna(val2):
        return True
    if not isinstance(val1, (list, tuple)) or not isinstance(val2, (list, tuple)):
        return str(val1) == str(val2)
    if len(val1) != len(val2):
        return False

    # Compare each tuple element by element
    for t1, t2 in zip(val1, val2):
        if not isinstance(t1, (list, tuple)) or not isinstance(t2, (list, tuple)):
            if t1 != t2:
                return False
            continue
        # Compare elements within each tuple
        if len(t1) != len(t2):
            return False
        for x1, x2 in zip(t1, t2):
            if not compare_flexible_string(x1, x2, try_date=True, try_numeric=True):
                return False
    return True

# Comparator dispatch map
COMPARATOR_MAP = {
    'exact': compare_exact,
    'numeric_tolerance': compare_numeric_tolerance,
    'exact_with_zero_nan': compare_exact_with_zero_nan,
    'flexible_string': compare_flexible_string,
    'frequency_list': compare_frequency_list
}

#>>> Compare two statistics dictionaries using schema <<<#
def compare_stats(pcds_stats, aws_stats, schema=STAT_COMPARISON_SCHEMA):
    """Compare PCDS and AWS stats using configuration-driven schema"""
    if not pcds_stats or not aws_stats:
        return False

    for field, rules in schema.items():
        pcds_val = pcds_stats.get(field)
        aws_val = aws_stats.get(field)

        comparator = COMPARATOR_MAP[rules['type']]
        if not comparator(pcds_val, aws_val, **rules):
            return False

    return True

#>>> Setup logger to output folder <<<#
def add_logger(folder):
    os.makedirs(folder, exist_ok=True)
    logger.remove()
    if os.path.exists(fpath := os.path.join(folder, 'events.log')):
        os.remove(fpath)
    logger.add(fpath, level='INFO', format='{time:YY-MM-DD HH:mm:ss} | {level} | {message}', mode='w')


#>>> Prepare table detail sections for Excel <<<#
def prepare_table_sections(pcds_result, aws_result, column_mapping, mismatched_columns_set):
    """Prepare Excel sections with PCDS vs AWS comparison per vintage (transposed format)"""
    sections = []

    pcds_vintages = {v['vintage']: v for v in pcds_result['vintage_stats']}
    aws_vintages = {v['vintage']: v for v in aws_result['vintage_stats']}

    for vintage_key in sorted(set(pcds_vintages.keys()) & set(aws_vintages.keys())):
        pcds_v = pcds_vintages[vintage_key]
        aws_v = aws_vintages[vintage_key]

        # Build dataframes using column mapping
        pcds_stats_dict = {}
        aws_stats_dict = {}

        for pcds_col, aws_col in column_mapping.items():
            # Get stats using correct column names (PCDS uppercase, AWS lowercase)
            pcds_stats = pcds_v['stats'].get(pcds_col.upper())
            aws_stats = aws_v['stats'].get(aws_col.lower())

            if pcds_stats and aws_stats:
                # Use PCDS column name as the key for both (for display)
                pcds_stats_dict[pcds_col] = {
                    'Count': pcds_stats.get('count'),
                    'Distinct': pcds_stats.get('distinct'),
                    'Min': pcds_stats.get('min'),
                    'Max': pcds_stats.get('max'),
                    'Mean': pcds_stats.get('avg'),
                    'Std': pcds_stats.get('std'),
                    'Missing': pcds_stats.get('missing'),
                    'Freq': pcds_stats.get('freq_top10', '')
                }

                aws_stats_dict[pcds_col] = {
                    'Count': aws_stats.get('count'),
                    'Distinct': aws_stats.get('distinct'),
                    'Min': aws_stats.get('min'),
                    'Max': aws_stats.get('max'),
                    'Mean': aws_stats.get('avg'),
                    'Std': aws_stats.get('std'),
                    'Missing': aws_stats.get('missing'),
                    'Freq': aws_stats.get('freq_top10', '')
                }

        if pcds_stats_dict:
            # Create dataframes: columns=stats, index=column_names
            pcds_df = pd.DataFrame(pcds_stats_dict).T
            aws_df = pd.DataFrame(aws_stats_dict).T

            # Reorder: mismatched columns first, then the rest
            mismatch_cols = [c for c in pcds_df.index if c in mismatched_columns_set]
            rest_cols = [c for c in pcds_df.index if c not in mismatched_columns_set]
            pcds_df = pcds_df.loc[mismatch_cols + rest_cols]
            aws_df = aws_df.loc[mismatch_cols + rest_cols]

            # Transpose: stats as rows, columns as columns
            pcds_df_T = pcds_df.T
            aws_df_T = aws_df.T

            sections.append({
                'vintage': vintage_key,
                'pcds_label': pcds_result['table'].split('.')[-1],
                'aws_label': aws_result['table'].lower(),
                'pcds_df': pcds_df_T,
                'aws_df': aws_df_T,
                'num_mismatched': len(mismatch_cols)
            })

    return sections

#>>> Analyze column quality across all vintages <<<#
def analyze_column_quality(pcds_result, aws_result, column_mapping, top_n=5):
    """Compare all columns across all vintages and identify top key columns for hash"""
    all_columns = list(column_mapping.keys())
    mismatched_columns = set()
    column_distinct_counts = {}

    # Compare each column across all vintages
    for pcds_v, aws_v in zip(pcds_result['vintage_stats'], aws_result['vintage_stats']):
        for pcds_col, aws_col in column_mapping.items():
            # Get stats using correct column names (case-sensitive)
            pcds_stats = pcds_v['stats'].get(pcds_col.upper())
            aws_stats = aws_v['stats'].get(aws_col.lower())

            # Track distinct count for key column selection (use max across vintages)
            if pcds_stats:
                distinct_count = pcds_stats.get('distinct', 0)
                if pcds_col not in column_distinct_counts:
                    column_distinct_counts[pcds_col] = distinct_count
                else:
                    column_distinct_counts[pcds_col] = max(column_distinct_counts[pcds_col], distinct_count)

            # Compare using configuration-driven schema
            if not compare_stats(pcds_stats, aws_stats):
                mismatched_columns.add(pcds_col)

    # Clean columns = all stats match across all vintages
    clean_columns = [col for col in all_columns if col not in mismatched_columns]

    # Get top N key columns by distinct count (only from clean columns)
    top_key_columns = sorted(
        [(col, cnt) for col, cnt in column_distinct_counts.items() if col in clean_columns],
        key=lambda x: x[1],
        reverse=True
    )[:top_n]

    return {
        'all_columns': all_columns,
        'mismatched_columns': sorted(list(mismatched_columns)),
        'clean_columns': clean_columns,
        'top_key_columns': [col for col, _ in top_key_columns],
        'key_column_stats': {col: cnt for col, cnt in top_key_columns}
    }

#>>> Build consolidated column check metadata <<<#
def build_consolidated_column_metadata(pcds_results, aws_results, meta_check):
    """Build consolidated metadata with column quality analysis for hash step"""
    validated_tables_meta = {t['pcds_table']: t for t in meta_check.get('validated_tables', [])}
    validated_tables = []

    for pcds, aws in zip(pcds_results, aws_results):
        table_name = pcds['table']
        table_meta = validated_tables_meta.get(table_name, {})

        # Get column mapping (PCDS uppercase -> AWS lowercase) directly from meta_check
        column_mapping = table_meta.get('column_mapping', {})

        # Analyze quality using proper column mapping
        quality = analyze_column_quality(pcds, aws, column_mapping)

        validated_tables.append({
            # Table identifiers
            'pcds_table': table_meta.get('pcds_table', table_name),
            'aws_table': table_meta.get('aws_table'),
            'pcds_svc': table_meta.get('pcds_svc'),
            'aws_database': table_meta.get('aws_database'),

            # Date variables
            'pcds_date_var': table_meta.get('pcds_date_var'),
            'aws_date_var': table_meta.get('aws_date_var'),

            # Where clauses (needed for hash queries)
            'pcds_where': table_meta.get('pcds_where'),
            'aws_where': table_meta.get('aws_where'),

            # Validated vintages with where clauses
            'validated_vintages': table_meta.get('validated_vintages', []),

            # Column information for hash step
            'column_mapping': column_mapping,              # {PCDS_COL: aws_col}
            'all_columns': quality['all_columns'],         # All comparable columns
            'key_columns': quality['top_key_columns'],     # Top N for hash
            'clean_columns': quality['clean_columns'],     # All stats match (renamed from matched_columns for consistency with old naming)
            'mismatched_columns': quality['mismatched_columns']  # Any stat mismatch
        })

    return {
        'pcds_results': pcds_results,
        'aws_results': aws_results,
        'validated_tables': validated_tables
    }

#>>> Main execution <<<#
def main():
    env = load_env('input_pcds')
    run_name = env['RUN_NAME']
    category = env['CATEGORY']
    s3_bucket = env.get('S3_BUCKET', os.environ.get('S3_BUCKET'))

    output_folder = f'output/{run_name}'
    add_logger(output_folder)
    logger.info(f"Starting column check comparison: {run_name} / {category}")

    s3 = S3Manager(s3_bucket, run_name)

    logger.info("Downloading PCDS column stats from S3")
    pcds_results = s3.download_json('column_check', f'pcds_{category}_column_stats.json')

    logger.info("Downloading AWS column stats from S3")
    aws_results = s3.download_json('column_check', f'aws_{category}_column_stats.json')

    logger.info("Downloading meta check from S3")
    meta_check = s3.download_json('', f'{category}_meta_check.json')

    consolidated = build_consolidated_column_metadata(pcds_results, aws_results, meta_check)

    for table_info in consolidated['validated_tables']:
        logger.info(f"{table_info['pcds_table']}: {len(table_info['clean_columns'])}/{len(table_info['all_columns'])} clean columns, "
                   f"key columns: {table_info['key_columns']}")

    s3.upload_json(consolidated, '', f'{category}_column_check.json')
    logger.info(f"Uploaded consolidated column_check.json to S3")

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = os.path.join(output_folder, f'column_check_comparison_{category}_{timestamp}.xlsx')

    logger.info(f"Generating Excel report: {report_path}")

    # Build mapping of table_name -> table_info for quick lookup
    table_info_map = {t['pcds_table']: t for t in consolidated['validated_tables']}

    with ExcelReporter(report_path) as reporter:
        # Build summary data with per-vintage details
        summary_rows = []
        for pcds, aws in zip(pcds_results, aws_results):
            table_name = pcds['table']
            table_info = table_info_map.get(table_name, {})
            # Extract the 'comparable' mapping from column_mapping
            column_mapping = table_info.get('column_mapping', {})
            total_cols = len(column_mapping)

            # Iterate through each vintage
            for pcds_v, aws_v in zip(pcds['vintage_stats'], aws['vintage_stats']):
                vintage = pcds_v['vintage']
                mismatched_cols = set()

                # Count mismatches for this vintage using configuration-driven comparison
                for pcds_col, aws_col in column_mapping.items():
                    pcds_stats = pcds_v['stats'].get(pcds_col.upper())
                    aws_stats = aws_v['stats'].get(aws_col.lower())

                    # Use compare_stats to check ALL fields (not just count and distinct)
                    if not compare_stats(pcds_stats, aws_stats):
                        mismatched_cols.add(pcds_col)

                matched = total_cols - len(mismatched_cols)
                match_rate = (matched / total_cols * 100) if total_cols > 0 else 0

                summary_rows.append([
                    table_name,
                    vintage,
                    total_cols,
                    matched,
                    len(mismatched_cols),
                    f'{match_rate:.1f}%'
                ])

        # Create summary sheet
        reporter.create_summary_sheet(
            title='Column Statistics Comparison',
            headers=['Dataset', 'Vintage', 'Total Columns', 'Matched Columns', 'Mismatched Columns', 'Match Rate %'],
            data_rows=summary_rows,
            color_by_match_rate=True
        )

        # Create detail sheets with comparisons
        for pcds, aws in zip(pcds_results, aws_results):
            table_name = pcds['table']
            table_info = table_info_map.get(table_name, {})
            column_mapping = table_info.get('column_mapping', {})

            # Identify all mismatched columns for this table (across all vintages)
            mismatched_columns_set = set()
            for pcds_v, aws_v in zip(pcds['vintage_stats'], aws['vintage_stats']):
                for pcds_col, aws_col in column_mapping.items():
                    pcds_stats = pcds_v['stats'].get(pcds_col.upper())
                    aws_stats = aws_v['stats'].get(aws_col.lower())

                    # Use compare_stats to check ALL fields
                    if not compare_stats(pcds_stats, aws_stats):
                        mismatched_columns_set.add(pcds_col)

            sections = prepare_table_sections(pcds, aws, column_mapping, mismatched_columns_set)
            reporter.create_column_comparison_sheet(pcds['table'].split('.')[-1], sections)

    logger.info(f"Report saved to {report_path}")

    return report_path

if __name__ == '__main__':
    main()
