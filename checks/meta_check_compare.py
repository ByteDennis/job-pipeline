"""Part 3: Compare Report - Download PCDS and AWS results from S3, compare them, and generate Excel report."""
import os
import json
import pandas as pd
from loguru import logger
from utils_config import load_env
from utils_s3 import S3Manager
from utils_xlsx import ExcelReporter
from datetime import datetime

#>>> Setup logger to output folder <<<#
def add_logger(folder):
    os.makedirs(folder, exist_ok=True)
    logger.remove()
    if os.path.exists(fpath := os.path.join(folder, 'events.log')):
        os.remove(fpath)
    logger.add(fpath, level='INFO', format='{time:YY-MM-DD HH:mm:ss} | {level} | {message}', mode='w')

#>>> Prepare table detail sections <<<#
def prepare_table_sections(pcds_result, aws_result):
    sections = []

    sections.append({
        'title': 'Table Information',
        'rows': [
            ['PCDS Table:', pcds_result['table']],
            ['AWS Table:', aws_result['table']],
            ['PCDS Accessible:', '✓' if pcds_result['accessible'] else '✗'],
            ['AWS Accessible:', '✓' if aws_result['accessible'] else '✗']
        ]
    })

    if pcds_result['row_counts'] and aws_result['row_counts']:
        pcds_counts = {r['date_std']: r['cnt'] for r in pcds_result['row_counts'] if r.get('date_std')}
        aws_counts = {r['date_std']: r['cnt'] for r in aws_result['row_counts'] if r.get('date_std')}
        all_dates = sorted(set(pcds_counts.keys()) | set(aws_counts.keys()))

        count_data = []
        for date in all_dates:
            pcds_cnt = pcds_counts.get(date, 0)
            aws_cnt = aws_counts.get(date, 0)
            count_data.append({
                'Date': date,
                'PCDS Count': pcds_cnt,
                'AWS Count': aws_cnt,
                'Difference': pcds_cnt - aws_cnt,
                'Match': '✓' if pcds_cnt == aws_cnt else '✗'
            })

        if count_data:
            sections.append({
                'title': f'Row Counts by Date ({len(count_data)} dates)',
                'dataframe': pd.DataFrame(count_data)
            })

    if pcds_result['crosswalk'] and aws_result['crosswalk']:
        pcds_cw = pcds_result['crosswalk']
        aws_cw = aws_result['crosswalk']

        sections.append({
            'title': 'Crosswalk Summary',
            'rows': [
                ['PCDS Comparable Columns:', len(pcds_cw['comparable'])],
                ['AWS Comparable Columns:', len(aws_cw['comparable'])],
                ['PCDS Tokenized Columns:', len(pcds_cw['tokenized'])],
                ['AWS Tokenized Columns:', len(aws_cw['tokenized'])],
                ['PCDS Only Columns:', len(pcds_cw['pcds_only'])],
                ['AWS Only Columns:', len(aws_cw['aws_only'])],
                ['PCDS Unmapped Columns:', len(pcds_cw['unmapped'])],
                ['AWS Unmapped Columns:', len(aws_cw['unmapped'])]
            ]
        })

        if pcds_cw['pcds_only']:
            sections.append({
                'title': f"PCDS Only Columns ({len(pcds_cw['pcds_only'])})",
                'rows': [[col] for col in pcds_cw['pcds_only']]
            })

        if aws_cw['aws_only']:
            sections.append({
                'title': f"AWS Only Columns ({len(aws_cw['aws_only'])})",
                'rows': [[col] for col in aws_cw['aws_only']]
            })

    if pcds_result['vintages'] or aws_result['vintages']:
        pcds_vintages = {v['vintage']: v for v in pcds_result['vintages']} if pcds_result['vintages'] else {}
        aws_vintages = {v['vintage']: v for v in aws_result['vintages']} if aws_result['vintages'] else {}

        all_vintages = sorted(set(pcds_vintages.keys()) | set(aws_vintages.keys()))
        vintage_data = []
        for v in all_vintages:
            pcds_v = pcds_vintages.get(v, {})
            aws_v = aws_vintages.get(v, {})
            vintage_data.append({
                'Vintage': v,
                'PCDS Start': pcds_v.get('start_date', ''),
                'PCDS End': pcds_v.get('end_date', ''),
                'AWS Start': aws_v.get('start_date', ''),
                'AWS End': aws_v.get('end_date', ''),
                'Match': '✓' if (v in pcds_vintages and v in aws_vintages) else '✗'
            })

        if vintage_data:
            sections.append({
                'title': f'Vintages Comparison ({len(vintage_data)} vintages)',
                'dataframe': pd.DataFrame(vintage_data)
            })

    return sections

#>>> Build column mapping from crosswalk <<<#
def build_column_mapping(crosswalk_df, col_map_name, comparable_pcds, comparable_aws):
    """Build PCDS->AWS column mapping using crosswalk

    Args:
        crosswalk_df: DataFrame with col_map, pcds_col, aws_col, is_tokenized
        col_map_name: Name of the column map for this table
        comparable_pcds: List of PCDS comparable columns (uppercase)
        comparable_aws: List of AWS comparable columns (lowercase)

    Returns:
        Dict with:
            - column_mapping: {PCDS_COL: aws_col}
            - unmapped_pcds: List of PCDS columns without AWS match
            - unmapped_aws: List of AWS columns without PCDS match
    """
    # Filter crosswalk for this table
    table_crosswalk = crosswalk_df[crosswalk_df['col_map'] == col_map_name.lower()].copy()

    # Convert to sets for fast lookup
    pcds_set = set(col.upper() for col in comparable_pcds)
    aws_set = set(col.lower() for col in comparable_aws)

    # Build mapping from crosswalk
    column_mapping = {}
    mapped_pcds = set()
    mapped_aws = set()

    for _, row in table_crosswalk.iterrows():
        if row.get('is_tokenized'):
            continue  # Skip tokenized columns

        pcds_col = str(row.get('pcds_col', '')).strip().upper()
        aws_col = str(row.get('aws_col', '')).strip().lower()

        # Only map if both columns exist in their respective comparable lists
        if pcds_col in pcds_set and aws_col in aws_set and pcds_col != 'NAN' and aws_col != 'nan':
            column_mapping[pcds_col] = aws_col
            mapped_pcds.add(pcds_col)
            mapped_aws.add(aws_col)

    # Find unmapped columns
    unmapped_pcds = sorted(list(pcds_set - mapped_pcds))
    unmapped_aws = sorted(list(aws_set - mapped_aws))

    return {
        'column_mapping': column_mapping,
        'unmapped_pcds': unmapped_pcds,
        'unmapped_aws': unmapped_aws
    }

#>>> Build consolidated metadata for next step <<<#
def build_consolidated_metadata(pcds_results, aws_results, tables_df, crosswalk_df):
    validated_tables = []
    excluded_tables = []

    tables_map = {row['pcds_tbl']: row for _, row in tables_df.iterrows()}

    for pcds, aws in zip(pcds_results, aws_results):
        pcds_table = pcds['table']
        aws_table = aws['table']

        table_info = tables_map.get(pcds_table, {})

        if not pcds['accessible'] or not aws['accessible']:
            excluded_tables.append(pcds_table)
            continue

        pcds_vintages = {v['vintage']: v for v in pcds.get('vintages', [])}
        aws_vintages = {v['vintage']: v for v in aws.get('vintages', [])}

        pcds_counts = {r['date_std']: r['cnt'] for r in pcds.get('row_counts', []) if r.get('date_std')}
        aws_counts = {r['date_std']: r['cnt'] for r in aws.get('row_counts', []) if r.get('date_std')}

        validated_vintages = []
        for vintage_key in set(pcds_vintages.keys()) & set(aws_vintages.keys()):
            pcds_v = pcds_vintages[vintage_key]
            aws_v = aws_vintages[vintage_key]

            vintage_dates = set()
            if pcds_v.get('start_date') and pcds_v.get('end_date'):
                vintage_dates = {d for d in pcds_counts.keys() if pcds_v['start_date'] <= d <= pcds_v['end_date']}

            row_counts_match = all(
                pcds_counts.get(d) == aws_counts.get(d) for d in vintage_dates
            ) if vintage_dates else True

            if row_counts_match:
                validated_vintages.append({
                    'vintage': vintage_key,
                    'start_date': pcds_v.get('start_date'),
                    'end_date': pcds_v.get('end_date'),
                    'pcds_where_clause': pcds_v.get('where_clause'),
                    'aws_where_clause': aws_v.get('where_clause')
                })

        if not validated_vintages:
            excluded_tables.append(pcds_table)
            continue

        pcds_crosswalk = pcds.get('crosswalk', {})
        aws_crosswalk = aws.get('crosswalk', {})

        comparable_pcds = pcds_crosswalk.get('comparable', [])
        comparable_aws = aws_crosswalk.get('comparable', [])

        # Build proper column mapping using crosswalk
        col_map_name = table_info.get('col_map', pcds_table.split('.')[-1])
        mapping_result = build_column_mapping(crosswalk_df, col_map_name, comparable_pcds, comparable_aws)
        column_mapping = mapping_result['column_mapping']

        pcds_column_types = pcds.get('column_types', {})
        aws_column_types = aws.get('column_types', {})

        # Build column types dict for mapped columns only (using PCDS column names as keys)
        pcds_types_for_comparable = {pcds_col: pcds_column_types.get(pcds_col)
                                      for pcds_col in column_mapping.keys()}
        aws_types_for_comparable = {pcds_col: aws_column_types.get(aws_col)
                                     for pcds_col, aws_col in column_mapping.items()}

        validated_tables.append({
            'pcds_table': pcds_table,
            'aws_table': aws_table,
            'pcds_svc': table_info.get('pcds_svc'),
            'aws_database': aws_table.split('.')[0] if '.' in aws_table else None,
            'pcds_date_var': table_info.get('pcds_var'),
            'aws_date_var': table_info.get('aws_var'),
            'pcds_where': table_info.get('pcds_where'),
            'aws_where': table_info.get('aws_where'),
            'partition_type': table_info.get('partition', 'month'),
            'column_mapping': column_mapping,  # Dict: {PCDS_COL: aws_col}
            'unmapped_pcds_columns': mapping_result['unmapped_pcds'],  # PCDS columns without AWS match
            'unmapped_aws_columns': mapping_result['unmapped_aws'],    # AWS columns without PCDS match
            'pcds_column_types': pcds_types_for_comparable,  # Types indexed by PCDS column
            'aws_column_types': aws_types_for_comparable,     # Types indexed by PCDS column
            'validated_vintages': validated_vintages
        })

    return {
        'pcds_results': pcds_results,
        'aws_results': aws_results,
        'validated_tables': validated_tables,
        'excluded_tables': excluded_tables
    }

#>>> Main execution <<<#
def main():
    env = load_env('input_pcds')
    run_name = env['RUN_NAME']
    category = env['CATEGORY']
    s3_bucket = env.get('S3_BUCKET', os.environ.get('S3_BUCKET'))

    output_folder = f'output/{run_name}'
    add_logger(output_folder)
    logger.info(f"Starting comparison report: {run_name} / {category}")

    s3 = S3Manager(s3_bucket, run_name)

    logger.info("Downloading PCDS results from S3")
    pcds_results = s3.download_json('meta_check', f'pcds_{category}_meta.json')

    logger.info("Downloading AWS results from S3")
    aws_results = s3.download_json('meta_check', f'aws_{category}_meta.json')

    logger.info("Downloading config from S3")
    tables_df = s3.download_csv('', 'input_tables.csv')
    crosswalk_df = s3.download_csv('', 'crosswalk.csv')

    consolidated = build_consolidated_metadata(pcds_results, aws_results, tables_df, crosswalk_df)
    logger.info(f"Validated {len(consolidated['validated_tables'])} tables, excluded {len(consolidated['excluded_tables'])} tables")

    s3.upload_json(consolidated, '', f'{category}_meta_check.json')
    logger.info(f"Uploaded consolidated meta_check.json to S3")

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = os.path.join(output_folder, f'meta_check_comparison_{category}_{timestamp}.xlsx')

    logger.info(f"Generating Excel report: {report_path}")

    with ExcelReporter(report_path) as reporter:
        summary_rows = []
        for pcds, aws in zip(pcds_results, aws_results):
            pcds_cw = pcds.get('crosswalk', {})
            aws_cw = aws.get('crosswalk', {})
            pcds_cnt = sum(r['cnt'] for r in pcds.get('row_counts', []) if r.get('cnt'))
            aws_cnt = sum(r['cnt'] for r in aws.get('row_counts', []) if r.get('cnt'))

            summary_rows.append([
                pcds['table'],
                '✓' if pcds['accessible'] else '✗',
                '✓' if aws['accessible'] else '✗',
                pcds_cnt,
                aws_cnt,
                len(pcds_cw.get('comparable', [])),
                len(aws_cw.get('comparable', [])),
                len(pcds.get('vintages', [])),
                len(aws.get('vintages', [])),
                '✓' if pcds['accessible'] == aws['accessible'] else '✗'
            ])

        reporter.create_summary_sheet(
            title='Meta Check Comparison Summary',
            headers=['Table', 'PCDS Access', 'AWS Access', 'PCDS Rows', 'AWS Rows',
                    'PCDS Comparable', 'AWS Comparable', 'PCDS Vintages', 'AWS Vintages', 'Match'],
            data_rows=summary_rows
        )

        for pcds, aws in zip(pcds_results, aws_results):
            sections = prepare_table_sections(pcds, aws)
            reporter.create_detail_sheet(pcds['table'].split('.')[-1], sections)

    logger.info(f"Report saved to {report_path}")

    return report_path

if __name__ == '__main__':
    main()
