"""Part 3: Compare Report - Download PCDS and AWS results from S3, compare them, and generate Excel report."""
if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv('input_pcds')

import os
import re
import pandas as pd
from upath import UPath
from loguru import logger

import constant
import utils_config as C
from utils_s3 import S3Manager
from utils_xlsx import ExcelReporter

def prepare_table_sections_from_consolidated(table_meta, pcds_accessible, aws_accessible, pcds_crosswalk, aws_crosswalk):

    sections = []

    # Overview (two rows, multiple columns)
    overview_columns = [[
        'Table', 'Accessible', 'Total Days Extracted',
        'Matched Day Count', 'Row Match (All)',
        'Comparable Columns', 'Tokenized Columns', 'Only Columns', 'Unmapped Columns'
    ]]
    row_match_all = '✓' if table_meta['row_match_all'] else '✗'
    total_union = table_meta['total_days_union']

    sections.append({
        'title': 'Table Information',
        'columns': overview_columns,
        'rows': [
            [
                table_meta['pcds_table'],
                ('✓' if pcds_accessible else '✗'),
                total_union,                     # union of days considered
                table_meta['matched_day_count'],
                row_match_all,
                len(pcds_crosswalk['comparable']),
                len(pcds_crosswalk['tokenized']),
                len(pcds_crosswalk['pcds_only']),
                len(pcds_crosswalk['unmapped'])
            ],
            [
                table_meta['aws_table'],
                ('✓' if aws_accessible else '✗'),
                total_union,
                table_meta['matched_day_count'],
                row_match_all,
                len(aws_crosswalk['comparable']),
                len(aws_crosswalk['tokenized']),
                len(aws_crosswalk['aws_only']),
                len(aws_crosswalk['unmapped'])
            ],
        ]
    })

    # Summary block (out of total counts (days), how many match)
    sections.append({
        'title': 'Matching Days Summary',
        'rows': [
            ['Total Distinct Days (PCDS ∪ AWS):', total_union],
            ['Matched Day Count:', table_meta['matched_day_count']],
            ['Unmatched Day Count:', total_union - table_meta['matched_day_count']]
        ]
    })

    # Only-columns lists
    if len(pcds_crosswalk['pcds_only']) > 0:
        sections.append({
            'title': f"PCDS Only Columns ({len(pcds_crosswalk['pcds_only'])})",
            'rows': [[c] for c in pcds_crosswalk['pcds_only']]
        })

    if len(aws_crosswalk['aws_only']) > 0:
        sections.append({
            'title': f"AWS Only Columns ({len(aws_crosswalk['aws_only'])})",
            'rows': [[c] for c in aws_crosswalk['aws_only']]
        })

    # Date mismatches (4 columns)
    mismatches = table_meta['mismatch_details']
    if len(mismatches) > 0:
        df = pd.DataFrame(
            [{'Date': m['date'], 'PCDS Count': m['pcds_count'], 'AWS Count': m['aws_count'], 'Match': '✗'}
             for m in mismatches],
            columns=['Date', 'PCDS Count', 'AWS Count', 'Match']
        )
        sections.append({
            'title': f"Dates with Count Mismatch ({len(mismatches)})",
            'dataframe': df
        })

    return sections


def sql_literal(val):
    if val is None:
        return "NULL"
    if isinstance(val, (int, float)):
        return str(val)
    elif re.search(r'^(?:date|time)', val, re.I):
        return val
    return val

def build_where(vintage, where_clause, date_var, excl_values):
    base = vintage['where_clause']
    return (
        f"({base})"
        + (f" AND ({where_clause})" if where_clause else "")
        + (
            f" AND {date_var} NOT IN ({', '.join(sql_literal(v) for v in excl_values)})"
            if excl_values else
            ""
        )
    )

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
    validated_tables, excluded_tables = [], []

    tables_map = {row['pcds_tbl']: row for _, row in tables_df.iterrows()}

    for pcds, aws in zip(pcds_results, aws_results):
        key = f"{pcds['service']}.{pcds['table']}".lower()
        table_info = tables_map[key]

        # Access control
        if (not pcds['accessible']) or (not aws['accessible']):
            logger.warning("Table Not Accessible")
            excluded_tables.append(pcds['table'])
            continue

        # Parsers
        pcds_parser = C.DateParser.from_json(pcds['date_var'])
        aws_parser  = C.DateParser.from_json(aws['date_var'])

        # Counts maps (strict)
        pcds_var = table_info['pcds_var']
        aws_var  = table_info['aws_var']
        pcds_counts = {r[pcds_var]: r['CNT'] for r in pcds['row_counts']}
        aws_counts  = {r[aws_var] : r['cnt'] for r in aws['row_counts']}

        all_dates = sorted(set(pcds_counts.keys()) | set(aws_counts.keys()))
        mismatch_details = [
            {'date': d, 'pcds_count': pcds_val, 'aws_count': aws_val}
            for d in all_dates
            if (pcds_val := pcds_counts.get(d, 0)) != (aws_val := aws_counts.get(d, 0))
        ]
        matched_day_count = len(all_dates) - len(mismatch_details)
        mismatch_dates_set = {m['date'] for m in mismatch_details}

        # Vintages with NOT IN exclusion for only mismatches in-window
        pcds_vintages = {v['vintage']: v for v in pcds['vintages']}
        aws_vintages  = {v['vintage']: v for v in aws['vintages']}
        pcds_where, aws_where = pcds['where_clause'], aws['where_clause']

        validated_vintages = []
        for vk in (set(pcds_vintages.keys()) & set(aws_vintages.keys())):
            p_v = pcds_vintages[vk]
            a_v = aws_vintages[vk]

            window_dates = [d for d in all_dates if p_v['start_date'] and p_v['end_date'] and p_v['start_date'] <= d <= p_v['end_date']]
            mismatched_in_window = [d for d in window_dates if d in mismatch_dates_set]

            pcds_excl_vals = [pcds_parser.to_original(d) for d in mismatched_in_window]
            aws_excl_vals  = [aws_parser.to_original(d)  for d in mismatched_in_window]

            p_where = build_where(p_v, pcds_where, pcds_var, pcds_excl_vals)
            a_where = build_where(a_v, aws_where, aws_var, aws_excl_vals)

            validated_vintages.append({
                'vintage': vk,
                'start_date': p_v['start_date'],
                'end_date': p_v['end_date'],
                'pcds_where_clause': p_where,
                'aws_where_clause': a_where,
                'excluded_dates_count': len(mismatched_in_window)
            })

        if len(validated_vintages) == 0:
            logger.warning('No Single Overlapped Vintage, Exclude this table')
            excluded_tables.append(pcds['table'])
            continue

        pcds_crosswalk = pcds.get('crosswalk', {})
        aws_crosswalk = aws.get('crosswalk', {})

        comparable_pcds = pcds_crosswalk.get('comparable', [])
        comparable_aws = aws_crosswalk.get('comparable', [])

        # Build proper column mapping using crosswalk
        col_map_name = table_info.get('col_map', pcds['table'])
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
            'pcds_table':  pcds['table'],
            'aws_table': aws['table'],
            'aws_database': aws['database'],
            'pcds_svc': table_info.pcds_svc,
            'pcds_date_var': table_info.pcds_var,
            'aws_date_var': table_info.aws_var,
            'pcds_where': table_info.pcds_where,
            'aws_where':  table_info.aws_where,
            'partition_type':  table_info.partition,
            'column_mapping': column_mapping, 
            'unmapped_pcds_columns': mapping_result['unmapped_pcds'], 
            'unmapped_aws_columns': mapping_result['unmapped_aws'],   
            'pcds_column_types': pcds_types_for_comparable,
            'aws_column_types': aws_types_for_comparable,  
            'validated_vintages': validated_vintages,
            'total_days_union': len(all_dates),
            'matched_day_count': matched_day_count,
            'row_match_all': (matched_day_count == len(all_dates) and len(all_dates) > 0),
            'mismatch_details': mismatch_details
        })

    return {
        'pcds_results': pcds_results,
        'aws_results': aws_results,
        'validated_tables': validated_tables,
        'excluded_tables': excluded_tables
    }


def main():
    run_name, category, config_path = C.get_env('RUN_NAME', 'CATEGORY', 'META_STEP')

    cfg = C.load_config(config_path)
    step_name = cfg.output.summary.format(s='meta')
    output_folder = cfg.output.disk.format(name=run_name)
    C.add_logger(output_folder, name=step_name)
    logger.info(f"Starting comparison report: {run_name} | {category}")

    s3_bucket = cfg.output.s3.format(name=run_name)
    s3 = S3Manager(s3_bucket)

    logger.info("Downloading PCDS results from S3")
    pcds_results = s3.read_json(f"{cfg.output.step_name.format(p='pcds')}.json")

    logger.info("Downloading AWS results from S3")
    aws_results = s3.read_json(f"{cfg.output.step_name.format(p='aws')}.json")

    logger.info("Downloading input tables from S3")
    tables_df = s3.read_df('input_tables.csv')

    logger.info("Downloading crosswalk tables from S3")
    crosswalk_df = s3.read_df('crosswalk.csv')

    consolidated = build_consolidated_metadata(pcds_results, aws_results, tables_df, crosswalk_df)
    logger.info(f"Validated {len(consolidated['validated_tables'])} tables, excluded {len(consolidated['excluded_tables'])} tables")

    local_path = os.path.join(output_folder, f'{step_name}.json')
    s3.write_json(consolidated, UPath(local_path))
    s3.write_json(consolidated, f'{step_name}.json')

    report_path = os.path.join(output_folder, f'{step_name}.xlsx')
    with ExcelReporter(report_path) as reporter:
        # Summary sheet: "out of total counts (days), how many match"
        summary_rows = [
            [
                t['pcds_table'],
                '✓',  # accessible already enforced; if you want exact flags, pass them in
                '✓',
                t['total_days_union'],
                t['matched_day_count'],
                '✓' if t['row_match_all'] else '✗'
            ]
            for t in consolidated['validated_tables']
        ]

        reporter.create_summary_sheet(
            title='Meta Check Comparison Summary',
            headers=['Table', 'PCDS Access', 'AWS Access',
                     'Total Distinct Days', 'Matched Day Count', 'All Days Match'],
            data_rows=summary_rows
        )

        # Detail sheets from consolidated
        for t, pcds, aws in zip(consolidated['validated_tables'], pcds_results, aws_results):
            sections = prepare_table_sections_from_consolidated(
                table_meta=t,
                pcds_accessible=pcds['accessible'],
                aws_accessible=aws['accessible'],
                pcds_crosswalk=pcds['crosswalk'],
                aws_crosswalk=aws['crosswalk']
            )
            reporter.create_detail_sheet(pcds['table'], sections)

    logger.info(f"Report saved to {report_path}")
    return report_path


if __name__ == '__main__':
    main()