"""Part 3: Hash Check Compare - Download PCDS and AWS hashes, compare, generate Excel report."""
if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv('input_pcds')

import os
from upath import UPath
import pandas as pd
from loguru import logger

import constant
import utils_config as C
from utils_s3 import S3Manager
from utils_xlsx import ExcelReporter

#>>> Compare hashes for a single vintage <<<#
def compare_vintage_hashes(pcds_hashes, aws_hashes, key_columns):
    if not pcds_hashes or not aws_hashes:
        return None

    pcds_df = pd.DataFrame(pcds_hashes['hashes'])
    aws_df = pd.DataFrame(aws_hashes['hashes'])

    if pcds_df.empty or aws_df.empty:
        return {
            'pcds_rows': len(pcds_df),
            'aws_rows': len(aws_df),
            'matched_rows': 0,
            'pcds_only_rows': len(pcds_df),
            'aws_only_rows': len(aws_df),
            'hash_mismatch_rows': 0,
            'sample_mismatches': []
        }

    merged = pd.merge(
        pcds_df, aws_df,
        on=key_columns,
        suffixes=('_pcds', '_aws'),
        how='outer',
        indicator=True
    )

    both = merged[merged['_merge'] == 'both']
    pcds_only = merged[merged['_merge'] == 'left_only']
    aws_only = merged[merged['_merge'] == 'right_only']

    hash_mismatches = both[both['hash_value_pcds'] != both['hash_value_aws']]

    sample_mismatches = []
    for _, row in hash_mismatches.head(100).iterrows():
        sample_mismatches.append({
            **{k: row[k] for k in key_columns},
            'pcds_hash': row['hash_value_pcds'],
            'aws_hash': row['hash_value_aws']
        })

    return {
        'pcds_rows': len(pcds_df),
        'aws_rows': len(aws_df),
        'matched_rows': len(both) - len(hash_mismatches),
        'pcds_only_rows': len(pcds_only),
        'aws_only_rows': len(aws_only),
        'hash_mismatch_rows': len(hash_mismatches),
        'sample_mismatches': sample_mismatches
    }

#>>> Prepare table detail sections for Excel <<<#
def prepare_table_sections(pcds_result, aws_result):
    sections = []

    sections.append({
        'title': 'Table Information',
        'rows': [
            ['PCDS Table:', pcds_result['table']],
            ['AWS Table:', aws_result['table']],
            ['Clean Columns:', len(pcds_result['clean_columns'])],
            ['Key Columns:', ', '.join(pcds_result['key_columns'])],
            ['Mismatched Columns (excluded):', ', '.join(pcds_result['mismatched_columns'])],
            ['Vintages:', len(pcds_result['vintage_hashes'])]
        ]
    })

    key_columns = pcds_result['key_columns']

    for pcds_v, aws_v in zip(pcds_result['vintage_hashes'], aws_result['vintage_hashes']):
        comparison = compare_vintage_hashes(pcds_v['hash_data'], aws_v['hash_data'], key_columns)

        if comparison:
            sections.append({
                'title': f"Vintage {pcds_v['vintage']} ({pcds_v['start_date']} to {pcds_v['end_date']})",
                'rows': [
                    ['PCDS Rows:', comparison['pcds_rows']],
                    ['AWS Rows:', comparison['aws_rows']],
                    ['Matched Hashes:', comparison['matched_rows']],
                    ['PCDS Only:', comparison['pcds_only_rows']],
                    ['AWS Only:', comparison['aws_only_rows']],
                    ['Hash Mismatches:', comparison['hash_mismatch_rows']],
                    ['Match Status:', '✓' if comparison['hash_mismatch_rows'] == 0 else '✗']
                ]
            })

            if comparison['sample_mismatches']:
                mismatch_df = pd.DataFrame(comparison['sample_mismatches'])
                sections.append({
                    'title': f"Sample Mismatches (first {len(comparison['sample_mismatches'])})",
                    'dataframe': mismatch_df
                })

    return sections

#>>> Build consolidated hash check metadata <<<#
def build_consolidated_hash_metadata(pcds_results, aws_results):
    return {
        'pcds_results': pcds_results,
        'aws_results': aws_results,
        'comparison_summary': [
            {
                'table': pcds['table'],
                'total_vintages': len(pcds['vintage_hashes']),
                'clean_columns': len(pcds['clean_columns']),
                'key_columns': pcds['key_columns']
            }
            for pcds in pcds_results
        ]
    }

#>>> Main execution <<<#
def main():
    run_name, category, config_path = C.get_env('RUN_NAME', 'CATEGORY', 'HASH_STEP')

    cfg = C.load_config(config_path)
    step_name = cfg.output.summary.format(s='hash')
    output_folder = cfg.output.disk.format(name=run_name)
    C.add_logger(output_folder, name=step_name)
    logger.info(f"Starting hash check comparison: {run_name} | {category}")

    s3_bucket = cfg.output.s3.format(name=run_name)
    s3 = S3Manager(s3_bucket)

    logger.info("Downloading PCDS hashes from S3")
    pcds_results = s3.read_json(f"{cfg.output.step_name.format(p='pcds')}.json")

    logger.info("Downloading AWS hashes from S3")
    aws_results = s3.read_json(f"{cfg.output.step_name.format(p='aws')}.json")

    consolidated = build_consolidated_hash_metadata(pcds_results, aws_results)

    local_path = os.path.join(output_folder, f'{step_name}.json')
    s3.write_json(consolidated, UPath(local_path))
    s3.write_json(consolidated, f'{step_name}.json')
    logger.info(f"Uploaded consolidated hash check to S3")

    report_path = os.path.join(output_folder, f'{step_name}.xlsx')
    logger.info(f"Generating Excel report: {report_path}")

    with ExcelReporter(report_path) as reporter:
        summary_rows = []
        for pcds, aws in zip(pcds_results, aws_results):
            total_vintages = len(pcds['vintage_hashes'])
            key_columns = pcds['key_columns']

            total_mismatches = 0
            for pcds_v, aws_v in zip(pcds['vintage_hashes'], aws['vintage_hashes']):
                comp = compare_vintage_hashes(pcds_v['hash_data'], aws_v['hash_data'], key_columns)
                if comp:
                    total_mismatches += comp['hash_mismatch_rows']

            summary_rows.append([
                pcds['table'],
                len(pcds['clean_columns']),
                ', '.join(key_columns),
                total_vintages,
                total_mismatches,
                '✓' if total_mismatches == 0 else '✗'
            ])

        reporter.create_summary_sheet(
            title='Hash Check Comparison Summary',
            headers=['Table', 'Clean Columns', 'Key Columns', 'Vintages', 'Mismatches', 'Match'],
            data_rows=summary_rows
        )

        for pcds, aws in zip(pcds_results, aws_results):
            sections = prepare_table_sections(pcds, aws)
            reporter.create_detail_sheet(pcds['table'].split('.')[-1], sections)

    logger.info(f"Report saved to {report_path}")

    return report_path

if __name__ == '__main__':
    main()
