# Hash Check - MD5 Hash Validation for Data Integrity
# Computes and compares MD5 hashes between PCDS and AWS data at multiple granularities

import os
import sys
import hashlib
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import numpy as np
from loguru import logger
from tqdm import tqdm
from dotenv import load_dotenv

# Add paths for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'common'))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

from config_loader import query_pcds, query_aws
from s3_utils import create_s3_manager
from excel_reporter import ExcelReporter


#>>> Hash Check class - MD5 hash validation <<<#
class HashChecker:

    #>>> Initialize - load environment from checks/input_pcds <<<#
    def __init__(self):
        # Load environment from checks/input_pcds
        env_path = Path(__file__).parent.parent.parent / 'checks' / 'input_pcds'
        load_dotenv(env_path)

        self.run_name = os.getenv('RUN_NAME', 'demo')
        self.category = os.getenv('CATEGORY', 'dpst')
        self.s3_bucket = os.getenv('S3_BUCKET')

        if not self.s3_bucket:
            raise ValueError("S3_BUCKET not found in environment")

        self.s3 = create_s3_manager(self.run_name)
        self.results = {}

        # Parallel execution limits
        self.pcds_parallel = 3  # Max concurrent Oracle queries
        self.aws_parallel = 5   # Max concurrent Athena queries

        logger.info(f"HashChecker initialized: run_name={self.run_name}, category={self.category}")

    #>>> Build WHERE clause for date range filtering - PCDS <<<#
    def build_date_filter_pcds(self, date_var: str, date_type: str,
                              date_format: Optional[str], start_date: str,
                              end_date: str) -> str:
        if not date_var or not date_type:
            return '1=1'

        # Convert dates based on type
        if date_type in ['DATE', 'TIMESTAMP']:
            # Use DATE literals with range query
            return f"{date_var} >= DATE '{start_date}' AND {date_var} < DATE '{end_date}'"

        elif date_type == 'STRING':
            if date_format == '%Y%m%d':
                # Convert to YYYYMMDD format
                start_str = start_date.replace('-', '')
                end_str = end_date.replace('-', '')
                return f"{date_var} >= '{start_str}' AND {date_var} < '{end_str}'"
            else:
                # Use as-is
                return f"{date_var} >= '{start_date}' AND {date_var} < '{end_date}'"

        return '1=1'

    #>>> Build WHERE clause for date range filtering - AWS <<<#
    def build_date_filter_aws(self, date_var: str, date_type: str,
                             date_format: Optional[str], start_date: str,
                             end_date: str) -> str:
        # Same logic as PCDS
        return self.build_date_filter_pcds(date_var, date_type, date_format,
                                          start_date, end_date)

    #>>> Build exclude clause for unequal dates - PCDS <<<#
    def build_exclude_clause_pcds(self, unequal_dates: List[str], date_var: str,
                                  date_type: str, date_format: Optional[str]) -> str:
        if not unequal_dates or not date_var or not date_type:
            return '1=1'

        # Convert dates to appropriate format based on date type
        if date_type in ['DATE', 'TIMESTAMP']:
            # Use DATE literals
            date_list = [f"DATE '{date}'" for date in unequal_dates]
            return f"{date_var} NOT IN ({', '.join(date_list)})"

        elif date_type == 'STRING':
            if date_format == '%Y%m%d':
                # Convert to YYYYMMDD format
                date_list = [f"'{date.replace('-', '')}'" for date in unequal_dates]
            else:
                # Use as-is
                date_list = [f"'{date}'" for date in unequal_dates]
            return f"{date_var} NOT IN ({', '.join(date_list)})"

        return '1=1'

    #>>> Build exclude clause for unequal dates - AWS <<<#
    def build_exclude_clause_aws(self, unequal_dates: List[str], date_var: str,
                                date_type: str, date_format: Optional[str]) -> str:
        # Same logic as PCDS
        return self.build_exclude_clause_pcds(unequal_dates, date_var, date_type, date_format)

    #>>> Generate PCDS query to extract data for hashing <<<#
    def generate_pcds_extract_query(self, table: str, columns: List[str],
                                    where_clause: str, exclude_clause: str,
                                    order_by: List[str]) -> str:
        # Build column list with CAST to VARCHAR for consistent hashing
        col_expressions = []
        for col in columns:
            col_expressions.append(f"NVL(CAST({col} AS VARCHAR2(4000)), 'NULL') AS {col}")

        col_list = ', '.join(col_expressions)
        order_list = ', '.join(order_by)

        return f"""
SELECT {col_list}
FROM {table}
WHERE {where_clause} AND {exclude_clause}
ORDER BY {order_list}
        """.strip()

    #>>> Generate AWS query to extract data for hashing <<<#
    def generate_aws_extract_query(self, table: str, columns: List[str],
                                   where_clause: str, exclude_clause: str,
                                   order_by: List[str]) -> str:
        database, table_name = table.split('.', 1)

        # Build column list with CAST to VARCHAR for consistent hashing
        col_expressions = []
        for col in columns:
            col_expressions.append(f"COALESCE(CAST({col} AS VARCHAR), 'NULL') AS {col}")

        col_list = ', '.join(col_expressions)
        order_list = ', '.join(order_by)

        return f"""
SELECT {col_list}
FROM {database}.{table_name}
WHERE {where_clause} AND {exclude_clause}
ORDER BY {order_list}
        """.strip()

    #>>> Compute MD5 hash from DataFrame <<<#
    @staticmethod
    def compute_hash(df: pd.DataFrame, columns: List[str]) -> str:
        if df.empty:
            return hashlib.md5(b'').hexdigest()

        # Ensure consistent column order
        df_sorted = df[columns].copy()

        # Convert all values to strings and handle NaN
        df_sorted = df_sorted.fillna('NULL').astype(str)

        # Create concatenated string for each row
        row_strings = df_sorted.apply(lambda row: '|'.join(row.values), axis=1)

        # Concatenate all rows and compute hash
        data_string = '\n'.join(row_strings.values)
        hash_value = hashlib.md5(data_string.encode('utf-8')).hexdigest()

        logger.debug(f"Computed hash for {len(df)} rows, {len(columns)} columns: {hash_value}")
        return hash_value

    #>>> Execute PCDS extraction query <<<#
    def execute_pcds_extract(self, query: str, service: str, description: str) -> pd.DataFrame:
        try:
            logger.debug(f"Executing PCDS extraction: {description}")
            result = query_pcds(query, service)
            logger.debug(f"  Retrieved {len(result)} rows")
            return result
        except Exception as e:
            logger.error(f"PCDS extraction failed for {description}: {e}")
            return pd.DataFrame()

    #>>> Execute AWS extraction query <<<#
    def execute_aws_extract(self, query: str, database: str, description: str) -> pd.DataFrame:
        try:
            logger.debug(f"Executing AWS extraction: {description}")
            result = query_aws(query, database)
            logger.debug(f"  Retrieved {len(result)} rows")
            return result
        except Exception as e:
            logger.error(f"AWS extraction failed for {description}: {e}")
            return pd.DataFrame()

    #>>> Compute overall table hash <<<#
    def compute_table_hash(self, table_name: str, meta_results: Dict) -> Dict:
        logger.info(f"  Computing overall table hash for {table_name}...")

        result = {
            'granularity': 'table',
            'pcds_hash': None,
            'aws_hash': None,
            'match': False,
            'pcds_row_count': 0,
            'aws_row_count': 0
        }

        # Get comparable columns
        column_mapping = meta_results.get('column_mapping', {})
        comparable_cols = column_mapping.get('comparable', {})

        if not comparable_cols:
            logger.warning("    No comparable columns found")
            return result

        # Get date info
        date_var = meta_results.get('date_var', '')
        date_type = meta_results.get('date_type')
        date_format = meta_results.get('date_format')

        # Build exclude clause for mismatched dates
        mismatched_dates = [d['date'] for d in meta_results.get('row_counts', {}).get('mismatched_dates', [])]
        pcds_exclude = self.build_exclude_clause_pcds(mismatched_dates, date_var, date_type, date_format)
        aws_exclude = self.build_exclude_clause_aws(mismatched_dates, date_var, date_type, date_format)

        # Get table info
        service_name = table_name.split('.', 1)[0]
        table_only = table_name.split('.', 1)[1]
        pcds_where = meta_results.get('pcds_where', '1=1')

        aws_table = meta_results.get('aws_table', '')
        database = aws_table.split('.', 1)[0] if aws_table else ''
        aws_where = meta_results.get('aws_where', '1=1')

        # Get PCDS columns and AWS columns (sorted)
        pcds_cols = sorted(comparable_cols.keys())
        aws_cols = [comparable_cols[col] for col in pcds_cols]

        # Determine order by columns (use date_var if available, else first column)
        if date_var and date_var in pcds_cols:
            order_by_pcds = [date_var] + [col for col in pcds_cols if col != date_var]
            order_by_aws = [comparable_cols[date_var]] + [col for col in aws_cols if col != comparable_cols[date_var]]
        else:
            order_by_pcds = pcds_cols
            order_by_aws = aws_cols

        # Generate queries
        pcds_query = self.generate_pcds_extract_query(
            table_only.upper(), pcds_cols, pcds_where, pcds_exclude, order_by_pcds
        )
        aws_query = self.generate_aws_extract_query(
            aws_table, aws_cols, aws_where, aws_exclude, order_by_aws
        )

        # Execute queries
        pcds_df = self.execute_pcds_extract(pcds_query, service_name, f"{table_name} PCDS")
        aws_df = self.execute_aws_extract(aws_query, database, f"{table_name} AWS")

        # Compute hashes
        if not pcds_df.empty:
            result['pcds_hash'] = self.compute_hash(pcds_df, pcds_cols)
            result['pcds_row_count'] = len(pcds_df)

        if not aws_df.empty:
            result['aws_hash'] = self.compute_hash(aws_df, aws_cols)
            result['aws_row_count'] = len(aws_df)

        # Compare
        result['match'] = (result['pcds_hash'] == result['aws_hash'] and
                          result['pcds_hash'] is not None)

        logger.info(f"    Table hash: {'MATCH' if result['match'] else 'MISMATCH'}")
        logger.info(f"    PCDS: {result['pcds_hash']} ({result['pcds_row_count']} rows)")
        logger.info(f"    AWS:  {result['aws_hash']} ({result['aws_row_count']} rows)")

        return result

    #>>> Compute per-vintage hashes <<<#
    def compute_vintage_hashes(self, table_name: str, meta_results: Dict) -> List[Dict]:
        logger.info(f"  Computing per-vintage hashes for {table_name}...")

        results = []

        # Get vintages from meta results
        vintages = meta_results.get('vintages', [])
        if not vintages:
            logger.warning("    No vintages found in meta results")
            return results

        # Get comparable columns
        column_mapping = meta_results.get('column_mapping', {})
        comparable_cols = column_mapping.get('comparable', {})

        if not comparable_cols:
            logger.warning("    No comparable columns found")
            return results

        # Get date info
        date_var = meta_results.get('date_var', '')
        date_type = meta_results.get('date_type')
        date_format = meta_results.get('date_format')

        # Get table info
        service_name = table_name.split('.', 1)[0]
        table_only = table_name.split('.', 1)[1]
        pcds_where = meta_results.get('pcds_where', '1=1')

        aws_table = meta_results.get('aws_table', '')
        database = aws_table.split('.', 1)[0] if aws_table else ''
        aws_where = meta_results.get('aws_where', '1=1')

        # Get PCDS columns and AWS columns (sorted)
        pcds_cols = sorted(comparable_cols.keys())
        aws_cols = [comparable_cols[col] for col in pcds_cols]

        # Determine order by columns
        if date_var and date_var in pcds_cols:
            order_by_pcds = [date_var] + [col for col in pcds_cols if col != date_var]
            order_by_aws = [comparable_cols[date_var]] + [col for col in aws_cols if col != comparable_cols[date_var]]
        else:
            order_by_pcds = pcds_cols
            order_by_aws = aws_cols

        # Process each vintage
        logger.info(f"    Processing {len(vintages)} vintages...")
        for vintage_info in tqdm(vintages, desc="Vintage hashes"):
            vintage_name = vintage_info.get('vintage', 'unknown')
            start_date = vintage_info.get('start_date', '')
            end_date = vintage_info.get('end_date', '')

            if not start_date or not end_date:
                logger.warning(f"    Skipping vintage {vintage_name}: missing date range")
                continue

            vintage_result = {
                'granularity': 'vintage',
                'vintage': vintage_name,
                'start_date': start_date,
                'end_date': end_date,
                'pcds_hash': None,
                'aws_hash': None,
                'match': False,
                'pcds_row_count': 0,
                'aws_row_count': 0
            }

            # Build date filter for this vintage
            pcds_date_filter = self.build_date_filter_pcds(
                date_var, date_type, date_format, start_date, end_date
            )
            aws_date_filter = self.build_date_filter_aws(
                date_var, date_type, date_format, start_date, end_date
            )

            # Combine with base where clause
            pcds_combined_where = f"({pcds_where}) AND ({pcds_date_filter})"
            aws_combined_where = f"({aws_where}) AND ({aws_date_filter})"

            # Generate queries
            pcds_query = self.generate_pcds_extract_query(
                table_only.upper(), pcds_cols, pcds_combined_where, '1=1', order_by_pcds
            )
            aws_query = self.generate_aws_extract_query(
                aws_table, aws_cols, aws_combined_where, '1=1', order_by_aws
            )

            # Execute queries
            pcds_df = self.execute_pcds_extract(pcds_query, service_name, f"{vintage_name} PCDS")
            aws_df = self.execute_aws_extract(aws_query, database, f"{vintage_name} AWS")

            # Compute hashes
            if not pcds_df.empty:
                vintage_result['pcds_hash'] = self.compute_hash(pcds_df, pcds_cols)
                vintage_result['pcds_row_count'] = len(pcds_df)

            if not aws_df.empty:
                vintage_result['aws_hash'] = self.compute_hash(aws_df, aws_cols)
                vintage_result['aws_row_count'] = len(aws_df)

            # Compare
            vintage_result['match'] = (vintage_result['pcds_hash'] == vintage_result['aws_hash'] and
                                      vintage_result['pcds_hash'] is not None)

            results.append(vintage_result)

        # Log summary
        matched = len([r for r in results if r['match']])
        logger.info(f"    Vintage hashes: {matched}/{len(results)} matched")

        return results

    #>>> Compute per-column hashes <<<#
    def compute_column_hashes(self, table_name: str, meta_results: Dict) -> List[Dict]:
        logger.info(f"  Computing per-column hashes for {table_name}...")

        results = []

        # Get comparable columns
        column_mapping = meta_results.get('column_mapping', {})
        comparable_cols = column_mapping.get('comparable', {})

        if not comparable_cols:
            logger.warning("    No comparable columns found")
            return results

        # Get date info
        date_var = meta_results.get('date_var', '')
        date_type = meta_results.get('date_type')
        date_format = meta_results.get('date_format')

        # Build exclude clause for mismatched dates
        mismatched_dates = [d['date'] for d in meta_results.get('row_counts', {}).get('mismatched_dates', [])]
        pcds_exclude = self.build_exclude_clause_pcds(mismatched_dates, date_var, date_type, date_format)
        aws_exclude = self.build_exclude_clause_aws(mismatched_dates, date_var, date_type, date_format)

        # Get table info
        service_name = table_name.split('.', 1)[0]
        table_only = table_name.split('.', 1)[1]
        pcds_where = meta_results.get('pcds_where', '1=1')

        aws_table = meta_results.get('aws_table', '')
        database = aws_table.split('.', 1)[0] if aws_table else ''
        aws_where = meta_results.get('aws_where', '1=1')

        # Process each column
        logger.info(f"    Processing {len(comparable_cols)} columns...")
        for pcds_col, aws_col in tqdm(comparable_cols.items(), desc="Column hashes"):
            # Determine order by (use date_var if available, else the column itself)
            if date_var:
                order_by_pcds = [date_var, pcds_col] if date_var != pcds_col else [date_var]
                order_by_aws = [comparable_cols.get(date_var, aws_col), aws_col] if date_var != pcds_col else [aws_col]
            else:
                order_by_pcds = [pcds_col]
                order_by_aws = [aws_col]

            # Generate queries for single column
            pcds_query = self.generate_pcds_extract_query(
                table_only.upper(), [pcds_col], pcds_where, pcds_exclude, order_by_pcds
            )
            aws_query = self.generate_aws_extract_query(
                aws_table, [aws_col], aws_where, aws_exclude, order_by_aws
            )

            # Execute queries
            pcds_df = self.execute_pcds_extract(pcds_query, service_name, f"{pcds_col} PCDS")
            aws_df = self.execute_aws_extract(aws_query, database, f"{aws_col} AWS")

            col_result = {
                'granularity': 'column',
                'pcds_column': pcds_col,
                'aws_column': aws_col,
                'pcds_hash': None,
                'aws_hash': None,
                'match': False,
                'pcds_row_count': 0,
                'aws_row_count': 0
            }

            # Compute hashes
            if not pcds_df.empty:
                col_result['pcds_hash'] = self.compute_hash(pcds_df, [pcds_col])
                col_result['pcds_row_count'] = len(pcds_df)

            if not aws_df.empty:
                col_result['aws_hash'] = self.compute_hash(aws_df, [aws_col])
                col_result['aws_row_count'] = len(aws_df)

            # Compare
            col_result['match'] = (col_result['pcds_hash'] == col_result['aws_hash'] and
                                  col_result['pcds_hash'] is not None)

            results.append(col_result)

        # Log summary
        matched = len([r for r in results if r['match']])
        logger.info(f"    Column hashes: {matched}/{len(results)} matched")

        return results

    #>>> Load meta check results from S3 <<<#
    def load_meta_results(self, table_name: str) -> Optional[Dict]:
        try:
            filename = f"{table_name.replace('.', '_')}_meta.json"
            result = self.s3.download_json('meta_check', filename)
            return result
        except Exception as e:
            logger.error(f"Failed to load meta results for {table_name}: {e}")
            return None

    #>>> Process single table for hash validation <<<#
    def process_table(self, table_name: str) -> Dict:
        logger.info(f"Processing table: {table_name}")

        result = {
            'table_name': table_name,
            'status': 'pending',
            'table_hash': {},
            'vintage_hashes': [],
            'column_hashes': []
        }

        # 1. Load meta check results
        logger.info("  Loading meta check results from S3...")
        meta_results = self.load_meta_results(table_name)

        if not meta_results:
            result['status'] = 'no_meta_results'
            logger.warning(f"  No meta results found for {table_name}")
            return result

        if not meta_results.get('pcds_accessible') or not meta_results.get('aws_accessible'):
            result['status'] = 'not_accessible'
            logger.warning(f"  Table not accessible: {table_name}")
            return result

        # 2. Compute overall table hash
        result['table_hash'] = self.compute_table_hash(table_name, meta_results)

        # 3. Compute per-vintage hashes
        result['vintage_hashes'] = self.compute_vintage_hashes(table_name, meta_results)

        # 4. Compute per-column hashes
        result['column_hashes'] = self.compute_column_hashes(table_name, meta_results)

        # 5. Determine overall status
        table_match = result['table_hash'].get('match', False)
        vintage_matches = [v['match'] for v in result['vintage_hashes']]
        column_matches = [c['match'] for c in result['column_hashes']]

        all_vintage_match = all(vintage_matches) if vintage_matches else True
        all_column_match = all(column_matches) if column_matches else True

        if table_match and all_vintage_match and all_column_match:
            result['status'] = 'PASS'
        elif table_match:
            result['status'] = 'PARTIAL'
        else:
            result['status'] = 'FAIL'

        logger.info(f"  ✓ Completed: Status={result['status']}")

        # 6. Save results to S3
        self.s3.upload_json(result, 'hash_check', f"{table_name.replace('.', '_')}_hash_validation.json")

        return result

    #>>> Run hash check for all tables <<<#
    def run(self):
        logger.info(f"Starting Hash Check for run: {self.run_name}, category: {self.category}")

        # Get list of tables from meta check results
        try:
            meta_results = self.s3.download_json('meta_check', 'meta_check_results.json')
            tables = list(meta_results.keys())
            logger.info(f"Found {len(tables)} tables from meta check")
        except Exception as e:
            logger.error(f"Failed to load meta check results: {e}")
            return {}

        # Process each table
        for table_name in tables:
            table_result = self.process_table(table_name)
            self.results[table_name] = table_result

        # Generate Excel report
        logger.info("Generating Excel report...")
        self.generate_excel_report()

        logger.info("✓ Hash Check completed!")
        return self.results

    #>>> Generate Excel report for hash check results <<<#
    def generate_excel_report(self):
        output_path = f"output_{self.run_name}_hash_check.xlsx"

        with ExcelReporter(output_path) as reporter:
            # Prepare summary data
            summary_rows = []
            for table_name, result in self.results.items():
                if result['status'] not in ['no_meta_results', 'not_accessible']:
                    table_match = '✓' if result['table_hash'].get('match', False) else '✗'

                    vintage_matches = [v['match'] for v in result['vintage_hashes']]
                    vintage_summary = f"{sum(vintage_matches)}/{len(vintage_matches)}" if vintage_matches else 'N/A'

                    column_matches = [c['match'] for c in result['column_hashes']]
                    column_summary = f"{sum(column_matches)}/{len(column_matches)}" if column_matches else 'N/A'

                    summary_rows.append([
                        table_name.split('.')[-1],
                        result['status'],
                        table_match,
                        vintage_summary,
                        column_summary
                    ])

            reporter.create_summary_sheet(
                title='Hash Check Summary',
                headers=['Table', 'Status', 'Table Hash', 'Vintages', 'Columns'],
                data_rows=summary_rows
            )

            # Create detail sheets for each table
            for table_name, result in self.results.items():
                if result['status'] not in ['no_meta_results', 'not_accessible']:
                    sections = self._prepare_table_sections(table_name, result)
                    reporter.create_detail_sheet(table_name.split('.')[-1], sections)

        # Upload Excel to S3
        self.s3.upload_file(output_path, 'hash_check', output_path)
        logger.info(f"Excel report uploaded: {output_path}")

    #>>> Prepare sections for table detail sheet <<<#
    def _prepare_table_sections(self, table_name: str, result: Dict) -> List[Dict]:
        sections = []

        # Section 1: Table Summary
        table_hash = result.get('table_hash', {})
        sections.append({
            'title': 'Table Hash Summary',
            'rows': [
                ['Table:', table_name],
                ['Status:', result['status']],
                ['Table Hash Match:', '✓' if table_hash.get('match', False) else '✗'],
                ['PCDS Hash:', table_hash.get('pcds_hash', 'N/A')],
                ['AWS Hash:', table_hash.get('aws_hash', 'N/A')],
                ['PCDS Rows:', table_hash.get('pcds_row_count', 0)],
                ['AWS Rows:', table_hash.get('aws_row_count', 0)]
            ]
        })

        # Section 2: Vintage Hashes
        vintage_hashes = result.get('vintage_hashes', [])
        if vintage_hashes:
            vintage_data = []
            for vh in vintage_hashes:
                vintage_data.append({
                    'Vintage': vh.get('vintage', 'N/A'),
                    'Match': '✓' if vh.get('match', False) else '✗',
                    'PCDS Hash': vh.get('pcds_hash', 'N/A')[:16] + '...',  # Truncate for display
                    'AWS Hash': vh.get('aws_hash', 'N/A')[:16] + '...',
                    'PCDS Rows': vh.get('pcds_row_count', 0),
                    'AWS Rows': vh.get('aws_row_count', 0)
                })

            sections.append({
                'title': f"Vintage Hashes ({len(vintage_hashes)})",
                'dataframe': pd.DataFrame(vintage_data)
            })

        # Section 3: Column Hashes
        column_hashes = result.get('column_hashes', [])
        if column_hashes:
            column_data = []
            for ch in column_hashes:
                column_data.append({
                    'PCDS Column': ch.get('pcds_column', 'N/A'),
                    'AWS Column': ch.get('aws_column', 'N/A'),
                    'Match': '✓' if ch.get('match', False) else '✗',
                    'PCDS Hash': ch.get('pcds_hash', 'N/A')[:16] + '...',
                    'AWS Hash': ch.get('aws_hash', 'N/A')[:16] + '...'
                })

            sections.append({
                'title': f"Column Hashes ({len(column_hashes)})",
                'dataframe': pd.DataFrame(column_data)
            })

        # Section 4: Mismatched Details
        mismatched_vintages = [v for v in vintage_hashes if not v.get('match', False)]
        mismatched_columns = [c for c in column_hashes if not c.get('match', False)]

        if mismatched_vintages or mismatched_columns:
            mismatch_rows = []
            mismatch_rows.append(['=== Mismatched Vintages ===', ''])

            for vh in mismatched_vintages:
                mismatch_rows.append([
                    f"  {vh.get('vintage', 'N/A')}",
                    f"PCDS: {vh.get('pcds_hash', 'N/A')}, AWS: {vh.get('aws_hash', 'N/A')}"
                ])

            if mismatched_columns:
                mismatch_rows.append(['', ''])
                mismatch_rows.append(['=== Mismatched Columns ===', ''])

                for ch in mismatched_columns:
                    mismatch_rows.append([
                        f"  {ch.get('pcds_column', 'N/A')} / {ch.get('aws_column', 'N/A')}",
                        f"PCDS: {ch.get('pcds_hash', 'N/A')}, AWS: {ch.get('aws_hash', 'N/A')}"
                    ])

            sections.append({
                'title': 'Mismatch Details',
                'rows': mismatch_rows
            })

        return sections


if __name__ == "__main__":
    # No argparse - environment loaded from checks/input_pcds
    checker = HashChecker()
    results = checker.run()

    print("\n" + "="*60)
    print("Hash Check Results Summary:")
    print("="*60)
    for table_name, result in results.items():
        print(f"\n{table_name}:")
        print(f"  Status: {result['status']}")
        if result['status'] not in ['no_meta_results', 'not_accessible']:
            table_match = '✓' if result['table_hash'].get('match', False) else '✗'
            print(f"  Table Hash Match: {table_match}")

            if result['vintage_hashes']:
                vintage_matches = sum(1 for v in result['vintage_hashes'] if v['match'])
                print(f"  Vintage Matches: {vintage_matches}/{len(result['vintage_hashes'])}")

            if result['column_hashes']:
                column_matches = sum(1 for c in result['column_hashes'] if c['match'])
                print(f"  Column Matches: {column_matches}/{len(result['column_hashes'])}")
