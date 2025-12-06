# Column Check - Parallel Statistics Comparison
# Compares comprehensive column statistics between PCDS and AWS with frequency analysis

import os
import sys
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


#>>> Column Check class - parallel statistics comparison <<<#
class ColumnChecker:

    #>>> Statistics naming convention mapping <<<#
    STATISTICS_MAPPING = {
        'col_type': 'Type',
        'col_count': 'N_Total',
        'col_distinct': 'N_Unique',
        'col_missing': 'N_Missing',
        'col_max': 'Max',
        'col_min': 'Min',
        'col_avg': 'Mean',
        'col_std': 'Std',
        'col_sum': 'Sum',
        'col_sum_sq': 'Sum_Square',
        'col_freq': 'Frequency'
    }

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

        logger.info(f"ColumnChecker initialized: run_name={self.run_name}, category={self.category}")

    #>>> Determine if data type is numeric (continuous) vs categorical <<<#
    def is_numeric_type_oracle(self, data_type: str) -> bool:
        data_type_upper = data_type.upper()
        return any(t in data_type_upper for t in ['NUMBER', 'FLOAT', 'BINARY_FLOAT', 'BINARY_DOUBLE'])

    #>>> Determine if data type is numeric for Athena <<<#
    def is_numeric_type_athena(self, data_type: str) -> bool:
        data_type_lower = data_type.lower()
        return any(t in data_type_lower for t in ['int', 'double', 'decimal', 'float', 'bigint', 'tinyint', 'smallint'])

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

    #>>> Generate PCDS query for numeric column statistics <<<#
    def generate_pcds_numeric_query(self, column: str, data_type: str, table: str,
                                   where_clause: str, exclude_clause: str) -> str:
        # Handle TIMESTAMP columns
        if 'TIMESTAMP' in data_type.upper():
            col_ref = f'TRUNC({column})'
        else:
            col_ref = column

        return f"""
SELECT
    '{column}' AS col_name,
    '{data_type}' AS col_type,
    COUNT({col_ref}) AS col_count,
    COUNT(DISTINCT {col_ref}) AS col_distinct,
    TO_CHAR(MIN({col_ref})) AS col_min,
    TO_CHAR(MAX({col_ref})) AS col_max,
    AVG({col_ref}) AS col_avg,
    STDDEV({col_ref}) AS col_std,
    SUM({col_ref}) AS col_sum,
    SUM({col_ref} * {col_ref}) AS col_sum_sq,
    '' AS col_freq_raw,
    COUNT(*) - COUNT({col_ref}) AS col_missing
FROM {table}
WHERE {where_clause} AND {exclude_clause}
        """.strip()

    #>>> Generate PCDS query for categorical column statistics <<<#
    def generate_pcds_categorical_query(self, column: str, data_type: str, table: str,
                                       where_clause: str, exclude_clause: str) -> str:
        # Handle TIMESTAMP columns
        if 'TIMESTAMP' in data_type.upper():
            col_ref = f'TRUNC({column})'
        else:
            col_ref = column

        return f"""
WITH FreqTable_RAW AS (
    SELECT {col_ref} AS p_col, COUNT(*) AS value_freq
    FROM {table}
    WHERE {where_clause} AND {exclude_clause}
    GROUP BY {col_ref}
),
FreqTable AS (
    SELECT p_col, value_freq,
           ROW_NUMBER() OVER (ORDER BY value_freq DESC, p_col ASC) AS rn
    FROM FreqTable_RAW
)
SELECT
    '{column}' AS col_name,
    '{data_type}' AS col_type,
    SUM(value_freq) AS col_count,
    COUNT(value_freq) AS col_distinct,
    TO_CHAR(MAX(value_freq)) AS col_max,
    TO_CHAR(MIN(value_freq)) AS col_min,
    AVG(value_freq) AS col_avg,
    STDDEV(value_freq) AS col_std,
    SUM(value_freq) AS col_sum,
    SUM(value_freq * value_freq) AS col_sum_sq,
    LISTAGG(p_col || '::' || value_freq, '||') WITHIN GROUP (ORDER BY rn) AS col_freq_raw,
    COALESCE((SELECT value_freq FROM FreqTable WHERE p_col IS NULL AND ROWNUM = 1), 0) AS col_missing
FROM FreqTable
WHERE rn <= 20
        """.strip()

    #>>> Generate AWS query for numeric column statistics <<<#
    def generate_aws_numeric_query(self, column: str, data_type: str, table: str,
                                  where_clause: str, exclude_clause: str) -> str:
        database, table_name = table.split('.', 1)

        return f"""
SELECT
    '{column}' AS col_name,
    '{data_type}' AS col_type,
    COUNT({column}) AS col_count,
    COUNT(DISTINCT {column}) AS col_distinct,
    CAST(MAX({column}) AS VARCHAR) AS col_max,
    CAST(MIN({column}) AS VARCHAR) AS col_min,
    AVG(CAST({column} AS DOUBLE)) AS col_avg,
    STDDEV_SAMP(CAST({column} AS DOUBLE)) AS col_std,
    SUM(CAST({column} AS DOUBLE)) AS col_sum,
    SUM(CAST({column} AS DOUBLE) * CAST({column} AS DOUBLE)) AS col_sum_sq,
    '' AS col_freq_raw,
    COUNT(*) - COUNT({column}) AS col_missing
FROM {database}.{table_name}
WHERE {where_clause} AND {exclude_clause}
        """.strip()

    #>>> Generate AWS query for categorical column statistics <<<#
    def generate_aws_categorical_query(self, column: str, data_type: str, table: str,
                                      where_clause: str, exclude_clause: str) -> str:
        database, table_name = table.split('.', 1)

        return f"""
WITH FreqTable_RAW AS (
    SELECT {column} AS p_col, COUNT(*) AS value_freq
    FROM {database}.{table_name}
    WHERE {where_clause} AND {exclude_clause}
    GROUP BY {column}
),
FreqTable AS (
    SELECT p_col, value_freq,
           ROW_NUMBER() OVER (ORDER BY value_freq DESC, p_col ASC) AS rn
    FROM FreqTable_RAW
)
SELECT
    '{column}' AS col_name,
    '{data_type}' AS col_type,
    SUM(value_freq) AS col_count,
    COUNT(value_freq) AS col_distinct,
    CAST(MAX(value_freq) AS VARCHAR) AS col_max,
    CAST(MIN(value_freq) AS VARCHAR) AS col_min,
    AVG(CAST(value_freq AS DOUBLE)) AS col_avg,
    STDDEV_SAMP(CAST(value_freq AS DOUBLE)) AS col_std,
    SUM(value_freq) AS col_sum,
    SUM(value_freq * value_freq) AS col_sum_sq,
    ARRAY_JOIN(ARRAY_AGG(COALESCE(CAST(p_col AS VARCHAR), '') || '::' || CAST(value_freq AS VARCHAR)), '||') AS col_freq_raw,
    COALESCE((SELECT value_freq FROM FreqTable WHERE p_col IS NULL LIMIT 1), 0) AS col_missing
FROM FreqTable
WHERE rn <= 20
        """.strip()

    #>>> Execute single PCDS query <<<#
    def execute_pcds_query(self, query: str, service: str, column_name: str) -> pd.DataFrame:
        try:
            logger.debug(f"Executing PCDS query for column: {column_name}")
            result = query_pcds(query, service)
            return result
        except Exception as e:
            logger.error(f"PCDS query failed for {column_name}: {e}")
            return pd.DataFrame()

    #>>> Execute single AWS query <<<#
    def execute_aws_query(self, query: str, database: str, column_name: str) -> pd.DataFrame:
        try:
            logger.debug(f"Executing AWS query for column: {column_name}")
            result = query_aws(query, database)
            return result
        except Exception as e:
            logger.error(f"AWS query failed for {column_name}: {e}")
            return pd.DataFrame()

    #>>> Execute PCDS queries in parallel <<<#
    def execute_pcds_queries_parallel(self, queries: List[Tuple[str, str, str]],
                                     service: str) -> pd.DataFrame:
        logger.info(f"Executing {len(queries)} PCDS queries in parallel (max {self.pcds_parallel} workers)...")

        results = []
        with ThreadPoolExecutor(max_workers=self.pcds_parallel) as executor:
            future_to_col = {
                executor.submit(self.execute_pcds_query, query, service, col_name): col_name
                for query, col_name, _ in queries
            }

            for future in tqdm(as_completed(future_to_col), total=len(queries), desc="PCDS queries"):
                col_name = future_to_col[future]
                try:
                    result = future.result()
                    if not result.empty:
                        results.append(result)
                except Exception as e:
                    logger.error(f"Error processing PCDS result for {col_name}: {e}")

        if results:
            return pd.concat(results, ignore_index=True)
        return pd.DataFrame()

    #>>> Execute AWS queries in parallel <<<#
    def execute_aws_queries_parallel(self, queries: List[Tuple[str, str, str]],
                                    database: str) -> pd.DataFrame:
        logger.info(f"Executing {len(queries)} AWS queries in parallel (max {self.aws_parallel} workers)...")

        results = []
        with ThreadPoolExecutor(max_workers=self.aws_parallel) as executor:
            future_to_col = {
                executor.submit(self.execute_aws_query, query, database, col_name): col_name
                for query, col_name, _ in queries
            }

            for future in tqdm(as_completed(future_to_col), total=len(queries), desc="AWS queries"):
                col_name = future_to_col[future]
                try:
                    result = future.result()
                    if not result.empty:
                        results.append(result)
                except Exception as e:
                    logger.error(f"Error processing AWS result for {col_name}: {e}")

        if results:
            return pd.concat(results, ignore_index=True)
        return pd.DataFrame()

    #>>> Parse frequency list from raw string <<<#
    def parse_frequency_list(self, freq_raw: str) -> List[Tuple[str, int]]:
        if not freq_raw or pd.isna(freq_raw):
            return []

        items = []
        for item in str(freq_raw).split('||'):
            if '::' in item:
                try:
                    value, count = item.rsplit('::', 1)
                    items.append((value, int(count)))
                except ValueError:
                    continue

        # Sort alphanumerically by value (not by frequency)
        items.sort(key=lambda x: str(x[0]))
        return items

    #>>> Check if string contains date-like patterns <<<#
    @staticmethod
    def contains_datelike(val_str: str) -> bool:
        import re
        if not val_str:
            return False
        # Check for date patterns: YYYY-MM-DD, DD-MM-YYYY, YYYYMMDD, etc.
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'\d{2}-\d{2}-\d{4}',  # DD-MM-YYYY
            r'\d{4}/\d{2}/\d{2}',  # YYYY/MM/DD
            r'\d{2}/\d{2}/\d{4}',  # DD/MM/YYYY
            r'\d{8}',              # YYYYMMDD
        ]
        return any(re.search(pattern, val_str) for pattern in date_patterns)

    #>>> Parse date value with multiple format attempts <<<#
    @staticmethod
    def parse_date_value(val):
        if pd.isna(val):
            return None

        val_str = str(val).strip()
        if not val_str:
            return None

        # Try common date formats
        date_formats = [
            '%Y-%m-%d',
            '%Y/%m/%d',
            '%d-%m-%Y',
            '%d/%m/%Y',
            '%Y%m%d',
            '%Y-%m-%d %H:%M:%S',
            '%Y/%m/%d %H:%M:%S',
        ]

        for fmt in date_formats:
            try:
                from datetime import datetime
                return datetime.strptime(val_str, fmt).date()
            except (ValueError, AttributeError):
                continue

        return None

    #>>> Robust value comparison with tolerance for numeric, NaN, dates, and lists <<<#
    @staticmethod
    def _values_different(val1, val2) -> bool:
        #>>> Handle list comparisons (for frequency tuples) <<<#
        if isinstance(val1, list):
            if not isinstance(val2, list) or len(val1) != len(val2):
                return True
            # Compare each tuple element by element
            return any(
                ColumnChecker._values_different(x1, x2)
                for t1, t2 in zip(val1, val2)
                for x1, x2 in zip(t1, t2)
            )

        #>>> Both NaN/None - considered equal <<<#
        if pd.isna(val1) and pd.isna(val2):
            return False

        #>>> One is NaN/None - check special case: 0 in PCDS is treated as NA in AWS <<<#
        if pd.isna(val1) or pd.isna(val2):
            # Special case: 0 == NA
            if (val1 == 0 and pd.isna(val2)) or (pd.isna(val1) and val2 == 0):
                return False
            return True

        #>>> Try numeric comparison with tolerance <<<#
        try:
            num1 = float(val1)
            num2 = float(val2)
            return not np.isclose(num1, num2, atol=1e-6, rtol=1e-6)
        except (ValueError, TypeError):
            pass

        #>>> Try date comparison <<<#
        val1_str = str(val1)
        val2_str = str(val2)

        if ColumnChecker.contains_datelike(val1_str) or ColumnChecker.contains_datelike(val2_str):
            date1 = ColumnChecker.parse_date_value(val1)
            date2 = ColumnChecker.parse_date_value(val2)
            if date1 and date2:
                return date1 != date2

        #>>> Fallback to string comparison <<<#
        return val1_str != val2_str

    #>>> Compare statistics between PCDS and AWS with robust value comparison <<<#
    def compare_statistics(self, pcds_stat: Dict, aws_stat: Dict) -> Dict:
        #>>> Initialize comparison result <<<#
        comparison = {
            'column': pcds_stat.get('col_name', ''),
            'data_type_pcds': pcds_stat.get('col_type', ''),
            'data_type_aws': aws_stat.get('col_type', ''),
        }

        #>>> Parse frequency data <<<#
        pcds_freq = self.parse_frequency_list(pcds_stat.get('col_freq_raw', ''))
        aws_freq = self.parse_frequency_list(aws_stat.get('col_freq_raw', ''))

        #>>> Track mismatches for detailed reporting <<<#
        mismatches = []
        non_freq_stats_match = True
        freq_match = True

        #>>> Compare all statistics except col_type <<<#
        for col_key, stat_name in self.STATISTICS_MAPPING.items():
            # Skip col_type - we always report both types
            if col_key == 'col_type':
                continue

            # Get values from both sources
            pcds_val = pcds_stat.get(col_key)
            aws_val = aws_stat.get(col_key)

            # Special handling for frequency
            if col_key == 'col_freq':
                pcds_val = pcds_freq
                aws_val = aws_freq

            # Use robust comparison
            different = self._values_different(pcds_val, aws_val)

            # Store match result with proper naming
            comparison[f'{stat_name}_match'] = not different

            # Track mismatches
            if different:
                mismatches.append({
                    'statistic': stat_name,
                    'pcds_value': pcds_val,
                    'aws_value': aws_val
                })

                # Categorize mismatch type
                if col_key == 'col_freq':
                    freq_match = False
                else:
                    non_freq_stats_match = False

        #>>> Determine overall status <<<#
        # PASS: All statistics match (except col_type)
        # PARTIAL: All match except col_freq
        # FAIL: Any stat besides col_freq fails
        if non_freq_stats_match and freq_match:
            comparison['status'] = 'PASS'
        elif non_freq_stats_match and not freq_match:
            comparison['status'] = 'PARTIAL'
        else:
            comparison['status'] = 'FAIL'

        #>>> Store detailed information <<<#
        comparison['mismatches'] = mismatches
        comparison['mismatch_count'] = len(mismatches)
        comparison['pcds_stats'] = pcds_stat
        comparison['aws_stats'] = aws_stat
        comparison['pcds_freq'] = pcds_freq
        comparison['aws_freq'] = aws_freq

        #>>> Legacy fields for backward compatibility <<<#
        comparison['count_match'] = comparison.get('N_Total_match', False)
        comparison['distinct_match'] = comparison.get('N_Unique_match', False)
        comparison['freq_match'] = comparison.get('Frequency_match', False)

        return comparison

    #>>> Load meta check results from S3 <<<#
    def load_meta_results(self, table_name: str) -> Optional[Dict]:
        try:
            filename = f"{table_name.replace('.', '_')}_meta.json"
            result = self.s3.download_json('meta_check', filename)
            return result
        except Exception as e:
            logger.error(f"Failed to load meta results for {table_name}: {e}")
            return None

    #>>> Process single table for column statistics comparison <<<#
    def process_table(self, table_name: str) -> Dict:
        logger.info(f"Processing table: {table_name}")

        result = {
            'table_name': table_name,
            'status': 'pending',
            'comparable_columns': 0,
            'matched_columns': 0,
            'mismatched_columns': 0,
            'comparisons': []
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

        # 2. Get comparable columns
        column_mapping = meta_results.get('column_mapping', {})
        comparable_cols = column_mapping.get('comparable', {})

        if not comparable_cols:
            result['status'] = 'no_comparable_columns'
            logger.warning(f"  No comparable columns found for {table_name}")
            return result

        result['comparable_columns'] = len(comparable_cols)
        logger.info(f"  Found {len(comparable_cols)} comparable columns")

        # 3. Get date info for exclude clauses
        date_var = meta_results.get('date_var', '')
        date_type = meta_results.get('date_type')
        date_format = meta_results.get('date_format')

        # 4. Build exclude clauses for unequal dates
        mismatched_dates = [d['date'] for d in meta_results.get('row_counts', {}).get('mismatched_dates', [])]
        pcds_exclude = self.build_exclude_clause_pcds(mismatched_dates, date_var, date_type, date_format)
        aws_exclude = self.build_exclude_clause_aws(mismatched_dates, date_var, date_type, date_format)

        logger.info(f"  Excluding {len(mismatched_dates)} unequal dates")

        # 5. Get type dictionaries
        pcds_types = column_mapping.get('pcds_types', {})
        aws_types = column_mapping.get('aws_types', {})

        # 6. Generate queries for all comparable columns
        service_name = table_name.split('.', 1)[0]
        table_only = table_name.split('.', 1)[1]
        pcds_where = meta_results.get('pcds_where', '1=1') if 'pcds_where' in meta_results else '1=1'

        aws_table = meta_results.get('aws_table', '')
        database = aws_table.split('.', 1)[0] if aws_table else ''
        aws_where = meta_results.get('aws_where', '1=1') if 'aws_where' in meta_results else '1=1'

        pcds_queries = []
        aws_queries = []

        for pcds_col, aws_col in comparable_cols.items():
            pcds_type = pcds_types.get(pcds_col, '')
            aws_type = aws_types.get(aws_col, '')

            if not pcds_type or not aws_type:
                continue

            # Determine if numeric
            is_numeric_pcds = self.is_numeric_type_oracle(pcds_type)
            is_numeric_aws = self.is_numeric_type_athena(aws_type)

            # Generate PCDS query
            if is_numeric_pcds:
                pcds_query = self.generate_pcds_numeric_query(
                    pcds_col, pcds_type, table_only.upper(), pcds_where, pcds_exclude
                )
            else:
                pcds_query = self.generate_pcds_categorical_query(
                    pcds_col, pcds_type, table_only.upper(), pcds_where, pcds_exclude
                )

            pcds_queries.append((pcds_query, pcds_col, pcds_type))

            # Generate AWS query
            if is_numeric_aws:
                aws_query = self.generate_aws_numeric_query(
                    aws_col, aws_type, aws_table, aws_where, aws_exclude
                )
            else:
                aws_query = self.generate_aws_categorical_query(
                    aws_col, aws_type, aws_table, aws_where, aws_exclude
                )

            aws_queries.append((aws_query, aws_col, aws_type))

        logger.info(f"  Generated {len(pcds_queries)} PCDS queries and {len(aws_queries)} AWS queries")

        # 7. Execute queries in parallel
        pcds_stats_df = self.execute_pcds_queries_parallel(pcds_queries, service_name)
        aws_stats_df = self.execute_aws_queries_parallel(aws_queries, database)

        # 8. Save raw statistics to S3
        if not pcds_stats_df.empty:
            self.s3.upload_parquet(pcds_stats_df, 'column_check', f"{table_name.replace('.', '_')}_pcds_stats")
        if not aws_stats_df.empty:
            self.s3.upload_parquet(aws_stats_df, 'column_check', f"{table_name.replace('.', '_')}_aws_stats")

        # 9. Compare statistics
        logger.info("  Comparing statistics...")
        comparisons = []

        if not pcds_stats_df.empty and not aws_stats_df.empty:
            # Convert to dictionaries for easier comparison
            pcds_stats_dict = {row['col_name']: row.to_dict() for _, row in pcds_stats_df.iterrows()}
            aws_stats_dict = {row['col_name']: row.to_dict() for _, row in aws_stats_df.iterrows()}

            # Map PCDS columns to AWS columns for comparison
            for pcds_col, aws_col in comparable_cols.items():
                if pcds_col in pcds_stats_dict and aws_col in aws_stats_dict:
                    comparison = self.compare_statistics(
                        pcds_stats_dict[pcds_col],
                        aws_stats_dict[aws_col]
                    )
                    comparisons.append(comparison)

        result['comparisons'] = comparisons
        result['matched_columns'] = len([c for c in comparisons if c['status'] == 'PASS'])
        result['mismatched_columns'] = len([c for c in comparisons if c['status'] == 'FAIL'])
        result['status'] = 'completed'

        logger.info(f"  ✓ Completed: {result['matched_columns']}/{len(comparisons)} columns matched")

        # 10. Save comparison results to S3
        self.s3.upload_json(result, 'column_check', f"{table_name.replace('.', '_')}_comparison.json")

        return result

    #>>> Run column check for all tables <<<#
    def run(self):
        logger.info(f"Starting Column Check for run: {self.run_name}, category: {self.category}")

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

        logger.info("✓ Column Check completed!")
        return self.results

    #>>> Generate Excel report for column check results <<<#
    def generate_excel_report(self):
        output_path = f"output_{self.run_name}_column_check.xlsx"

        with ExcelReporter(output_path) as reporter:
            # Prepare summary data
            summary_rows = []
            for table_name, result in self.results.items():
                if result['status'] == 'completed':
                    match_rate = (result['matched_columns'] / result['comparable_columns'] * 100
                                 if result['comparable_columns'] > 0 else 0)
                    summary_rows.append([
                        table_name.split('.')[-1],
                        result['comparable_columns'],
                        result['matched_columns'],
                        result['mismatched_columns'],
                        f"{match_rate:.1f}%"
                    ])

            reporter.create_summary_sheet(
                title='Column Check Summary',
                headers=['Table', 'Total Cols', 'Matched', 'Mismatched', 'Match Rate'],
                data_rows=summary_rows
            )

            # Create detail sheets for each table
            for table_name, result in self.results.items():
                if result['status'] == 'completed' and result['comparisons']:
                    sections = self._prepare_table_sections(table_name, result)
                    reporter.create_detail_sheet(table_name.split('.')[-1], sections)

        # Upload Excel to S3
        self.s3.upload_file(output_path, 'column_check', output_path)
        logger.info(f"Excel report uploaded: {output_path}")

    #>>> Prepare sections for table detail sheet <<<#
    def _prepare_table_sections(self, table_name: str, result: Dict) -> List[Dict]:
        sections = []

        # Section 1: Summary
        sections.append({
            'title': 'Table Summary',
            'rows': [
                ['Table:', table_name],
                ['Comparable Columns:', result['comparable_columns']],
                ['Matched Columns:', result['matched_columns']],
                ['Mismatched Columns:', result['mismatched_columns']],
                ['Match Rate:', f"{result['matched_columns']/result['comparable_columns']*100:.1f}%"
                 if result['comparable_columns'] > 0 else '0%']
            ]
        })

        # Section 2: Comparison Results
        comparison_data = []
        for comp in result['comparisons']:
            comparison_data.append({
                'Column': comp['column'],
                'Status': comp['status'],
                'Count Match': '✓' if comp['count_match'] else '✗',
                'Distinct Match': '✓' if comp['distinct_match'] else '✗',
                'Freq Match': '✓' if comp['freq_match'] else '✗',
                'PCDS Type': comp['data_type_pcds'],
                'AWS Type': comp['data_type_aws']
            })

        if comparison_data:
            sections.append({
                'title': 'Column Comparison Results',
                'dataframe': pd.DataFrame(comparison_data)
            })

        # Section 3: Failed Columns Detail
        failed = [c for c in result['comparisons'] if c['status'] == 'FAIL']
        if failed:
            failed_detail = []
            for comp in failed:
                failed_detail.append({
                    'Column': comp['column'],
                    'PCDS Count': comp['pcds_stats'].get('col_count', 'N/A'),
                    'AWS Count': comp['aws_stats'].get('col_count', 'N/A'),
                    'PCDS Distinct': comp['pcds_stats'].get('col_distinct', 'N/A'),
                    'AWS Distinct': comp['aws_stats'].get('col_distinct', 'N/A')
                })

            sections.append({
                'title': f"Failed Columns Detail ({len(failed)})",
                'dataframe': pd.DataFrame(failed_detail)
            })

        return sections


if __name__ == "__main__":
    # No argparse - environment loaded from checks/input_pcds
    checker = ColumnChecker()
    results = checker.run()

    print("\n" + "="*60)
    print("Column Check Results Summary:")
    print("="*60)
    for table_name, result in results.items():
        print(f"\n{table_name}:")
        print(f"  Status: {result['status']}")
        if result['status'] == 'completed':
            print(f"  Comparable Columns: {result['comparable_columns']}")
            print(f"  Matched: {result['matched_columns']}")
            print(f"  Mismatched: {result['mismatched_columns']}")
            match_rate = (result['matched_columns'] / result['comparable_columns'] * 100
                         if result['comparable_columns'] > 0 else 0)
            print(f"  Match Rate: {match_rate:.1f}%")
