# Meta Check - Comprehensive Table Validation
# Combines table accessibility, row counts per date, column mapping with data types

import os
import sys
import re
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple, Optional
import pandas as pd
from loguru import logger
from tqdm import tqdm
from dotenv import load_dotenv

# Add paths for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'common'))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

from config_loader import (
    get_input_tables,
    get_column_mappings,
    query_pcds,
    query_aws,
    is_missing
)
from s3_utils import create_s3_manager
from excel_reporter import ExcelReporter


#>>> Meta Check class - comprehensive table validation <<<#
class MetaChecker:

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

        logger.info(f"MetaChecker initialized: run_name={self.run_name}, category={self.category}")

    #>>> Detect date column type and format from metadata and sample data <<<#
    def detect_date_type(self, table: str, date_var: str, service: str) -> Tuple[Optional[str], Optional[str]]:
        if not date_var or date_var == 'NaT':
            return None, None

        try:
            # Extract table name from service.table format
            table_name = table.split('.')[1] if '.' in table else table

            # Query metadata for data type
            query = f"""
            SELECT data_type
            FROM all_tab_cols
            WHERE table_name = UPPER('{table_name}')
              AND column_name = UPPER('{date_var}')
            """
            result = query_pcds(query, service)

            if result.empty:
                logger.warning(f"Date variable {date_var} not found in {table_name}")
                return None, None

            data_type = result.iloc[0]['data_type'].upper()
            logger.debug(f"Detected data_type for {date_var}: {data_type}")

            # If DATE or TIMESTAMP, return type with no format
            if 'DATE' in data_type or 'TIMESTAMP' in data_type:
                return data_type, None

            # If STRING/VARCHAR, detect format from sample
            if 'CHAR' in data_type:
                sample_query = f"SELECT {date_var} FROM {table_name.upper()} WHERE ROWNUM = 1"
                sample = query_pcds(sample_query, service)

                if sample.empty:
                    return 'STRING', None

                sample_value = str(sample.iloc[0][date_var])
                logger.debug(f"Sample value for {date_var}: {sample_value}")

                # Detect format
                if re.match(r'^\d{8}$', sample_value):
                    return 'STRING', '%Y%m%d'
                elif re.match(r'^\d{4}-\d{2}-\d{2}$', sample_value):
                    return 'STRING', '%Y-%m-%d'
                else:
                    logger.warning(f"Unknown date format: {sample_value}")
                    return 'STRING', None

            return data_type, None

        except Exception as e:
            logger.error(f"Error detecting date type for {date_var}: {e}")
            return None, None

    #>>> Build index-friendly WHERE clause for date filtering <<<#
    def build_date_filter(self, date_var: str, date_type: str, date_format: Optional[str],
                         start_date: str, end_date: str, platform: str = 'PCDS') -> str:
        if not date_var or not date_type:
            return '1=1'

        if platform == 'PCDS':
            if date_type in ['DATE', 'TIMESTAMP']:
                # Use DATE literals - index friendly!
                return f"{date_var} >= DATE '{start_date}' AND {date_var} < DATE '{end_date}'"

            elif date_type == 'STRING':
                if date_format == '%Y%m%d':
                    # Convert to YYYYMMDD format
                    start_str = start_date.replace('-', '')
                    end_str = end_date.replace('-', '')
                    return f"{date_var} >= '{start_str}' AND {date_var} < '{end_str}'"

                elif date_format == '%Y-%m-%d':
                    # Use as-is
                    return f"{date_var} >= '{start_date}' AND {date_var} < '{end_date}'"

        elif platform == 'AWS':
            if date_type in ['DATE', 'TIMESTAMP']:
                return f"{date_var} >= DATE '{start_date}' AND {date_var} < DATE '{end_date}'"

            elif date_type == 'STRING':
                if date_format == '%Y%m%d':
                    start_str = start_date.replace('-', '')
                    end_str = end_date.replace('-', '')
                    return f"{date_var} >= '{start_str}' AND {date_var} < '{end_str}'"

                elif date_format == '%Y-%m-%d':
                    return f"{date_var} >= '{start_date}' AND {date_var} < '{end_date}'"

        return '1=1'

    #>>> Build Oracle query to count rows by partition date - index friendly <<<#
    def build_pcds_count_query(self, table: str, date_var: str, date_type: str,
                              date_format: Optional[str], where_clause: str = '1=1') -> str:
        if not date_var or not date_type:
            return f"SELECT COUNT(*) AS nrows FROM {table} WHERE {where_clause}"

        # Group by raw date_var - index friendly!
        # Convert in SELECT only for display
        if date_type in ['DATE', 'TIMESTAMP']:
            select_expr = f"TO_CHAR({date_var}, 'YYYY-MM-DD') AS partition_date"
            group_expr = date_var
        elif date_type == 'STRING' and date_format == '%Y%m%d':
            # Convert YYYYMMDD to YYYY-MM-DD in SELECT only
            select_expr = f"SUBSTR({date_var}, 1, 4) || '-' || SUBSTR({date_var}, 5, 2) || '-' || SUBSTR({date_var}, 7, 2) AS partition_date"
            group_expr = date_var
        else:
            select_expr = f"{date_var} AS partition_date"
            group_expr = date_var

        return f"""
SELECT {select_expr}, COUNT(*) AS nrows
FROM {table}
WHERE {where_clause}
GROUP BY {group_expr}
ORDER BY {group_expr} DESC
        """.strip()

    #>>> Build Athena query to count rows by partition date - index friendly <<<#
    def build_aws_count_query(self, table: str, date_var: str, date_type: str,
                             date_format: Optional[str], where_clause: str = '1=1') -> str:
        database, table_name = table.split('.', 1)

        if not date_var or not date_type:
            return f"SELECT COUNT(*) AS nrows FROM {database}.{table_name} WHERE {where_clause}"

        # Similar to PCDS - group by raw column
        if date_type in ['DATE', 'TIMESTAMP']:
            select_expr = f"DATE_FORMAT({date_var}, '%Y-%m-%d') AS partition_date"
            group_expr = date_var
        elif date_type == 'STRING' and date_format == '%Y%m%d':
            # Convert YYYYMMDD to YYYY-MM-DD in SELECT only
            select_expr = f"SUBSTR({date_var}, 1, 4) || '-' || SUBSTR({date_var}, 5, 2) || '-' || SUBSTR({date_var}, 7, 2) AS partition_date"
            group_expr = date_var
        else:
            select_expr = f"{date_var} AS partition_date"
            group_expr = date_var

        return f"""
SELECT {select_expr}, COUNT(*) AS nrows
FROM {database}.{table_name}
WHERE {where_clause}
GROUP BY {group_expr}
ORDER BY {group_expr} DESC
        """.strip()

    #>>> Get vintages as [start_date, end_date] ranges for index-friendly queries <<<#
    def get_vintages_as_ranges(self, table: str, date_var: str, date_type: str,
                              date_format: Optional[str], partition_type: str,
                              service: str, where_clause: str = '1=1') -> List[Dict]:
        if not date_var or not date_type:
            return []

        try:
            # Extract table name
            table_name = table.split('.')[1] if '.' in table else table

            # Get distinct dates - simple query, uses index
            query = f"SELECT DISTINCT {date_var} FROM {table_name.upper()} WHERE {where_clause}"
            dates_df = query_pcds(query, service)

            if dates_df.empty:
                return []

            # Parse dates based on type/format
            dates = []
            for val in dates_df[date_var]:
                if pd.isna(val):
                    continue

                try:
                    if date_type in ['DATE', 'TIMESTAMP']:
                        date_obj = pd.to_datetime(val).date()
                    elif date_type == 'STRING' and date_format:
                        date_obj = datetime.strptime(str(val), date_format).date()
                    else:
                        continue
                    dates.append(date_obj)
                except Exception as e:
                    logger.debug(f"Failed to parse date {val}: {e}")
                    continue

            if not dates:
                return []

            dates = sorted(dates)

            # Group into vintages
            vintages = {}
            for date_obj in dates:
                if partition_type == 'week':
                    vintage = date_obj.strftime('%Y-W%W')
                elif partition_type == 'month':
                    vintage = date_obj.strftime('%Y-%m')
                elif partition_type == 'day':
                    vintage = date_obj.strftime('%Y-%m-%d')
                else:
                    vintage = 'all'

                if vintage not in vintages:
                    vintages[vintage] = []
                vintages[vintage].append(date_obj)

            # Convert to ranges
            vintage_ranges = []
            for vintage, dates_list in sorted(vintages.items()):
                start_date = min(dates_list)
                end_date = max(dates_list) + timedelta(days=1)  # Exclusive end

                vintage_ranges.append({
                    'vintage': vintage,
                    'start_date': start_date.strftime('%Y-%m-%d'),
                    'end_date': end_date.strftime('%Y-%m-%d')
                })

            return vintage_ranges

        except Exception as e:
            logger.error(f"Error getting vintages: {e}")
            return []

    #>>> Build query to get PCDS column metadata with data types <<<#
    def get_pcds_meta_query(self, table_name: str) -> str:
        return f"""
SELECT column_name,
       data_type || CASE
           WHEN data_type = 'NUMBER' THEN
               CASE WHEN data_precision IS NULL AND data_scale IS NULL
                   THEN ''
               ELSE '(' || TO_CHAR(data_precision) || ',' || TO_CHAR(data_scale) || ')'
               END
           WHEN data_type LIKE '%CHAR%' THEN '(' || TO_CHAR(data_length) || ')'
           ELSE ''
       END AS data_type
FROM all_tab_cols
WHERE table_name = UPPER('{table_name}')
ORDER BY column_id
        """.strip()

    #>>> Build query to get AWS column metadata with data types <<<#
    def get_aws_meta_query(self, database: str, table_name: str) -> str:
        return f"""
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = LOWER('{database}')
  AND table_name = LOWER('{table_name}')
ORDER BY ordinal_position
        """.strip()

    #>>> Check if PCDS and AWS data types are compatible <<<#
    def types_compatible(self, pcds_type: str, aws_type: str) -> bool:
        pcds_type = pcds_type.upper()
        aws_type = aws_type.lower()

        # NUMBER -> double/decimal
        if 'NUMBER' in pcds_type:
            return any(t in aws_type for t in ['double', 'decimal', 'bigint', 'int'])

        # VARCHAR2/CHAR -> string/varchar
        if any(t in pcds_type for t in ['VARCHAR', 'CHAR']):
            return any(t in aws_type for t in ['string', 'varchar', 'char'])

        # DATE -> date/timestamp
        if 'DATE' in pcds_type and 'TIMESTAMP' not in pcds_type:
            return any(t in aws_type for t in ['date', 'timestamp'])

        # TIMESTAMP -> timestamp
        if 'TIMESTAMP' in pcds_type:
            return 'timestamp' in aws_type

        return False

    #>>> Build column mapping and identify discrepancies <<<#
    def build_column_mapping(self, crosswalk: pd.DataFrame,
                            pcds_meta: pd.DataFrame,
                            aws_meta: pd.DataFrame) -> Dict:
        # Convert metadata to sets
        pcds_cols = set(pcds_meta['column_name'].str.upper())
        aws_cols = set(aws_meta['column_name'].str.lower())

        # Create type dictionaries
        pcds_types = pcds_meta.set_index('column_name')['data_type'].to_dict()
        aws_types = aws_meta.set_index('column_name')['data_type'].to_dict()

        # Extract mappings from crosswalk
        pcds_to_aws = {}
        tokenized = set()

        for _, row in crosswalk.iterrows():
            pcds_col = str(row.get('pcds_col', '')).strip().upper()
            aws_col = str(row.get('aws_col', '')).strip().lower()
            is_tokenized = row.get('is_tokenized', False)

            if pcds_col and aws_col and pcds_col != 'NAN' and aws_col != 'nan':
                pcds_to_aws[pcds_col] = aws_col
                if is_tokenized:
                    tokenized.add(pcds_col)

        # Find comparable columns (in both systems, documented, not tokenized)
        comparable = {}
        type_mismatches = []

        for pcds, aws in pcds_to_aws.items():
            if pcds in pcds_cols and aws in aws_cols and pcds not in tokenized:
                comparable[pcds] = aws

                # Check type compatibility
                if pcds in pcds_types and aws in aws_types:
                    if not self.types_compatible(pcds_types[pcds], aws_types[aws]):
                        type_mismatches.append({
                            'pcds_column': pcds,
                            'aws_column': aws,
                            'pcds_type': pcds_types[pcds],
                            'aws_type': aws_types[aws]
                        })

        # Find PCDS-only columns
        mapped_pcds = set(pcds_to_aws.keys())
        pcds_only = pcds_cols - mapped_pcds

        # Find AWS-only columns
        mapped_aws = set(pcds_to_aws.values())
        aws_only = aws_cols - mapped_aws

        return {
            'comparable': comparable,
            'tokenized': sorted(list(tokenized & pcds_cols)),  # Only tokenized that exist
            'pcds_only': sorted(list(pcds_only)),
            'aws_only': sorted(list(aws_only)),
            'type_mismatches': type_mismatches,
            'pcds_types': pcds_types,
            'aws_types': aws_types
        }

    #>>> Check table accessibility on PCDS <<<#
    def check_pcds_accessibility(self, pcds_tbl: str) -> Dict:
        try:
            service_name, table_name = pcds_tbl.split('.', 1)
            sql = f"SELECT COUNT(*) as cnt FROM {table_name.upper()} WHERE ROWNUM = 1"
            result = query_pcds(sql, service_name)
            return {'accessible': True, 'error': None}
        except Exception as e:
            logger.error(f"PCDS access error for {pcds_tbl}: {e}")
            return {'accessible': False, 'error': str(e)}

    #>>> Check table accessibility on AWS <<<#
    def check_aws_accessibility(self, aws_tbl: str) -> Dict:
        try:
            database, table_name = aws_tbl.split('.', 1)
            sql = f"SELECT COUNT(*) as cnt FROM {database}.{table_name} LIMIT 1"
            result = query_aws(sql, database)
            return {'accessible': True, 'error': None}
        except Exception as e:
            logger.error(f"AWS access error for {aws_tbl}: {e}")
            return {'accessible': False, 'error': str(e)}

    #>>> Get row counts by date for PCDS with date type auto-detection <<<#
    def get_pcds_row_counts(self, table_info: Dict, date_type: str, date_format: Optional[str]) -> Dict:
        try:
            service_name, table_name = table_info['pcds_tbl'].split('.', 1)
            pcds_var = table_info.get('pcds_var', '')
            pcds_where = table_info.get('pcds_where') or '1=1'

            query = self.build_pcds_count_query(
                table_name.upper(), pcds_var, date_type, date_format, pcds_where
            )
            df = query_pcds(query, service_name)

            if 'PARTITION_DATE' in df.columns:
                counts = df.set_index('PARTITION_DATE')['NROWS'].to_dict()
                total = df['NROWS'].sum()
            else:
                counts = {}
                total = int(df.iloc[0, 0]) if len(df) > 0 else 0

            return {'counts': counts, 'total': total, 'error': None}
        except Exception as e:
            logger.error(f"Error getting PCDS row counts: {e}")
            return {'counts': {}, 'total': 0, 'error': str(e)}

    #>>> Get row counts by date for AWS with date type auto-detection <<<#
    def get_aws_row_counts(self, table_info: Dict, date_type: str, date_format: Optional[str]) -> Dict:
        try:
            aws_var = table_info.get('aws_var', '')
            aws_where = table_info.get('aws_where') or '1=1'

            query = self.build_aws_count_query(
                table_info['aws_tbl'], aws_var, date_type, date_format, aws_where
            )
            database = table_info['aws_tbl'].split('.', 1)[0]
            df = query_aws(query, database)

            if 'partition_date' in df.columns:
                counts = df.set_index('partition_date')['nrows'].to_dict()
                total = df['nrows'].sum()
            else:
                counts = {}
                total = int(df.iloc[0, 0]) if len(df) > 0 else 0

            return {'counts': counts, 'total': total, 'error': None}
        except Exception as e:
            logger.error(f"Error getting AWS row counts: {e}")
            return {'counts': {}, 'total': 0, 'error': str(e)}

    #>>> Get column metadata from PCDS <<<#
    def get_pcds_columns(self, pcds_tbl: str) -> pd.DataFrame:
        try:
            service_name, table_name = pcds_tbl.split('.', 1)
            query = self.get_pcds_meta_query(table_name)
            return query_pcds(query, service_name)
        except Exception as e:
            logger.error(f"Error getting PCDS columns: {e}")
            return pd.DataFrame(columns=['column_name', 'data_type'])

    #>>> Get column metadata from AWS <<<#
    def get_aws_columns(self, aws_tbl: str) -> pd.DataFrame:
        try:
            database, table_name = aws_tbl.split('.', 1)
            query = self.get_aws_meta_query(database, table_name)
            return query_aws(query, database)
        except Exception as e:
            logger.error(f"Error getting AWS columns: {e}")
            return pd.DataFrame(columns=['column_name', 'data_type'])

    #>>> Process single table for comprehensive meta check <<<#
    def process_table(self, table_row: pd.Series) -> Dict:
        table_name = table_row['pcds_tbl']
        logger.info(f"Processing table: {table_name}")

        result = {
            'table_name': table_name,
            'aws_table': table_row['aws_tbl'],
            'pcds_accessible': False,
            'aws_accessible': False,
            'pcds_error': None,
            'aws_error': None,
            'date_var': table_row.get('pcds_var', ''),
            'date_type': None,
            'date_format': None,
            'vintages': [],
            'row_counts': {},
            'column_mapping': {},
            'pcds_meta': None,
            'aws_meta': None
        }

        # 1. Check accessibility
        logger.info("  Checking accessibility...")
        pcds_access = self.check_pcds_accessibility(table_row['pcds_tbl'])
        aws_access = self.check_aws_accessibility(table_row['aws_tbl'])

        result['pcds_accessible'] = pcds_access['accessible']
        result['aws_accessible'] = aws_access['accessible']
        result['pcds_error'] = pcds_access['error']
        result['aws_error'] = aws_access['error']

        if not (pcds_access['accessible'] and aws_access['accessible']):
            logger.warning(f"  Skipping {table_name} due to accessibility issues")
            return result

        # 2. Auto-detect date type and format
        pcds_var = table_row.get('pcds_var', '')
        if pcds_var and pcds_var != 'NaT':
            logger.info(f"  Detecting date type for {pcds_var}...")
            service_name = table_row['pcds_tbl'].split('.', 1)[0]
            date_type, date_format = self.detect_date_type(
                table_row['pcds_tbl'], pcds_var, service_name
            )
            result['date_type'] = date_type
            result['date_format'] = date_format
            logger.info(f"  Detected: type={date_type}, format={date_format}")
        else:
            date_type, date_format = None, None

        # 3. Get row counts by date
        logger.info("  Getting row counts by date...")
        pcds_counts = self.get_pcds_row_counts(table_row.to_dict(), date_type, date_format)
        aws_counts = self.get_aws_row_counts(table_row.to_dict(), date_type, date_format)

        # Compare counts
        pcds_dates = set(pcds_counts['counts'].keys())
        aws_dates = set(aws_counts['counts'].keys())
        common_dates = pcds_dates & aws_dates

        mismatched_dates = []
        for date in common_dates:
            if pcds_counts['counts'][date] != aws_counts['counts'][date]:
                mismatched_dates.append({
                    'date': date,
                    'pcds_count': pcds_counts['counts'][date],
                    'aws_count': aws_counts['counts'][date],
                    'diff': pcds_counts['counts'][date] - aws_counts['counts'][date]
                })

        result['row_counts'] = {
            'pcds': pcds_counts,
            'aws': aws_counts,
            'pcds_only_dates': sorted(list(pcds_dates - aws_dates)),
            'aws_only_dates': sorted(list(aws_dates - pcds_dates)),
            'mismatched_dates': mismatched_dates
        }

        # 4. Get column metadata with data types
        logger.info("  Getting column metadata...")
        pcds_meta = self.get_pcds_columns(table_row['pcds_tbl'])
        aws_meta = self.get_aws_columns(table_row['aws_tbl'])

        result['pcds_meta'] = pcds_meta
        result['aws_meta'] = aws_meta

        # 5. Build column mapping
        logger.info("  Building column mapping...")
        column_mappings = get_column_mappings(self.category)
        table_col_map = table_name.split('.')[-1].lower()
        table_crosswalk = column_mappings[column_mappings['col_map'] == table_col_map]

        mapping = self.build_column_mapping(table_crosswalk, pcds_meta, aws_meta)
        result['column_mapping'] = mapping

        # 6. Get vintages as ranges (for later use in column_check and hash_check)
        if pcds_var and date_type:
            logger.info("  Getting vintages as date ranges...")
            try:
                service_name = table_row['pcds_tbl'].split('.', 1)[0]
                partition_type = table_row.get('partition', 'year')
                pcds_where = table_row.get('pcds_where') or '1=1'

                vintages = self.get_vintages_as_ranges(
                    table_row['pcds_tbl'],
                    pcds_var,
                    date_type,
                    date_format,
                    partition_type,
                    service_name,
                    pcds_where
                )
                result['vintages'] = vintages
                logger.info(f"  Found {len(vintages)} vintages")
            except Exception as e:
                logger.error(f"  Error getting vintages: {e}")
                result['vintages'] = []
        else:
            result['vintages'] = []

        logger.info(f"  ✓ Completed {table_name}")
        return result

    #>>> Run meta check for all tables <<<#
    def run(self):
        logger.info(f"Starting Meta Check for run: {self.run_name}, category: {self.category}")

        # Get input tables
        tables_df = get_input_tables(self.category)
        logger.info(f"Found {len(tables_df)} tables to process")

        # Process each table
        for idx, row in tables_df.iterrows():
            table_result = self.process_table(row)
            self.results[table_result['table_name']] = table_result

            # Save intermediate results to S3
            self.s3.upload_json(
                table_result,
                'meta_check',
                f"{table_result['table_name'].replace('.', '_')}_meta.json"
            )

        # Generate Excel report
        logger.info("Generating Excel report...")
        self.generate_excel_report()

        # Save final results
        self.s3.upload_json(self.results, 'meta_check', 'meta_check_results.json')

        logger.info("✓ Meta Check completed!")
        return self.results

    #>>> Generate Excel report for meta check results <<<#
    def generate_excel_report(self):
        output_path = f"output_{self.run_name}_meta_check.xlsx"

        with ExcelReporter(output_path) as reporter:
            # Prepare summary data
            summary_rows = []
            for table_name, result in self.results.items():
                col_map = result['column_mapping']
                summary_rows.append([
                    table_name.split('.')[-1],
                    '✓' if result['pcds_accessible'] else '✗',
                    '✓' if result['aws_accessible'] else '✗',
                    result['row_counts'].get('pcds', {}).get('total', 0),
                    result['row_counts'].get('aws', {}).get('total', 0),
                    len(result['row_counts'].get('mismatched_dates', [])),
                    len(col_map.get('comparable', {})),
                    len(col_map.get('tokenized', [])),
                    len(col_map.get('pcds_only', [])),
                    len(col_map.get('aws_only', [])),
                    len(col_map.get('type_mismatches', []))
                ])

            reporter.create_summary_sheet(
                title='Meta Check Summary',
                headers=['Table', 'PCDS Access', 'AWS Access', 'PCDS Rows', 'AWS Rows',
                        'Date Mismatches', 'Comparable Cols', 'Tokenized Cols',
                        'PCDS Only', 'AWS Only', 'Type Mismatches'],
                data_rows=summary_rows
            )

            # Create detail sheets for each table
            for table_name, result in self.results.items():
                sections = self._prepare_table_sections(table_name, result)
                reporter.create_detail_sheet(table_name.split('.')[-1], sections)

        # Upload Excel to S3
        self.s3.upload_file(output_path, 'meta_check', output_path)
        logger.info(f"Excel report uploaded: {output_path}")

    #>>> Prepare sections for table detail sheet <<<#
    def _prepare_table_sections(self, table_name: str, result: Dict) -> List[Dict]:
        sections = []

        # Section 1: Table Info
        sections.append({
            'title': 'Table Information',
            'rows': [
                ['PCDS Table:', result['table_name']],
                ['AWS Table:', result['aws_table']],
                ['Date Variable:', result.get('date_var', 'N/A')],
                ['Date Type:', result.get('date_type', 'N/A')],
                ['Date Format:', result.get('date_format', 'N/A')],
                ['PCDS Accessible:', '✓' if result['pcds_accessible'] else f"✗ {result['pcds_error']}"],
                ['AWS Accessible:', '✓' if result['aws_accessible'] else f"✗ {result['aws_error']}"],
                ['PCDS Total Rows:', result['row_counts'].get('pcds', {}).get('total', 0)],
                ['AWS Total Rows:', result['row_counts'].get('aws', {}).get('total', 0)],
                ['Vintages:', len(result.get('vintages', []))]
            ]
        })

        # Section 2: Row Count Mismatches by Date
        mismatched = result['row_counts'].get('mismatched_dates', [])
        if mismatched:
            sections.append({
                'title': 'Row Count Mismatches by Date',
                'dataframe': pd.DataFrame(mismatched)
            })

        # Section 3: Vintage Ranges
        if result.get('vintages'):
            sections.append({
                'title': f"Vintage Ranges ({len(result['vintages'])})",
                'dataframe': pd.DataFrame(result['vintages'])
            })

        # Section 4: Column Classification
        col_map = result['column_mapping']
        if col_map:
            comparable_data = [{'PCDS': k, 'AWS': v} for k, v in col_map.get('comparable', {}).items()]
            if comparable_data:
                sections.append({
                    'title': f"Comparable Columns ({len(comparable_data)})",
                    'dataframe': pd.DataFrame(comparable_data)
                })

            if col_map.get('tokenized'):
                sections.append({
                    'title': f"Tokenized Columns ({len(col_map['tokenized'])})",
                    'rows': [[col] for col in col_map['tokenized']]
                })

            if col_map.get('pcds_only'):
                sections.append({
                    'title': f"PCDS Only Columns - Manual Review ({len(col_map['pcds_only'])})",
                    'rows': [[col] for col in col_map['pcds_only']]
                })

            if col_map.get('aws_only'):
                sections.append({
                    'title': f"AWS Only Columns - Manual Review ({len(col_map['aws_only'])})",
                    'rows': [[col] for col in col_map['aws_only']]
                })

            # Section 5: Type Mismatches
            if col_map.get('type_mismatches'):
                sections.append({
                    'title': f"Data Type Mismatches ({len(col_map['type_mismatches'])})",
                    'dataframe': pd.DataFrame(col_map['type_mismatches'])
                })

        return sections


if __name__ == "__main__":
    # No argparse - environment loaded from checks/input_pcds
    checker = MetaChecker()
    results = checker.run()

    print("\n" + "="*60)
    print("Meta Check Results Summary:")
    print("="*60)
    for table_name, result in results.items():
        print(f"\n{table_name}:")
        print(f"  Date Type: {result.get('date_type', 'N/A')} (format: {result.get('date_format', 'N/A')})")
        print(f"  PCDS Accessible: {'✓' if result['pcds_accessible'] else '✗'}")
        print(f"  AWS Accessible: {'✓' if result['aws_accessible'] else '✗'}")
        print(f"  Row Count Mismatches: {len(result['row_counts'].get('mismatched_dates', []))}")
        print(f"  Vintages: {len(result.get('vintages', []))}")
        print(f"  Comparable Columns: {len(result['column_mapping'].get('comparable', {}))}")
        print(f"  Type Mismatches: {len(result['column_mapping'].get('type_mismatches', []))}")
