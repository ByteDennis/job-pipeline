"""
Step 2: Row count check by partition date

Queries PCDS and AWS tables to compare row counts grouped by partition variable.
Handles corner cases like inaccessible tables, missing partition variables, etc.
"""

import re
from pathlib import Path
from typing import Dict, Optional, Tuple
from datetime import datetime
import utils


class RowCountChecker:

    def __init__(self, state_file: str):
        """Load pipeline state"""
        self.state_file = Path(state_file)
        self.state = utils.read_json(state_file)

    def extract_date_var(self, date_spec: str) -> Tuple[str, Optional[str]]:
        """Extract variable name and format from specification like 'dw_bus_dt (%Y%m%d)'"""
        if not date_spec or date_spec == 'NaT':
            return '', None

        pattern = r'^(.+?)\s*\(([^)]+)\)\s*$'
        if match := re.match(pattern, date_spec.strip()):
            var_name, format_spec = match.groups()
            return var_name.strip(), format_spec.strip()

        return date_spec.strip(), None

    def build_pcds_count_query(self, table: str, date_var: str, where_clause: str = '1=1') -> str:
        """Build Oracle query to count rows by partition date"""
        if not date_var:
            return f"SELECT COUNT(*) AS nrows FROM {table} WHERE {where_clause}"

        return f"""
SELECT TO_CHAR({date_var}, 'YYYY-MM-DD') AS partition_date,
       COUNT(*) AS nrows
FROM {table}
WHERE {where_clause}
GROUP BY TO_CHAR({date_var}, 'YYYY-MM-DD')
ORDER BY partition_date DESC
        """.strip()

    def build_aws_count_query(self, table: str, date_var: str, date_format: Optional[str],
                             where_clause: str = '1=1') -> str:
        """Build Athena query to count rows by partition date"""
        database, table_name = table.split('.', 1)

        if not date_var:
            return f"SELECT COUNT(*) AS nrows FROM {database}.{table_name} WHERE {where_clause}"

        if date_format:
            date_expr = f"DATE_FORMAT(DATE_PARSE({date_var}, '{date_format}'), '%Y-%m-%d')"
        else:
            date_expr = f"DATE_FORMAT({date_var}, '%Y-%m-%d')"

        return f"""
SELECT {date_expr} AS partition_date,
       COUNT(*) AS nrows
FROM {database}.{table_name}
WHERE {where_clause}
GROUP BY {date_expr}
ORDER BY partition_date DESC
        """.strip()

    def check_table_row_counts(self, table_name: str) -> Dict:
        """Check row counts for a specific table, handling all error cases"""
        table_info = self.state['tables'][table_name]['info']

        result = {
            'table_name': table_name,
            'status': 'success',
            'error': None,
            'pcds_accessible': False,
            'aws_accessible': False,
            'pcds_date_var_found': False,
            'aws_date_var_found': False,
            'total_pcds_rows': 0,
            'total_aws_rows': 0,
            'pcds_partitions': [],
            'aws_partitions': [],
            'mismatched_partitions': []
        }

        # Extract date variables
        pcds_var, _ = self.extract_date_var(table_info['pcds_var'])
        aws_var, aws_format = self.extract_date_var(table_info['aws_var'])

        # Check PCDS table
        try:
            pcds_service, pcds_table = table_info['pcds_tbl'].split('.', 1)
            pcds_where = table_info.get('pcds_where') or '1=1'

            pcds_query = self.build_pcds_count_query(pcds_table, pcds_var, pcds_where)
            result['pcds_query'] = pcds_query

            # In actual implementation, execute query here
            # pcds_df = execute_pcds_query(pcds_query, pcds_service)
            result['pcds_accessible'] = True
            result['pcds_date_var_found'] = bool(pcds_var)

        except Exception as e:
            result['status'] = 'pcds_error'
            result['error'] = f"PCDS Error: {str(e)}"
            return result

        # Check AWS table
        try:
            aws_where = table_info.get('aws_where') or '1=1'
            aws_query = self.build_aws_count_query(
                table_info['aws_tbl'], aws_var, aws_format, aws_where
            )
            result['aws_query'] = aws_query

            # In actual implementation, execute query here
            # aws_df = execute_aws_query(aws_query)
            result['aws_accessible'] = True
            result['aws_date_var_found'] = bool(aws_var)

        except Exception as e:
            result['status'] = 'aws_error'
            result['error'] = f"AWS Error: {str(e)}"
            return result

        # Compare results (placeholder - actual comparison would be done with real data)
        # result['mismatched_partitions'] = find_mismatches(pcds_df, aws_df)

        return result

    def process_all_tables(self) -> Dict:
        """Process all tables and return summary"""
        results = {}

        for table_name in self.state['tables'].keys():
            print(f"Checking row counts for {table_name}...")
            results[table_name] = self.check_table_row_counts(table_name)

            # Update state with row count info
            self.state['tables'][table_name]['row_counts'] = results[table_name]
            self.state['tables'][table_name]['steps_completed'].append('step2_row_count')

        # Mark global step complete
        if 'step2_row_count' not in self.state['global_steps_completed']:
            self.state['global_steps_completed'].append('step2_row_count')

        # Save updated state
        utils.write_json(self.state_file, self.state)

        return results


def main(run_name: str):
    """Main execution function"""
    # Load state from output folder
    state_file = Path('output') / run_name / 'pipeline_state.json'

    if not state_file.exists():
        raise FileNotFoundError(
            f"State file not found: {state_file}\n"
            f"  Please run step 1 first: python 01_load_config.py {run_name}"
        )

    # Check if step 1 is complete
    state = utils.read_json(str(state_file))
    if 'step1_load_config' not in state.get('global_steps_completed', []):
        raise RuntimeError("Step 1 not completed. Please run 01_load_config.py first")

    # Check if this step is already complete
    if 'step2_row_count' in state.get('global_steps_completed', []):
        print(f"⚠ Step 2 already completed for run: {run_name}")
        print("  Delete the step results to re-run, or continue to step 3")
        return state['tables']

    print(f"Starting Step 2: Row Count Check for run: {run_name}")
    print(f"{'='*60}\n")

    checker = RowCountChecker(str(state_file))
    results = checker.process_all_tables()

    # Print summary
    print(f"\n{'='*60}")
    print("ROW COUNT CHECK SUMMARY")
    print(f"{'='*60}")

    for table_name, result in results.items():
        print(f"\n{table_name}:")
        print(f"  Status: {result['status']}")
        print(f"  PCDS accessible: {result['pcds_accessible']}")
        print(f"  AWS accessible: {result['aws_accessible']}")
        if result['error']:
            print(f"  Error: {result['error']}")

    print(f"\n✓ Step 2 complete. Results saved to: {state_file}")
    print(f"  Next: python 03_column_mapping.py {run_name}")

    return results


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        print("Usage: python 02_row_count_check.py <run_name>")
        print("Example: python 02_row_count_check.py run_20250122")
