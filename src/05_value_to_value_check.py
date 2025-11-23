"""
Step 5: Value-to-value hash comparison

Linear workflow:
1. Generate .sas and .sql files with hash queries (with column batching)
2. Prompt user to review and execute AWS hash queries in parallel
3. Compare PCDS hashes (from email) with AWS hashes
"""

import concurrent.futures
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd
import utils


class HashComparator:

    def __init__(self, state_file: str, output_folder: str, max_cols_per_batch: int = 20):
        """Initialize with state file and output folder"""
        self.state_file = Path(state_file)
        self.state = utils.read_json(state_file)
        self.output_folder = Path(output_folder)
        self.output_folder.mkdir(parents=True, exist_ok=True)
        self.max_cols_per_batch = max_cols_per_batch

    def batch_columns(self, columns: List[str], max_per_batch: int) -> List[List[str]]:
        """Split columns into batches to avoid query length limits"""
        batches = []
        for i in range(0, len(columns), max_per_batch):
            batches.append(columns[i:i + max_per_batch])
        return batches

    def generate_pcds_hash_sas(self, table_name: str, batch_num: int,
                               comparable_cols: Dict[str, str],
                               col_types: Dict[str, str],
                               where_clause: str,
                               key_column: Optional[str] = None) -> str:
        """Generate SAS code to compute row hashes for PCDS"""
        table_info = self.state['tables'][table_name]['info']
        pcds_service, pcds_table = table_info['pcds_tbl'].split('.', 1)

        # Build column list for hash computation
        col_specs = [
            {'column_name': col, 'data_type': col_types.get(col, 'VARCHAR2')}
            for col in comparable_cols.keys()
        ]

        # Generate hash expression using utils
        hash_expr = utils.build_oracle_hash_expr(col_specs)

        # Determine key column
        if not key_column:
            key_column = list(comparable_cols.keys())[0] if comparable_cols else 'ROWNUM'

        sas_template = f"""
LIBNAME proj oracle user=&PCDS_USR password=&PCDS_PWD path=&PCDS_PATH;

DATA row_hashes_batch_{batch_num};
    SET proj.{pcds_table.upper()}(WHERE=({where_clause}));
    row_hash = {hash_expr};
    KEEP {key_column} row_hash;
RUN;

PROC SORT DATA=row_hashes_batch_{batch_num};
    BY {key_column};
RUN;

PROC EXPORT DATA=row_hashes_batch_{batch_num}
    OUTFILE="/path/to/output/{table_name}_pcds_hash_batch_{batch_num}.csv"
    DBMS=CSV REPLACE;
RUN;

FILENAME outbox EMAIL
    TO=("user@example.com")
    SUBJECT="PCDS Hash Complete: {table_name} Batch {batch_num}"
    IMPORTANCE='HIGH';

DATA _null_;
    FILE outbox;
    PUT "Row hashes for {table_name} (batch {batch_num}) completed.";
    PUT "Columns: {', '.join(comparable_cols.keys())}";
RUN;
"""
        return sas_template

    def generate_aws_hash_sql(self, table_name: str, batch_num: int,
                             comparable_cols: Dict[str, str],
                             col_types: Dict[str, str],
                             where_clause: str,
                             key_column: Optional[str] = None) -> str:
        """Generate AWS Athena SQL to compute row hashes"""
        table_info = self.state['tables'][table_name]['info']
        database, aws_table = table_info['aws_tbl'].split('.', 1)

        # Build column list for hash computation (using AWS column names)
        col_specs = [
            {'column_name': aws_col, 'data_type': col_types.get(aws_col, 'string')}
            for aws_col in comparable_cols.values()
        ]

        # Generate hash expression using utils
        hash_expr = utils.build_athena_hash_expr(col_specs)

        # Determine key column
        if not key_column:
            key_column = list(comparable_cols.values())[0] if comparable_cols else 'row_number() OVER ()'

        query = f"""SELECT {key_column} AS key_col, {hash_expr} AS row_hash FROM {database}.{aws_table} WHERE {where_clause} ORDER BY {key_column}"""
        return query

    def execute_aws_hash_query(self, query: str, batch_num: int) -> pd.DataFrame:
        """Execute single AWS hash query (placeholder)"""
        print(f"  Executing batch {batch_num}...")
        # In actual implementation:
        # import pyathena
        # conn = pyathena.connect(...)
        # df = pd.read_sql(query, conn)
        # return df
        return pd.DataFrame()  # Placeholder

    def execute_all_aws_hash_queries(self, table_name: str, queries: List[Tuple[int, str]],
                                     n_parallel: int = 3) -> Dict[int, pd.DataFrame]:
        """Execute all AWS hash queries in parallel"""
        print(f"Executing {len(queries)} hash queries in parallel (max {n_parallel} at a time)...")

        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=n_parallel) as executor:
            future_to_batch = {
                executor.submit(self.execute_aws_hash_query, query, batch_num): batch_num
                for batch_num, query in queries
            }

            for future in concurrent.futures.as_completed(future_to_batch):
                batch_num = future_to_batch[future]
                try:
                    result_df = future.result()
                    results[batch_num] = result_df
                    print(f"  ✓ Batch {batch_num}/{len(queries)} completed")
                except Exception as e:
                    print(f"  ✗ Batch {batch_num} failed: {e}")
                    results[batch_num] = pd.DataFrame()

        return results

    def process_table(self, table_name: str, sample_columns: Optional[List[str]] = None) -> Dict:
        """Mode 1: Generate .sas and .sql files"""
        table_info = self.state['tables'][table_name]['info']
        column_mapping = self.state['tables'][table_name].get('column_mapping', {})

        if not column_mapping or column_mapping.get('status') != 'success':
            return {'status': 'no_column_mapping', 'error': 'Column mapping not available'}

        mapping = column_mapping['mapping']
        comparable_cols = mapping['comparable_columns']

        # Filter for sample columns if specified
        if sample_columns:
            comparable_cols = {k: v for k, v in comparable_cols.items() if k in sample_columns}

        if not comparable_cols:
            return {'status': 'no_comparable_columns', 'error': 'No comparable columns found'}

        # Get column types (placeholder)
        pcds_col_types = {}
        aws_col_types = {}

        # Batch columns
        pcds_cols_list = list(comparable_cols.keys())
        batches = self.batch_columns(pcds_cols_list, self.max_cols_per_batch)

        generated_files = []

        for batch_num, batch_cols in enumerate(batches, start=1):
            batch_mapping = {k: comparable_cols[k] for k in batch_cols}

            # Generate PCDS SAS code
            pcds_where = table_info.get('pcds_where') or '1=1'
            sas_code = self.generate_pcds_hash_sas(
                table_name, batch_num, batch_mapping, pcds_col_types, pcds_where
            )

            # Generate AWS SQL
            aws_where = table_info.get('aws_where') or '1=1'
            sql_query = self.generate_aws_hash_sql(
                table_name, batch_num, batch_mapping, aws_col_types, aws_where
            )

            # Write to files
            sas_file = self.output_folder / f"{table_name}_pcds_hash_batch_{batch_num}.sas"
            sql_file = self.output_folder / f"{table_name}_aws_hash_batch_{batch_num}.sql"

            with open(sas_file, 'w') as f:
                f.write(sas_code)

            with open(sql_file, 'w') as f:
                f.write(sql_query)

            generated_files.append({
                'batch': batch_num,
                'sas_file': str(sas_file),
                'sql_file': str(sql_file),
                'columns': list(batch_mapping.keys()),
                'column_count': len(batch_mapping)
            })

        return {
            'status': 'generated',
            'total_columns': len(comparable_cols),
            'total_batches': len(batches),
            'batches': generated_files
        }

    def mode_execute_aws(self, table_name: str, n_parallel: int = 3) -> Dict:
        """Mode 2: Execute AWS hash queries in parallel"""
        # Find all SQL batch files
        sql_files = sorted(self.output_folder.glob(f"{table_name}_aws_hash_batch_*.sql"))

        if not sql_files:
            return {'status': 'error', 'error': 'No SQL batch files found. Run --mode generate first.'}

        queries = []
        for sql_file in sql_files:
            batch_num = int(sql_file.stem.split('_')[-1])
            with open(sql_file, 'r') as f:
                query = f.read().strip()
            queries.append((batch_num, query))

        # Execute queries in parallel
        batch_results = self.execute_all_aws_hash_queries(table_name, queries, n_parallel)

        # Save each batch result
        saved_files = []
        for batch_num, result_df in batch_results.items():
            result_file = self.output_folder / f"{table_name}_aws_hash_batch_{batch_num}_results.csv"
            result_df.to_csv(result_file, index=False)
            saved_files.append(str(result_file))

        return {
            'status': 'executed',
            'batches_executed': len(batch_results),
            'results_files': saved_files
        }

    def mode_compare(self, table_name: str, pcds_results_folder: str) -> Dict:
        """Mode 3: Load PCDS results and compare with AWS results"""
        pcds_folder = Path(pcds_results_folder)

        # Find all PCDS and AWS result files
        pcds_files = sorted(pcds_folder.glob(f"{table_name}_pcds_hash_batch_*_results.csv"))
        aws_files = sorted(self.output_folder.glob(f"{table_name}_aws_hash_batch_*_results.csv"))

        if not pcds_files:
            return {'status': 'error', 'error': f'No PCDS results found in {pcds_results_folder}'}

        if not aws_files:
            return {'status': 'error', 'error': 'No AWS results found. Run --mode execute_aws first.'}

        all_mismatches = []
        total_pcds = 0
        total_aws = 0
        total_matched = 0

        # Compare each batch
        for pcds_file, aws_file in zip(pcds_files, aws_files):
            batch_num = int(pcds_file.stem.split('_')[-2])

            pcds_df = pd.read_csv(pcds_file)
            aws_df = pd.read_csv(aws_file)

            # Merge on key column
            merged = pd.merge(
                pcds_df, aws_df,
                on='key_col',
                suffixes=('_pcds', '_aws'),
                how='outer',
                indicator=True
            )

            # Find mismatches
            both = merged[merged['_merge'] == 'both']
            hash_mismatches = both[both['row_hash_pcds'] != both['row_hash_aws']]['key_col'].tolist()

            total_pcds += len(pcds_df)
            total_aws += len(aws_df)
            total_matched += len(both) - len(hash_mismatches)

            if hash_mismatches:
                all_mismatches.extend([
                    {'batch': batch_num, 'key': key} for key in hash_mismatches[:50]
                ])

        return {
            'status': 'compared',
            'total_pcds_rows': total_pcds,
            'total_aws_rows': total_aws,
            'matched_hashes': total_matched,
            'mismatched_hashes': len(all_mismatches),
            'mismatch_sample': all_mismatches[:100]
        }


def main(run_name: str):
    """Main execution function"""
    # Load state from output folder
    output_folder = Path('output') / run_name
    state_file = output_folder / 'pipeline_state.json'

    if not state_file.exists():
        raise FileNotFoundError(
            f"State file not found: {state_file}\n"
            f"  Please run step 1 first: python 01_load_config.py {run_name}"
        )

    # Check if step 4 is complete
    state = utils.read_json(str(state_file))
    if 'step4_statistics' not in state.get('global_steps_completed', []):
        raise RuntimeError("Step 4 not completed. Please run 04_column_statistics.py first")

    # Check if this step is already complete
    if 'step5_hash_comparison' in state.get('global_steps_completed', []):
        print(f"⚠ Step 5 already completed for run: {run_name}")
        print("  Delete the step results to re-run")
        return state['tables']

    print(f"Starting Step 5: Value-to-Value Hash Comparison for run: {run_name}")
    print(f"{'='*60}\n")

    comparator = HashComparator(str(state_file), str(output_folder), max_cols_per_batch=20)
    tables_to_process = list(comparator.state['tables'].keys())

    # Step 5a: Generate .sas and .sql files with batching
    print("Step 5a: Generating hash query files (with column batching)...")
    for table_name in tables_to_process:
        # Check if batch files already exist
        batch_files = list(output_folder.glob(f"{table_name}_*_hash_batch_*.sas"))
        if batch_files:
            print(f"  ✓ {table_name}: Batch files already exist, skipping")
            continue

        result = comparator.process_table(table_name)
        if result['status'] == 'generated':
            print(f"  ✓ {table_name}: Generated {result['total_batches']} batches ({result['total_columns']} columns)")

    # Step 5b: Prompt user to review and execute AWS queries
    print(f"\n{'='*60}")
    print("Step 5b: AWS Hash Query Execution")
    print(f"{'='*60}")
    print(f"SQL files have been generated in: {output_folder}")
    print("Please review the SQL files before execution.")
    response = input("Do you want to execute AWS hash queries now? (Y/Yes to continue): ").strip()

    if response.lower() not in ['y', 'yes']:
        print("\n⚠ AWS execution skipped.")
        print(f"  To execute later, run this script again: python 05_value_to_value_check.py {run_name}")
        return {}

    # Execute AWS queries in parallel
    print("\nExecuting AWS hash queries in parallel...")
    for table_name in tables_to_process:
        # Check if AWS results already exist
        result_files = list(output_folder.glob(f"{table_name}_aws_hash_batch_*_results.csv"))
        if result_files:
            print(f"  ✓ {table_name}: AWS results already exist, skipping")
            continue

        result = comparator.mode_execute_aws(table_name, n_parallel=3)
        if result['status'] == 'executed':
            print(f"  ✓ {table_name}: Executed {result['batches_executed']} batches")

    # Step 5c: Check for PCDS results and compare
    print(f"\n{'='*60}")
    print("Step 5c: Comparing Hash Results")
    print(f"{'='*60}")

    comparison_results = {}
    for table_name in tables_to_process:
        # Find all batch files
        pcds_batch_files = sorted(output_folder.glob(f"{table_name}_pcds_hash_batch_*_results.csv"))
        aws_batch_files = sorted(output_folder.glob(f"{table_name}_aws_hash_batch_*_results.csv"))

        if not pcds_batch_files:
            print(f"  ⚠ {table_name}: PCDS hash results not found")
            print(f"    Please run the SAS files and save results to: {output_folder}")
            continue

        if not aws_batch_files:
            print(f"  ⚠ {table_name}: AWS hash results not found (should have been generated)")
            continue

        # Compare results
        result = comparator.mode_compare(table_name, str(output_folder))
        comparison_results[table_name] = result

        if result['status'] == 'compared':
            print(f"  ✓ {table_name}: Compared {result['total_pcds_rows']} PCDS rows vs {result['total_aws_rows']} AWS rows")
            print(f"    Matched: {result['matched_hashes']}, Mismatched: {result['mismatched_hashes']}")
            if result['mismatched_hashes'] > 0:
                print(f"    ⚠ Found {len(result['mismatch_sample'])} sample mismatches")

        # Update state
        if 'hash_comparison' not in comparator.state['tables'][table_name]:
            comparator.state['tables'][table_name]['hash_comparison'] = {}
        comparator.state['tables'][table_name]['hash_comparison'] = result

    # Mark step complete if all tables have been compared
    if comparison_results and all(r.get('status') == 'compared' for r in comparison_results.values()):
        for table_name in tables_to_process:
            if 'step5_hash_comparison' not in comparator.state['tables'][table_name]['steps_completed']:
                comparator.state['tables'][table_name]['steps_completed'].append('step5_hash_comparison')

        if 'step5_hash_comparison' not in comparator.state['global_steps_completed']:
            comparator.state['global_steps_completed'].append('step5_hash_comparison')

    # Save state
    utils.write_json(comparator.state_file, comparator.state)

    print(f"\n✓ Step 5 complete. Results saved to: {output_folder}")
    print("  All pipeline steps completed!")

    return comparison_results


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        print("Usage: python 05_value_to_value_check.py <run_name>")
        print("Example: python 05_value_to_value_check.py run_20250122")
