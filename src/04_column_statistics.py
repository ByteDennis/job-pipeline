"""
Step 4: Column statistics comparison

Linear workflow:
1. Generate .sas and .sql files
2. Prompt user to review and execute AWS queries in parallel
3. Compare PCDS results (from email) with AWS results
"""

import concurrent.futures
from pathlib import Path
from typing import Dict, List
import pandas as pd
import utils


class ColumnStatisticsGenerator:

    def __init__(self, state_file: str, output_folder: str):
        """Initialize with state file and output folder"""
        self.state_file = Path(state_file)
        self.state = utils.read_json(state_file)
        self.output_folder = Path(output_folder)
        self.output_folder.mkdir(parents=True, exist_ok=True)

    def is_numeric_type(self, data_type: str, is_oracle: bool = True) -> bool:
        """Determine if a data type is numeric (continuous) vs categorical"""
        data_type = data_type.upper() if is_oracle else data_type.lower()

        if is_oracle:
            # Oracle numeric types
            return any(t in data_type for t in ['NUMBER', 'FLOAT', 'BINARY_FLOAT', 'BINARY_DOUBLE'])
        else:
            # Athena numeric types
            return any(t in data_type for t in ['int', 'double', 'decimal', 'float', 'bigint', 'tinyint', 'smallint'])

    def generate_pcds_stats_sas(self, table_name: str, comparable_cols: Dict[str, str],
                                col_types: Dict[str, str], where_clause: str) -> str:
        """Generate SAS code to compute column statistics for PCDS"""
        table_info = self.state['tables'][table_name]['info']
        pcds_service, pcds_table = table_info['pcds_tbl'].split('.', 1)

        # Generate PROC SQL for each column
        sas_code = f"""LIBNAME proj oracle user=&PCDS_USR password=&PCDS_PWD path=&PCDS_PATH;

/* Column Statistics for {table_name} */
PROC SQL;
CREATE TABLE col_stats (
    col_name VARCHAR(128),
    col_type VARCHAR(128),
    col_count NUM,
    col_distinct NUM,
    col_max VARCHAR(4000),
    col_min VARCHAR(4000),
    col_avg NUM,
    col_std NUM,
    col_sum NUM,
    col_sum_sq NUM,
    col_freq VARCHAR(4000),
    col_missing NUM
);
QUIT;

"""

        for pcds_col, pcds_type in col_types.items():
            if pcds_col not in comparable_cols:
                continue

            is_numeric = self.is_numeric_type(pcds_type, is_oracle=True)

            # Determine column reference for timestamps
            if 'TIMESTAMP' in pcds_type.upper():
                col_ref = f'TRUNC({pcds_col})'
            else:
                col_ref = pcds_col

            if is_numeric:
                # Continuous variable - direct statistics
                sas_code += f"""
/* Continuous: {pcds_col} */
PROC SQL;
INSERT INTO col_stats
SELECT
    '{pcds_col}' AS col_name,
    '{pcds_type}' AS col_type,
    COUNT({pcds_col}) AS col_count,
    COUNT(DISTINCT {pcds_col}) AS col_distinct,
    PUT(MAX({pcds_col}), BEST32.) AS col_max,
    PUT(MIN({pcds_col}), BEST32.) AS col_min,
    AVG({pcds_col}) AS col_avg,
    STD({pcds_col}) AS col_std,
    SUM({pcds_col}) AS col_sum,
    SUM({pcds_col} * {pcds_col}) AS col_sum_sq,
    '' AS col_freq,
    SUM(CASE WHEN {pcds_col} IS NULL THEN 1 ELSE 0 END) AS col_missing
FROM proj.{pcds_table.upper()}
WHERE {where_clause};
QUIT;

"""
            else:
                # Categorical variable - frequency distribution first
                sas_code += f"""
/* Categorical: {pcds_col} */
PROC SQL;
CREATE TABLE FreqTable_RAW AS
SELECT
    {col_ref} AS p_col,
    COUNT(*) AS value_freq
FROM proj.{pcds_table.upper()}
WHERE {where_clause}
GROUP BY {col_ref};

CREATE TABLE FreqTable AS
SELECT
    p_col,
    value_freq,
    MONOTONIC() AS rn
FROM FreqTable_RAW
ORDER BY value_freq DESC, p_col ASC;

INSERT INTO col_stats
SELECT
    '{pcds_col}' AS col_name,
    '{pcds_type}' AS col_type,
    SUM(value_freq) AS col_count,
    COUNT(value_freq) AS col_distinct,
    PUT(MAX(value_freq), BEST32.) AS col_max,
    PUT(MIN(value_freq), BEST32.) AS col_min,
    AVG(value_freq) AS col_avg,
    STD(value_freq) AS col_std,
    SUM(value_freq) AS col_sum,
    SUM(value_freq * value_freq) AS col_sum_sq,
    (SELECT CATX('; ', CATS(p_col, '(', value_freq, ')')) FROM FreqTable WHERE rn <= 10) AS col_freq,
    COALESCE((SELECT value_freq FROM FreqTable WHERE p_col IS NULL), 0) AS col_missing
FROM FreqTable;

DROP TABLE FreqTable_RAW;
DROP TABLE FreqTable;
QUIT;

"""

        sas_code += f"""
/* Export results */
PROC EXPORT DATA=col_stats
    OUTFILE="/path/to/output/{table_name}_pcds_stats.csv"
    DBMS=CSV REPLACE;
RUN;

FILENAME outbox EMAIL
    TO=("user@example.com")
    SUBJECT="PCDS Stats Complete: {table_name}"
    IMPORTANCE='HIGH';

DATA _null_;
    FILE outbox;
    PUT "Statistics for {table_name} completed.";
    PUT "CSV file: {table_name}_pcds_stats.csv";
RUN;
"""
        return sas_code

    def generate_aws_stats_sql(self, table_name: str, comparable_cols: Dict[str, str],
                               col_types: Dict[str, str], where_clause: str) -> List[str]:
        """Generate AWS Athena SQL to compute column statistics (one query per column)"""
        table_info = self.state['tables'][table_name]['info']
        database, aws_table = table_info['aws_tbl'].split('.', 1)

        queries = []

        for aws_col, aws_type in col_types.items():
            if aws_col not in comparable_cols.values():
                continue

            is_numeric = self.is_numeric_type(aws_type, is_oracle=False)

            if is_numeric:
                # Continuous variable - direct statistics
                query = f"""
SELECT
    '{aws_col}' AS col_name,
    '{aws_type}' AS col_type,
    COUNT({aws_col}) AS col_count,
    COUNT(DISTINCT {aws_col}) AS col_distinct,
    MAX({aws_col}) AS col_max,
    MIN({aws_col}) AS col_min,
    AVG(CAST({aws_col} AS DOUBLE)) AS col_avg,
    STDDEV_SAMP(CAST({aws_col} AS DOUBLE)) AS col_std,
    SUM(CAST({aws_col} AS DOUBLE)) AS col_sum,
    SUM(CAST({aws_col} AS DOUBLE) * CAST({aws_col} AS DOUBLE)) AS col_sum_sq,
    '' AS col_freq,
    COUNT(*) - COUNT({aws_col}) AS col_missing
FROM {database}.{aws_table}
WHERE {where_clause}
""".strip()
            else:
                # Categorical variable - frequency distribution first
                query = f"""
WITH FreqTable_RAW AS (
    SELECT
        {aws_col} AS p_col,
        COUNT(*) AS value_freq
    FROM {database}.{aws_table}
    WHERE {where_clause}
    GROUP BY {aws_col}
), FreqTable AS (
    SELECT
        p_col,
        value_freq,
        ROW_NUMBER() OVER (ORDER BY value_freq DESC, p_col ASC) AS rn
    FROM FreqTable_RAW
)
SELECT
    '{aws_col}' AS col_name,
    '{aws_type}' AS col_type,
    SUM(value_freq) AS col_count,
    COUNT(value_freq) AS col_distinct,
    MAX(value_freq) AS col_max,
    MIN(value_freq) AS col_min,
    AVG(CAST(value_freq AS DOUBLE)) AS col_avg,
    STDDEV_SAMP(CAST(value_freq AS DOUBLE)) AS col_std,
    SUM(value_freq) AS col_sum,
    SUM(value_freq * value_freq) AS col_sum_sq,
    (SELECT ARRAY_JOIN(ARRAY_AGG(COALESCE(CAST(p_col AS VARCHAR), '') || '(' || CAST(value_freq AS VARCHAR) || ')' ORDER BY value_freq DESC), '; ') FROM FreqTable WHERE rn <= 10) AS col_freq,
    (SELECT COALESCE(value_freq, 0) FROM FreqTable WHERE p_col IS NULL) AS col_missing
FROM FreqTable
""".strip()

            queries.append(query)

        return queries

    def execute_aws_query(self, query: str, query_id: str) -> pd.DataFrame:
        """Execute single AWS query (placeholder - implement with actual AWS connection)"""
        print(f"  Executing query {query_id}...")
        # In actual implementation:
        # import pyathena
        # conn = pyathena.connect(...)
        # df = pd.read_sql(query, conn)
        # return df
        return pd.DataFrame()  # Placeholder

    def execute_all_aws_queries(self, table_name: str, queries: List[str],
                                n_parallel: int = 5) -> pd.DataFrame:
        """Execute all AWS queries in parallel"""
        print(f"Executing {len(queries)} AWS queries in parallel (max {n_parallel} at a time)...")

        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=n_parallel) as executor:
            future_to_query = {
                executor.submit(self.execute_aws_query, query, f"Q{i+1}"): i
                for i, query in enumerate(queries)
            }

            for future in concurrent.futures.as_completed(future_to_query):
                query_idx = future_to_query[future]
                try:
                    result_df = future.result()
                    results.append(result_df)
                    print(f"  ✓ Query {query_idx + 1}/{len(queries)} completed")
                except Exception as e:
                    print(f"  ✗ Query {query_idx + 1} failed: {e}")

        # Combine all results
        if results:
            combined = pd.concat(results, ignore_index=True)
            return combined
        return pd.DataFrame()

    def process_table(self, table_name: str) -> Dict:
        """Mode 1: Generate .sas and .sql files"""
        table_info = self.state['tables'][table_name]['info']
        column_mapping = self.state['tables'][table_name].get('column_mapping', {})

        if not column_mapping or column_mapping.get('status') != 'success':
            return {'status': 'no_column_mapping', 'error': 'Column mapping not available'}

        mapping = column_mapping['mapping']
        comparable_cols = mapping['comparable_columns']

        # Get column types (placeholder)
        pcds_col_types = {}
        aws_col_types = {}

        # Generate files
        pcds_where = table_info.get('pcds_where') or '1=1'
        sas_code = self.generate_pcds_stats_sas(table_name, comparable_cols, pcds_col_types, pcds_where)

        aws_where = table_info.get('aws_where') or '1=1'
        sql_queries = self.generate_aws_stats_sql(table_name, comparable_cols, aws_col_types, aws_where)

        # Write files
        sas_file = self.output_folder / f"{table_name}_pcds_stats.sas"
        sql_file = self.output_folder / f"{table_name}_aws_stats.sql"

        with open(sas_file, 'w') as f:
            f.write(sas_code)

        with open(sql_file, 'w') as f:
            for i, query in enumerate(sql_queries, 1):
                f.write(f"-- Query {i}\n{query};\n\n")

        return {
            'status': 'generated',
            'sas_file': str(sas_file),
            'sql_file': str(sql_file),
            'total_columns': len(comparable_cols)
        }

    def mode_execute_aws(self, table_name: str, n_parallel: int = 5) -> Dict:
        """Mode 2: Execute AWS queries in parallel"""
        sql_file = self.output_folder / f"{table_name}_aws_stats.sql"

        if not sql_file.exists():
            return {'status': 'error', 'error': 'SQL file not found. Run --mode generate first.'}

        # Load and parse SQL file
        with open(sql_file, 'r') as f:
            content = f.read()

        # Split by semicolon and filter empty queries
        queries = [q.strip() for q in content.split(';') if q.strip() and not q.strip().startswith('--')]

        # Execute queries in parallel
        aws_results = self.execute_all_aws_queries(table_name, queries, n_parallel)

        # Save results
        aws_results_file = self.output_folder / f"{table_name}_aws_stats_results.csv"
        aws_results.to_csv(aws_results_file, index=False)

        return {
            'status': 'executed',
            'queries_executed': len(queries),
            'results_file': str(aws_results_file)
        }

    def mode_compare(self, table_name: str, pcds_results_csv: str) -> Dict:
        """Mode 3: Load PCDS results and compare with AWS results"""
        aws_results_file = self.output_folder / f"{table_name}_aws_stats_results.csv"

        if not Path(pcds_results_csv).exists():
            return {'status': 'error', 'error': f'PCDS results not found: {pcds_results_csv}'}

        if not aws_results_file.exists():
            return {'status': 'error', 'error': 'AWS results not found. Run --mode execute_aws first.'}

        # Load results
        pcds_df = pd.read_csv(pcds_results_csv)
        aws_df = pd.read_csv(aws_results_file)

        # Compare
        merged = pd.merge(pcds_df, aws_df, on='col_name', suffixes=('_pcds', '_aws'))

        mismatches = []
        for _, row in merged.iterrows():
            issues = []
            if row.get('col_count_pcds') != row.get('col_count_aws'):
                issues.append(f"count: {row['col_count_pcds']} vs {row['col_count_aws']}")
            if row.get('col_distinct_pcds') != row.get('col_distinct_aws'):
                issues.append(f"distinct: {row['col_distinct_pcds']} vs {row['col_distinct_aws']}")

            if issues:
                mismatches.append({'column': row['col_name'], 'issues': issues})

        return {
            'status': 'compared',
            'total_columns': len(merged),
            'matched': len(merged) - len(mismatches),
            'mismatched': len(mismatches),
            'mismatches': mismatches
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

    # Check if step 3 is complete
    state = utils.read_json(str(state_file))
    if 'step3_column_mapping' not in state.get('global_steps_completed', []):
        raise RuntimeError("Step 3 not completed. Please run 03_column_mapping.py first")

    # Check if this step is already complete
    if 'step4_statistics' in state.get('global_steps_completed', []):
        print(f"⚠ Step 4 already completed for run: {run_name}")
        print("  Delete the step results to re-run, or continue to step 5")
        return state['tables']

    print(f"Starting Step 4: Column Statistics for run: {run_name}")
    print(f"{'='*60}\n")

    generator = ColumnStatisticsGenerator(str(state_file), str(output_folder))
    tables_to_process = list(generator.state['tables'].keys())

    # Step 4a: Generate .sas and .sql files
    print("Step 4a: Generating SAS and SQL files...")
    for table_name in tables_to_process:
        sas_file = output_folder / f"{table_name}_pcds_stats.sas"
        sql_file = output_folder / f"{table_name}_aws_stats.sql"

        if sas_file.exists() and sql_file.exists():
            print(f"  ✓ {table_name}: Files already exist, skipping")
            continue

        result = generator.process_table(table_name)
        if result['status'] == 'generated':
            print(f"  ✓ {table_name}: Generated {result['total_columns']} column queries")

    # Step 4b: Prompt user to review and execute AWS queries
    print(f"\n{'='*60}")
    print("Step 4b: AWS Query Execution")
    print(f"{'='*60}")
    print(f"SQL files have been generated in: {output_folder}")
    print("Please review the SQL files before execution.")
    response = input("Do you want to execute AWS queries now? (Y/Yes to continue): ").strip()

    if response.lower() not in ['y', 'yes']:
        print("\n⚠ AWS execution skipped.")
        print(f"  To execute later, run this script again: python 04_column_statistics.py {run_name}")
        return {}

    # Execute AWS queries in parallel
    print("\nExecuting AWS queries in parallel...")
    for table_name in tables_to_process:
        result_file = output_folder / f"{table_name}_aws_stats_results.csv"
        if result_file.exists():
            print(f"  ✓ {table_name}: Results already exist, skipping")
            continue

        result = generator.mode_execute_aws(table_name, n_parallel=5)
        if result['status'] == 'executed':
            print(f"  ✓ {table_name}: Executed {result['queries_executed']} queries")

    # Step 4c: Check for PCDS results and compare
    print(f"\n{'='*60}")
    print("Step 4c: Comparing Results")
    print(f"{'='*60}")

    comparison_results = {}
    for table_name in tables_to_process:
        pcds_file = output_folder / f"{table_name}_pcds_stats.csv"
        aws_file = output_folder / f"{table_name}_aws_stats_results.csv"

        if not pcds_file.exists():
            print(f"  ⚠ {table_name}: PCDS results not found")
            print(f"    Please run the SAS file and save results to: {pcds_file}")
            continue

        if not aws_file.exists():
            print(f"  ⚠ {table_name}: AWS results not found (should have been generated)")
            continue

        result = generator.mode_compare(table_name, str(pcds_file))
        comparison_results[table_name] = result

        if result['status'] == 'compared':
            print(f"  ✓ {table_name}: Matched {result['matched']}/{result['total_columns']} columns")
            if result['mismatched'] > 0:
                print(f"    ⚠ {result['mismatched']} mismatches found")

        # Update state
        generator.state['tables'][table_name]['statistics'] = result

    # Mark step complete if all tables have been compared
    if comparison_results and all(r.get('status') == 'compared' for r in comparison_results.values()):
        generator.state['tables'][table_name]['steps_completed'].append('step4_statistics')
        if 'step4_statistics' not in generator.state['global_steps_completed']:
            generator.state['global_steps_completed'].append('step4_statistics')

    # Save state
    utils.write_json(generator.state_file, generator.state)

    print(f"\n✓ Step 4 complete. Results saved to: {output_folder}")
    print(f"  Next: python 05_value_to_value_check.py {run_name}")

    return comparison_results


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        print("Usage: python 04_column_statistics.py <run_name>")
        print("Example: python 04_column_statistics.py run_20250122")
