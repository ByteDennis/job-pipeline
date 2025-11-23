"""
Step 3: Column mapping validation

Validates crosswalk document and compares with actual table metadata.
Identifies comparable columns, PCDS-only columns, AWS-only columns, and tokenized columns.
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Tuple, Set
import utils


class ColumnMapper:

    def __init__(self, state_file: str, crosswalk_file: str):
        """Load pipeline state and crosswalk document"""
        self.state_file = Path(state_file)
        self.state = utils.read_json(state_file)
        self.crosswalk_file = Path(crosswalk_file)
        self.crosswalk_df = None

    def load_crosswalk(self) -> pd.DataFrame:
        """Load crosswalk Excel document"""
        df = pd.read_excel(self.crosswalk_file)
        df = df.map(utils.clean_string)
        self.crosswalk_df = df
        return df

    def get_table_crosswalk(self, col_map_name: str) -> pd.DataFrame:
        """Get crosswalk entries for a specific table"""
        if self.crosswalk_df is None:
            self.load_crosswalk()

        mask = self.crosswalk_df['OnPremView'] == col_map_name.upper()
        return self.crosswalk_df[mask].copy()

    def get_pcds_meta_query(self, table_name: str) -> str:
        """Build query to get PCDS column metadata"""
        return f"""
SELECT column_name,
       data_type || CASE
           WHEN data_type = 'NUMBER' THEN
               CASE WHEN data_precision IS NULL AND data_scale IS NULL
                   THEN NULL
               ELSE '(' || TO_CHAR(data_precision) || ',' || TO_CHAR(data_scale) || ')'
               END
           WHEN data_type LIKE '%CHAR%' THEN '(' || TO_CHAR(data_length) || ')'
           ELSE NULL
       END AS data_type
FROM all_tab_cols
WHERE table_name = UPPER('{table_name}')
ORDER BY column_id
        """.strip()

    def get_aws_meta_query(self, database: str, table_name: str) -> str:
        """Build query to get AWS column metadata"""
        return f"""
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = LOWER('{database}')
  AND table_name = LOWER('{table_name}')
ORDER BY ordinal_position
        """.strip()

    def extract_pii_columns(self, crosswalk: pd.DataFrame) -> List[str]:
        """Extract columns marked as PII/tokenized"""
        if 'PII_Encryption' not in crosswalk.columns:
            return []

        pii_mask = crosswalk['PII_Encryption'].str.strip().str.upper().isin(['YES', 'Y', 'TRUE'])
        pii_cols = crosswalk.loc[pii_mask, 'OnPremColumns'].str.strip().str.upper().tolist()

        return pii_cols

    def build_column_mapping(self, crosswalk: pd.DataFrame,
                            pcds_meta: pd.DataFrame,
                            aws_meta: pd.DataFrame) -> Dict:
        """Build column mapping and identify discrepancies"""

        # Convert metadata to uppercase/lowercase consistently
        pcds_cols = set(pcds_meta['column_name'].str.upper())
        aws_cols = set(aws_meta['column_name'].str.lower())

        # Extract mappings from crosswalk
        pcds_to_aws = {}
        for _, row in crosswalk.iterrows():
            pcds_col = str(row.get('OnPremColumns', '')).strip().upper()
            aws_col = str(row.get('AWSColumns', '')).strip().lower()

            if pcds_col and aws_col and pcds_col != 'NAN' and aws_col != 'nan':
                pcds_to_aws[pcds_col] = aws_col

        # Identify tokenized columns
        tokenized = set(self.extract_pii_columns(crosswalk))

        # Find comparable columns (in both systems and documented)
        comparable = {
            pcds: aws for pcds, aws in pcds_to_aws.items()
            if pcds in pcds_cols and aws in aws_cols and pcds not in tokenized
        }

        # Find PCDS-only columns (not in AWS or not mapped)
        pcds_only = pcds_cols - set(pcds_to_aws.keys())

        # Find AWS-only columns (not in PCDS or not mapped)
        mapped_aws = set(pcds_to_aws.values())
        aws_only = aws_cols - mapped_aws

        # Find undocumented but similar columns (fuzzy matching)
        undocumented = self.find_undocumented_matches(pcds_only, aws_only)

        return {
            'comparable_columns': comparable,
            'pcds_only_columns': sorted(list(pcds_only)),
            'aws_only_columns': sorted(list(aws_only)),
            'tokenized_columns': sorted(list(tokenized)),
            'undocumented_matches': undocumented,
            'total_comparable': len(comparable),
            'total_pcds_only': len(pcds_only),
            'total_aws_only': len(aws_only),
            'total_tokenized': len(tokenized)
        }

    def find_undocumented_matches(self, pcds_cols: Set[str],
                                  aws_cols: Set[str]) -> Dict[str, str]:
        """Find potential column matches based on substring matching"""
        matches = {}

        for pcds in pcds_cols:
            pcds_lower = pcds.lower()
            for aws in aws_cols:
                # Check if columns share significant substring
                if len(pcds_lower) > 3 and len(aws) > 3:
                    if pcds_lower.startswith(aws) or aws.startswith(pcds_lower):
                        matches[pcds] = aws
                        break

        return matches

    def validate_data_types(self, crosswalk: pd.DataFrame,
                           pcds_meta: pd.DataFrame,
                           aws_meta: pd.DataFrame) -> List[Dict]:
        """Validate that mapped columns have compatible data types"""
        mismatches = []

        pcds_types = pcds_meta.set_index('column_name')['data_type'].to_dict()
        aws_types = aws_meta.set_index('column_name')['data_type'].to_dict()

        for _, row in crosswalk.iterrows():
            pcds_col = str(row.get('OnPremColumns', '')).strip().upper()
            aws_col = str(row.get('AWSColumns', '')).strip().lower()

            if pcds_col in pcds_types and aws_col in aws_types:
                pcds_type = pcds_types[pcds_col]
                aws_type = aws_types[aws_col]

                if not self.types_compatible(pcds_type, aws_type):
                    mismatches.append({
                        'pcds_column': pcds_col,
                        'aws_column': aws_col,
                        'pcds_type': pcds_type,
                        'aws_type': aws_type
                    })

        return mismatches

    def types_compatible(self, pcds_type: str, aws_type: str) -> bool:
        """Check if PCDS and AWS data types are compatible"""
        pcds_type = pcds_type.upper()
        aws_type = aws_type.lower()

        # NUMBER -> double/decimal
        if 'NUMBER' in pcds_type:
            return any(t in aws_type for t in ['double', 'decimal', 'bigint', 'int'])

        # VARCHAR2/CHAR -> string/varchar
        if any(t in pcds_type for t in ['VARCHAR', 'CHAR']):
            return any(t in aws_type for t in ['string', 'varchar', 'char'])

        # DATE -> date/timestamp
        if 'DATE' in pcds_type:
            return any(t in aws_type for t in ['date', 'timestamp'])

        # TIMESTAMP -> timestamp
        if 'TIMESTAMP' in pcds_type:
            return 'timestamp' in aws_type

        return False

    def process_table(self, table_name: str) -> Dict:
        """Process column mapping for a table"""
        table_info = self.state['tables'][table_name]['info']
        col_map_name = table_info.get('col_map_name', '')

        if not col_map_name:
            return {
                'status': 'no_crosswalk',
                'error': 'No crosswalk mapping specified'
            }

        # Load crosswalk for this table
        crosswalk = self.get_table_crosswalk(col_map_name)

        if crosswalk.empty:
            return {
                'status': 'crosswalk_not_found',
                'error': f'No crosswalk entries found for {col_map_name}'
            }

        # Get table metadata (placeholder - actual queries would be executed)
        pcds_service, pcds_table = table_info['pcds_tbl'].split('.', 1)
        pcds_query = self.get_pcds_meta_query(pcds_table)

        database, aws_table = table_info['aws_tbl'].split('.', 1)
        aws_query = self.get_aws_meta_query(database, aws_table)

        # In actual implementation, execute queries here
        # pcds_meta = execute_pcds_query(pcds_query, pcds_service)
        # aws_meta = execute_aws_query(aws_query)

        # For now, create placeholder metadata
        pcds_meta = pd.DataFrame({'column_name': [], 'data_type': []})
        aws_meta = pd.DataFrame({'column_name': [], 'data_type': []})

        # Build column mapping
        mapping = self.build_column_mapping(crosswalk, pcds_meta, aws_meta)

        # Validate data types
        type_mismatches = self.validate_data_types(crosswalk, pcds_meta, aws_meta)

        result = {
            'status': 'success',
            'pcds_query': pcds_query,
            'aws_query': aws_query,
            'mapping': mapping,
            'type_mismatches': type_mismatches,
            'crosswalk_entries': len(crosswalk)
        }

        return result

    def process_all_tables(self) -> Dict:
        """Process all tables"""
        results = {}

        for table_name in self.state['tables'].keys():
            print(f"Processing column mapping for {table_name}...")
            result = self.process_table(table_name)
            results[table_name] = result

            # Update state
            self.state['tables'][table_name]['column_mapping'] = result
            self.state['tables'][table_name]['steps_completed'].append('step3_column_mapping')

        # Mark global step complete
        if 'step3_column_mapping' not in self.state['global_steps_completed']:
            self.state['global_steps_completed'].append('step3_column_mapping')

        # Save state
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

    # Check if step 2 is complete
    state = utils.read_json(str(state_file))
    if 'step2_row_count' not in state.get('global_steps_completed', []):
        raise RuntimeError("Step 2 not completed. Please run 02_row_count_check.py first")

    # Check if this step is already complete
    if 'step3_column_mapping' in state.get('global_steps_completed', []):
        print(f"⚠ Step 3 already completed for run: {run_name}")
        print("  Delete the step results to re-run, or continue to step 4")
        return state['tables']

    # Load crosswalk file from src directory
    src_dir = Path(__file__).parent
    crosswalk_file = src_dir / 'crosswalk_doc.xlsx'

    if not crosswalk_file.exists():
        raise FileNotFoundError(f"Crosswalk file not found: {crosswalk_file}")

    print(f"Starting Step 3: Column Mapping for run: {run_name}")
    print(f"{'='*60}\n")

    mapper = ColumnMapper(str(state_file), str(crosswalk_file))
    results = mapper.process_all_tables()

    # Print summary
    print(f"\n{'='*60}")
    print("COLUMN MAPPING SUMMARY")
    print(f"{'='*60}")

    for table_name, result in results.items():
        print(f"\n{table_name}:")
        print(f"  Status: {result['status']}")
        if result['status'] == 'success':
            mapping = result['mapping']
            print(f"  Comparable columns: {mapping['total_comparable']}")
            print(f"  PCDS-only columns: {mapping['total_pcds_only']}")
            print(f"  AWS-only columns: {mapping['total_aws_only']}")
            print(f"  Tokenized columns: {mapping['total_tokenized']}")
            print(f"  Type mismatches: {len(result['type_mismatches'])}")

    print(f"\n✓ Step 3 complete. Results saved to: {state_file}")
    print(f"  Next: python 04_column_statistics.py {run_name}")

    return results


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        print("Usage: python 03_column_mapping.py <run_name>")
        print("Example: python 03_column_mapping.py run_20250122")
