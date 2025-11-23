"""
Step 1: Load configuration and table list, initialize pipeline state

Reads config file and Excel table list, validates inputs, creates output JSON state file.
"""

import json
import configparser
from pathlib import Path
from typing import Dict
from datetime import datetime
import utils


class ConfigLoader:

    def __init__(self, config_path: str):
        """Load configuration file"""
        self.config = self._parse_config(config_path)
        self.config_path = Path(config_path)

    def _parse_config(self, config_path: str) -> Dict:
        """Parse .cfg file using configparser"""
        parser = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
        parser.read(config_path)

        config = {}
        for section in parser.sections():
            config[section] = dict(parser.items(section))

        return config

    def get(self, section: str, key: str, default=None) -> str:
        """Get configuration value"""
        return self.config.get(section, {}).get(key, default)

    def get_section(self, section: str) -> Dict:
        """Get entire configuration section"""
        return self.config.get(section, {})


class PipelineStateManager:

    def __init__(self, output_path: str):
        """Manage pipeline state JSON file"""
        self.output_path = Path(output_path)
        self.state = {
            'created_at': datetime.now().isoformat(),
            'last_updated': datetime.now().isoformat(),
            'tables': {},
            'metadata': {},
            'global_steps_completed': []
        }

    def load(self) -> Dict:
        """Load existing state if available"""
        if self.output_path.exists():
            with open(self.output_path, 'r') as f:
                self.state = json.load(f)
        return self.state

    def save(self):
        """Save state to JSON file"""
        self.state['last_updated'] = datetime.now().isoformat()
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.output_path, 'w') as f:
            json.dump(self.state, f, indent=2)

    def add_table(self, table_name: str, table_info: Dict):
        """Add or update table information"""
        if table_name not in self.state['tables']:
            self.state['tables'][table_name] = {
                'created_at': datetime.now().isoformat(),
                'steps_completed': [],
                'info': {},
                'row_counts': {},
                'column_mapping': {},
                'statistics': {},
                'hash_comparison': {}
            }
        self.state['tables'][table_name]['info'].update(table_info)

    def mark_step_complete(self, table_name: str, step_name: str):
        """Mark a pipeline step as complete for a table"""
        if table_name in self.state['tables']:
            steps = self.state['tables'][table_name]['steps_completed']
            if step_name not in steps:
                steps.append(step_name)

    def mark_global_step_complete(self, step_name: str):
        """Mark a global pipeline step as complete"""
        if step_name not in self.state['global_steps_completed']:
            self.state['global_steps_completed'].append(step_name)

    def update_metadata(self, metadata: Dict):
        """Update pipeline metadata"""
        self.state['metadata'].update(metadata)

    def get_table_info(self, table_name: str) -> Dict:
        """Retrieve table information"""
        return self.state['tables'].get(table_name)


def main(run_name: str):
    """Main execution function"""
    # Hardcoded paths - load config from src directory
    src_dir = Path(__file__).parent
    config_path = src_dir / 'config.cfg'

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    config = ConfigLoader(str(config_path))

    # Create output folder for this run
    output_folder = Path('output') / run_name
    output_folder.mkdir(parents=True, exist_ok=True)

    # Copy config to output folder
    import shutil
    shutil.copy(config_path, output_folder / 'config.cfg')

    # State file location
    state_file = output_folder / 'pipeline_state.json'

    # Check if already initialized
    if state_file.exists():
        print(f"⚠ Pipeline already initialized: {state_file}")
        print("  To re-initialize, delete the output folder or use a different run_name")
        return utils.read_json(str(state_file))

    # Load Excel table list from config
    excel_config = config.get_section('input.table')
    excel_path = src_dir / excel_config['file']

    if not excel_path.exists():
        raise FileNotFoundError(f"Excel file not found: {excel_path}")

    df = utils.read_excel_input(
        excel_path=str(excel_path),
        excel_sheet=excel_config['sheet']
    )

    state_manager = PipelineStateManager(str(state_file))

    for _, row in df.iterrows():
        pcds_tbl_full = row['pcds_tbl']
        table_name = pcds_tbl_full.split('.')[-1]

        table_info = {
            'enabled': row['enabled'],
            'business_name': row.get('tables_requested', ''),
            'col_map_name': row.get('col_map', ''),
            'pcds_tbl': pcds_tbl_full,
            'aws_tbl': row['aws_tbl'],
            'pcds_var': str(row['pcds_var']) if not str(row['pcds_var']) == 'NaT' else '',
            'aws_var': str(row['aws_var']) if not str(row['aws_var']) == 'NaT' else '',
            'pcds_where': row.get('pcds_where', '') or '',
            'aws_where': row.get('aws_where', '') or '',
            'start_dt': str(row.get('start_dt', '')) if row.get('start_dt') else '',
            'end_dt': str(row.get('end_dt', '')) if row.get('end_dt') else '',
            'partition': row.get('partition', ''),
        }

        state_manager.add_table(table_name, table_info)

    state_manager.update_metadata({
        'total_tables': len(df),
        'config_file': str(config_path),
        'run_name': run_name,
        'category': config.get('input', 'category', 'dpst'),
        'source_file': Path(excel_config['file']).name,
        'output_folder': str(output_folder)
    })

    state_manager.mark_global_step_complete('step1_load_config')
    state_manager.save()

    print(f"✓ Pipeline state initialized: {state_file}")
    print(f"✓ Config copied to: {output_folder / 'config.cfg'}")
    print(f"✓ Total tables loaded: {state_manager.state['metadata']['total_tables']}")
    print(f"✓ Tables: {list(state_manager.state['tables'].keys())}")

    return state_manager.state


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        print("Usage: python 01_load_config.py <run_name>")
        print("Example: python 01_load_config.py run_20250122")
