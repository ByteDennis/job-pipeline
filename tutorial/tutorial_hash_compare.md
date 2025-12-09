# Tutorial: Hash Check Comparison

## Overview

This tutorial walks through the **Hash Check Comparison** step, the final step in the validation pipeline. It compares PCDS and AWS row-level hashes to identify specific rows that differ, generates consolidated metadata, and produces an Excel report.

### What This Step Produces

- **Consolidated hash check JSON** containing:
  - Comparison summary for each table
  - Clean columns and key columns used
  - Vintage-level comparison results

- **Excel report** with:
  - Summary sheet showing match statistics
  - Detail sheets per table showing:
    - Total rows vs unique hashes
    - Matched rows, PCDS-only, AWS-only
    - Sample mismatched rows

### Prerequisites

1. **PCDS Hash Check completed**
2. **AWS Hash Check completed**

---

## Environment Variables Required

```bash
RUN_NAME=my_validation_run
CATEGORY=dpst
HASH_STEP=/path/to/config.toml
```

---

## Step 1: Load Environment and Configuration

### Function Definitions

```python
def get_env(*var_names):
    import os
    #>>> implementation <<<#
    pass

def load_config(config_path):
    #>>> implementation <<<#
    pass

def add_logger(output_folder, name):
    import os
    from loguru import logger
    #>>> implementation <<<#
    pass
```

### Execute This Step

```python
from loguru import logger
import os
from upath import UPath
import pandas as pd

run_name, category, config_path = get_env('RUN_NAME', 'CATEGORY', 'HASH_STEP')

cfg = load_config(config_path)
step_name = cfg.output.summary.format(s='hash')
output_folder = cfg.output.disk.format(name=run_name)

add_logger(output_folder, name=step_name)
logger.info(f"Starting hash check comparison: {run_name} | {category}")
```

---

## Step 2: Download Hash Results from S3

### S3Manager Class

```python
class S3Manager:
    def __init__(self, s3_bucket):
        #>>> implementation <<<#
        pass

    def read_json(self, filename):
        #>>> implementation <<<#
        pass

    def write_json(self, data, filepath):
        #>>> implementation <<<#
        pass
```

### Execute This Step

```python
s3_bucket = cfg.output.s3.format(name=run_name)
s3 = S3Manager(s3_bucket)

logger.info("Downloading PCDS hashes from S3")
pcds_results = s3.read_json(f"{cfg.output.step_name.format(p='pcds')}.json")

logger.info("Downloading AWS hashes from S3")
aws_results = s3.read_json(f"{cfg.output.step_name.format(p='aws')}.json")
```

---

## Step 3: Compare Hashes for Single Vintage

### What This Step Does

For one vintage, merge PCDS and AWS hashes on key columns and identify:
- **Matched rows**: Same key, same hash
- **Hash mismatches**: Same key, different hash
- **PCDS-only rows**: Key exists only in PCDS
- **AWS-only rows**: Key exists only in AWS

### Function Definition

```python
def compare_vintage_hashes(pcds_hashes, aws_hashes, key_columns):
    import pandas as pd
    #>>> implementation <<<#
    pass
```

### How It Will Be Used

```python
pcds_hashes = {
    'hashes': [
        {'CUST_ID': 123, 'hash_value': 'abc123'},
        {'CUST_ID': 456, 'hash_value': 'def456'}
    ]
}

aws_hashes = {
    'hashes': [
        {'customer_id': 123, 'hash_value': 'abc123'},
        {'customer_id': 456, 'hash_value': 'xyz789'}
    ]
}

key_columns = ['CUST_ID']

comparison = compare_vintage_hashes(pcds_hashes, aws_hashes, key_columns)

print(f"Matched rows: {comparison['matched_rows']}")
print(f"Hash mismatches: {comparison['hash_mismatch_rows']}")
print(f"PCDS only: {comparison['pcds_only_rows']}")
print(f"AWS only: {comparison['aws_only_rows']}")
```

### Expected Output

```
Matched rows: 1
Hash mismatches: 1
PCDS only: 0
AWS only: 0
```

---

## Step 4: Prepare Table Detail Sections for Excel

### What This Step Does

For each table, prepare sections showing:
- Table information (name, columns, key columns, mismatched columns)
- Vintage-level comparison results
- Sample mismatched rows (up to 100)

### Function Definition

```python
def prepare_table_sections(pcds_result, aws_result):
    import pandas as pd
    #>>> implementation <<<#
    pass
```

### How It Will Be Used

```python
pcds_result = {
    'table': 'SERVICE1.TABLE_A',
    'clean_columns': ['CUST_ID', 'TXN_AMT'],
    'key_columns': ['CUST_ID'],
    'mismatched_columns': [],
    'vintage_hashes': [...]
}

aws_result = {
    'table': 'mydb.table_a',
    'clean_columns': ['customer_id', 'txn_amt'],
    'key_columns': ['customer_id'],
    'mismatched_columns': [],
    'vintage_hashes': [...]
}

sections = prepare_table_sections(pcds_result, aws_result)
```

---

## Step 5: Build Consolidated Hash Check Metadata

### What This Step Does

Consolidate hash check results for all tables, providing summary statistics.

### Function Definition

```python
def build_consolidated_hash_metadata(pcds_results, aws_results):
    #>>> implementation <<<#
    pass
```

### Execute This Step

```python
consolidated = build_consolidated_hash_metadata(pcds_results, aws_results)

local_path = os.path.join(output_folder, f'{step_name}.json')
s3.write_json(consolidated, UPath(local_path))
s3.write_json(consolidated, f'{step_name}.json')

logger.info(f"Uploaded consolidated hash check to S3")
```

---

## Step 6: Generate Excel Report

### What This Step Does

Create Excel workbook with:
- **Summary sheet**: Shows total mismatches per table
- **Detail sheets**: Per-table comparison with vintage breakdown

### ExcelReporter Class

```python
class ExcelReporter:
    def __init__(self, report_path):
        #>>> implementation <<<#
        pass

    def __enter__(self):
        #>>> implementation <<<#
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        #>>> implementation <<<#
        pass

    def create_summary_sheet(self, title, headers, data_rows):
        #>>> implementation <<<#
        pass

    def create_detail_sheet(self, sheet_name, sections):
        #>>> implementation <<<#
        pass
```

### Execute This Step

```python
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
```

---

## Putting It All Together

### Complete main() Function

```python
def main():
    import os
    from upath import UPath
    import pandas as pd
    from loguru import logger

    run_name, category, config_path = get_env('RUN_NAME', 'CATEGORY', 'HASH_STEP')

    cfg = load_config(config_path)
    step_name = cfg.output.summary.format(s='hash')
    output_folder = cfg.output.disk.format(name=run_name)

    add_logger(output_folder, name=step_name)
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
```

---

## Summary

This tutorial covered the Hash Check Comparison step, the final validation step, which:

1. **Downloads PCDS and AWS hashes** from S3
2. **Compares hashes on key columns** for each vintage
3. **Identifies mismatched rows** at the individual row level
4. **Generates consolidated metadata** with comparison results
5. **Produces Excel report** showing hash match statistics

This completes the full validation pipeline: Meta → Column → Hash!
