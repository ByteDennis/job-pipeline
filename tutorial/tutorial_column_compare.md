# Tutorial: Column Check Comparison

## Overview

This tutorial walks through the **Column Check Comparison** step, which compares PCDS and AWS column statistics, identifies clean columns (where all statistics match), selects key columns for hash checking, and generates an Excel report.

### What This Step Produces

- **Consolidated column check JSON** containing:
  - Clean columns (all stats match across all vintages)
  - Mismatched columns (any stat differs)
  - Top key columns for hash checking (highest distinct count)
  - Complete metadata for hash check step

- **Excel report** with:
  - Summary sheet showing match rates per table/vintage
  - Detail sheets comparing PCDS vs AWS statistics side-by-side

### Prerequisites

1. **PCDS Column Check completed**
2. **AWS Column Check completed**
3. **Meta Check metadata available**

---

## Environment Variables Required

```bash
RUN_NAME=my_validation_run
CATEGORY=dpst
COLUMN_STEP=/path/to/config.toml
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

run_name, category, config_path = get_env('RUN_NAME', 'CATEGORY', 'COLUMN_STEP')

cfg = load_config(config_path)
step_name = cfg.output.summary.format(s='column')
output_folder = cfg.output.disk.format(name=run_name)

add_logger(output_folder, name=step_name)
logger.info(f"Starting column check comparison: {run_name} | {category}")
```

---

## Step 2: Download Results from S3

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

logger.info("Downloading PCDS column stats from S3")
pcds_results = s3.read_json(f"{cfg.output.step_name.format(p='pcds')}.json")

logger.info("Downloading AWS column stats from S3")
aws_results = s3.read_json(f"{cfg.output.step_name.format(p='aws')}.json")

logger.info("Downloading meta check from S3")
meta_check = s3.read_json(f'{cfg.output.summary.format(s="meta")}.json')
```

---

## Step 3: Define Statistics Comparison Schema

### What This Step Does

Define how each statistic field should be compared. Different fields need different comparison logic:
- **Exact match**: count, distinct
- **Numeric tolerance**: avg, std
- **Flexible string**: min, max (try date/numeric parsing)
- **Zero equals NaN**: missing (PCDS 0 = AWS NaN)
- **Frequency list**: freq_top10 (element-by-element)

### Configuration

```python
STAT_COMPARISON_SCHEMA = {
    'count': {'type': 'exact', 'description': 'Row count must match exactly'},
    'distinct': {'type': 'exact', 'description': 'Distinct count must match exactly'},
    'min': {'type': 'flexible_string', 'try_date': True, 'try_numeric': True},
    'max': {'type': 'flexible_string', 'try_date': True, 'try_numeric': True},
    'avg': {'type': 'numeric_tolerance', 'atol': 1e-6, 'rtol': 1e-6},
    'std': {'type': 'numeric_tolerance', 'atol': 1e-6, 'rtol': 1e-6},
    'missing': {'type': 'exact_with_zero_nan'},
    'freq_top10': {'type': 'frequency_list'}
}
```

---

## Step 4: Define Comparator Functions

### What This Step Does

Implement comparison functions for each type defined in the schema.

### Function Definitions

```python
def compare_exact(val1, val2, **kwargs):
    import pandas as pd
    #>>> implementation <<<#
    pass

def compare_numeric_tolerance(val1, val2, atol=1e-6, rtol=1e-6, **kwargs):
    import pandas as pd
    import numpy as np
    #>>> implementation <<<#
    pass

def compare_exact_with_zero_nan(val1, val2, **kwargs):
    import pandas as pd
    #>>> implementation <<<#
    pass

def compare_flexible_string(val1, val2, try_date=False, try_numeric=False, **kwargs):
    import pandas as pd
    import numpy as np
    #>>> implementation <<<#
    pass

def compare_frequency_list(val1, val2, **kwargs):
    #>>> implementation <<<#
    pass
```

### Comparator Dispatch Map

```python
COMPARATOR_MAP = {
    'exact': compare_exact,
    'numeric_tolerance': compare_numeric_tolerance,
    'exact_with_zero_nan': compare_exact_with_zero_nan,
    'flexible_string': compare_flexible_string,
    'frequency_list': compare_frequency_list
}
```

---

## Step 5: Compare Two Statistics Dictionaries

### What This Step Does

Compare PCDS and AWS statistics for a single column using the comparison schema. Returns True if all fields match, False otherwise.

### Function Definition

```python
def compare_stats(pcds_stats, aws_stats, schema=STAT_COMPARISON_SCHEMA):
    #>>> implementation <<<#
    pass
```

### How It Will Be Used

```python
pcds_stats = {
    'count': 10000,
    'distinct': 5000,
    'min': 10.5,
    'max': 9999.99,
    'avg': 150.25,
    'std': 75.5,
    'missing': 0
}

aws_stats = {
    'count': 10000,
    'distinct': 5000,
    'min': 10.5,
    'max': 9999.99,
    'avg': 150.25000001,
    'std': 75.5,
    'missing': None
}

match = compare_stats(pcds_stats, aws_stats)
print(f"Statistics match: {match}")
```

### Expected Output

```
Statistics match: True
```

---

## Step 6: Analyze Column Quality

### What This Step Does

For a single table, compare all columns across all vintages and categorize them:
- **Clean columns**: All statistics match in all vintages
- **Mismatched columns**: Any statistic differs in any vintage
- **Top key columns**: Top N clean columns with highest distinct count

### Function Definition

```python
def analyze_column_quality(pcds_result, aws_result, column_mapping, top_n=5):
    #>>> implementation <<<#
    pass
```

### How It Will Be Used

```python
pcds_result = {
    'table': 'SERVICE1.TABLE_A',
    'columns': ['CUST_ID', 'TXN_DATE', 'TXN_AMT'],
    'vintage_stats': [...]
}

aws_result = {
    'table': 'mydb.table_a',
    'columns': ['customer_id', 'txn_date', 'txn_amt'],
    'vintage_stats': [...]
}

column_mapping = {
    'CUST_ID': 'customer_id',
    'TXN_DATE': 'txn_date',
    'TXN_AMT': 'txn_amt'
}

quality = analyze_column_quality(pcds_result, aws_result, column_mapping, top_n=5)

print(f"Clean columns: {quality['clean_columns']}")
print(f"Mismatched columns: {quality['mismatched_columns']}")
print(f"Top key columns: {quality['top_key_columns']}")
```

---

## Step 7: Build Consolidated Column Check Metadata

### What This Step Does

Build consolidated metadata for all tables containing:
- Table identifiers
- Column mappings
- Clean vs mismatched columns
- Top key columns for hash checking
- Validated vintages with WHERE clauses

### Function Definition

```python
def build_consolidated_column_metadata(pcds_results, aws_results, meta_check):
    #>>> implementation <<<#
    pass
```

### Execute This Step

```python
consolidated = build_consolidated_column_metadata(pcds_results, aws_results, meta_check)

for table_info in consolidated['validated_tables']:
    logger.info(f"{table_info['pcds_table']}: {len(table_info['clean_columns'])}/{len(table_info['all_columns'])} clean columns, "
               f"key columns: {table_info['key_columns']}")
```

---

## Step 8: Prepare Excel Report Sections

### What This Step Does

Prepare data for Excel detail sheets, showing PCDS vs AWS statistics side-by-side for each vintage. Mismatched columns are displayed first for easy review.

### Function Definition

```python
def prepare_table_sections(pcds_result, aws_result, column_mapping, mismatched_columns_set):
    import pandas as pd
    #>>> implementation <<<#
    pass
```

---

## Step 9: Generate Excel Report

### What This Step Does

Create Excel workbook with:
- **Summary sheet**: Shows match rate per table/vintage
- **Detail sheets**: PCDS vs AWS comparison for each table/vintage

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

    def create_summary_sheet(self, title, headers, data_rows, color_by_match_rate=False):
        #>>> implementation <<<#
        pass

    def create_column_comparison_sheet(self, sheet_name, sections):
        #>>> implementation <<<#
        pass
```

### Execute This Step

```python
report_path = os.path.join(output_folder, f'{step_name}.xlsx')
logger.info(f"Generating Excel report: {report_path}")

table_info_map = {t['pcds_table']: t for t in consolidated['validated_tables']}

with ExcelReporter(report_path) as reporter:
    summary_rows = []

    for pcds, aws in zip(pcds_results, aws_results):
        table_name = pcds['table']
        table_info = table_info_map.get(table_name, {})
        column_mapping = table_info.get('column_mapping', {})
        total_cols = len(column_mapping)

        for pcds_v, aws_v in zip(pcds['vintage_stats'], aws['vintage_stats']):
            vintage = pcds_v['vintage']
            mismatched_cols = set()

            for pcds_col, aws_col in column_mapping.items():
                pcds_stats = pcds_v['stats'].get(pcds_col.upper())
                aws_stats = aws_v['stats'].get(aws_col.lower())

                if not compare_stats(pcds_stats, aws_stats):
                    mismatched_cols.add(pcds_col)

            matched = total_cols - len(mismatched_cols)
            match_rate = (matched / total_cols * 100) if total_cols > 0 else 0

            summary_rows.append([
                table_name,
                vintage,
                total_cols,
                matched,
                len(mismatched_cols),
                f'{match_rate:.1f}%'
            ])

    reporter.create_summary_sheet(
        title='Column Statistics Comparison',
        headers=['Dataset', 'Vintage', 'Total Columns', 'Matched Columns', 'Mismatched Columns', 'Match Rate %'],
        data_rows=summary_rows,
        color_by_match_rate=True
    )

    for pcds, aws in zip(pcds_results, aws_results):
        table_name = pcds['table']
        table_info = table_info_map.get(table_name, {})
        column_mapping = table_info.get('column_mapping', {})

        mismatched_columns_set = set()
        for pcds_v, aws_v in zip(pcds['vintage_stats'], aws['vintage_stats']):
            for pcds_col, aws_col in column_mapping.items():
                pcds_stats = pcds_v['stats'].get(pcds_col.upper())
                aws_stats = aws_v['stats'].get(aws_col.lower())
                if not compare_stats(pcds_stats, aws_stats):
                    mismatched_columns_set.add(pcds_col)

        sections = prepare_table_sections(pcds, aws, column_mapping, mismatched_columns_set)
        reporter.create_column_comparison_sheet(pcds['table'].split('.')[-1], sections)

logger.info(f"Report saved to {report_path}")
```

---

## Step 10: Save and Upload Consolidated Metadata

### Execute This Step

```python
s3.write_json(consolidated, f'{step_name}.json')
logger.info(f"Uploaded consolidated column_check.json to S3")
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

    run_name, category, config_path = get_env('RUN_NAME', 'CATEGORY', 'COLUMN_STEP')

    cfg = load_config(config_path)
    step_name = cfg.output.summary.format(s='column')
    output_folder = cfg.output.disk.format(name=run_name)

    add_logger(output_folder, name=step_name)
    logger.info(f"Starting column check comparison: {run_name} | {category}")

    s3_bucket = cfg.output.s3.format(name=run_name)
    s3 = S3Manager(s3_bucket)

    logger.info("Downloading PCDS column stats from S3")
    pcds_results = s3.read_json(f"{cfg.output.step_name.format(p='pcds')}.json")

    logger.info("Downloading AWS column stats from S3")
    aws_results = s3.read_json(f"{cfg.output.step_name.format(p='aws')}.json")

    logger.info("Downloading meta check from S3")
    meta_check = s3.read_json(f'{cfg.output.summary.format(s="meta")}.json')

    consolidated = build_consolidated_column_metadata(pcds_results, aws_results, meta_check)

    for table_info in consolidated['validated_tables']:
        logger.info(f"{table_info['pcds_table']}: {len(table_info['clean_columns'])}/{len(table_info['all_columns'])} clean columns, "
                   f"key columns: {table_info['key_columns']}")

    s3.write_json(consolidated, f'{step_name}.json')
    logger.info(f"Uploaded consolidated column_check.json to S3")

    report_path = os.path.join(output_folder, f'{step_name}.xlsx')
    logger.info(f"Generating Excel report: {report_path}")

    table_info_map = {t['pcds_table']: t for t in consolidated['validated_tables']}

    with ExcelReporter(report_path) as reporter:
        summary_rows = []

        for pcds, aws in zip(pcds_results, aws_results):
            table_name = pcds['table']
            table_info = table_info_map.get(table_name, {})
            column_mapping = table_info.get('column_mapping', {})
            total_cols = len(column_mapping)

            for pcds_v, aws_v in zip(pcds['vintage_stats'], aws['vintage_stats']):
                vintage = pcds_v['vintage']
                mismatched_cols = set()

                for pcds_col, aws_col in column_mapping.items():
                    pcds_stats = pcds_v['stats'].get(pcds_col.upper())
                    aws_stats = aws_v['stats'].get(aws_col.lower())

                    if not compare_stats(pcds_stats, aws_stats):
                        mismatched_cols.add(pcds_col)

                matched = total_cols - len(mismatched_cols)
                match_rate = (matched / total_cols * 100) if total_cols > 0 else 0

                summary_rows.append([
                    table_name,
                    vintage,
                    total_cols,
                    matched,
                    len(mismatched_cols),
                    f'{match_rate:.1f}%'
                ])

        reporter.create_summary_sheet(
            title='Column Statistics Comparison',
            headers=['Dataset', 'Vintage', 'Total Columns', 'Matched Columns', 'Mismatched Columns', 'Match Rate %'],
            data_rows=summary_rows,
            color_by_match_rate=True
        )

        for pcds, aws in zip(pcds_results, aws_results):
            table_name = pcds['table']
            table_info = table_info_map.get(table_name, {})
            column_mapping = table_info.get('column_mapping', {})

            mismatched_columns_set = set()
            for pcds_v, aws_v in zip(pcds['vintage_stats'], aws['vintage_stats']):
                for pcds_col, aws_col in column_mapping.items():
                    pcds_stats = pcds_v['stats'].get(pcds_col.upper())
                    aws_stats = aws_v['stats'].get(aws_col.lower())
                    if not compare_stats(pcds_stats, aws_stats):
                        mismatched_columns_set.add(pcds_col)

            sections = prepare_table_sections(pcds, aws, column_mapping, mismatched_columns_set)
            reporter.create_column_comparison_sheet(pcds['table'].split('.')[-1], sections)

    logger.info(f"Report saved to {report_path}")

    return report_path


if __name__ == '__main__':
    main()
```

---

## Summary

This tutorial covered the Column Check Comparison step which:

1. **Downloads PCDS and AWS column statistics**
2. **Compares statistics using schema-driven approach**
3. **Identifies clean vs mismatched columns**
4. **Selects top key columns for hash checking**
5. **Generates consolidated metadata for hash step**
6. **Produces Excel report with comparison details**

The clean columns and key columns are used by Hash Check to validate actual row-level data.
