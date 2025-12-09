# Tutorial: Meta Check Comparison

## Overview

This tutorial walks through the **Meta Check Comparison** step, which downloads PCDS and AWS meta check results, compares them, builds consolidated metadata for downstream processing, and generates an Excel report showing the comparison results.

### What This Step Produces

- **Consolidated metadata JSON** containing:
  - Validated tables (tables that passed all checks)
  - Excluded tables (tables that failed checks)
  - Column mappings between PCDS and AWS
  - Validated vintages with mismatched dates excluded
  - Complete metadata for column and hash check steps

- **Excel report** with:
  - Summary sheet showing all tables and match status
  - Detail sheets for each table showing crosswalk and date mismatches

### Prerequisites

1. **PCDS Meta Check completed** - meta_check_pcds.json uploaded to S3
2. **AWS Meta Check completed** - meta_check_aws.json uploaded to S3
3. **Input files on S3** - input_tables.csv, crosswalk.csv

---

## Environment Variables Required

```bash
RUN_NAME=my_validation_run
CATEGORY=dpst
META_STEP=/path/to/config.toml
```

---

## Step 1: Load Environment and Configuration

### What This Step Does

Load environment variables and configuration, similar to previous steps.

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

run_name, category, config_path = get_env('RUN_NAME', 'CATEGORY', 'META_STEP')

cfg = load_config(config_path)
step_name = cfg.output.summary.format(s='meta')
output_folder = cfg.output.disk.format(name=run_name)

add_logger(output_folder, name=step_name)
logger.info(f"Starting comparison report: {run_name} | {category}")
```

---

## Step 2: Download Results from S3

### What This Step Does

Download PCDS results, AWS results, input tables, and crosswalk from S3 for comparison.

### S3Manager Class

```python
class S3Manager:
    def __init__(self, s3_bucket):
        #>>> implementation <<<#
        pass

    def read_json(self, filename):
        #>>> implementation <<<#
        pass

    def read_df(self, filename):
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

logger.info("Downloading PCDS results from S3")
pcds_results = s3.read_json(f"{cfg.output.step_name.format(p='pcds')}.json")

logger.info("Downloading AWS results from S3")
aws_results = s3.read_json(f"{cfg.output.step_name.format(p='aws')}.json")

logger.info("Downloading input tables from S3")
tables_df = s3.read_df('input_tables.csv')

logger.info("Downloading crosswalk tables from S3")
crosswalk_df = s3.read_df('crosswalk.csv')
```

---

## Step 3: Build Column Mapping from Crosswalk

### What This Step Does

Build a mapping dictionary from PCDS columns (uppercase) to AWS columns (lowercase) using the crosswalk. Only include comparable columns (not tokenized, not one-sided).

### Function Definition

```python
def build_column_mapping(crosswalk_df, col_map_name, comparable_pcds, comparable_aws):
    import pandas as pd
    #>>> implementation <<<#
    pass
```

### How It Will Be Used

```python
col_map_name = 'table_a'
comparable_pcds = ['CUST_ID', 'TXN_DATE', 'TXN_AMT']
comparable_aws = ['customer_id', 'txn_date', 'txn_amt']

mapping_result = build_column_mapping(crosswalk_df, col_map_name, comparable_pcds, comparable_aws)

column_mapping = mapping_result['column_mapping']
unmapped_pcds = mapping_result['unmapped_pcds']
unmapped_aws = mapping_result['unmapped_aws']

print(f"Mapped {len(column_mapping)} columns")
print(f"Column mapping: {column_mapping}")
```

### Expected Output

```
Mapped 3 columns
Column mapping: {'CUST_ID': 'customer_id', 'TXN_DATE': 'txn_date', 'TXN_AMT': 'txn_amt'}
```

---

## Step 4: Build WHERE Clause Helper

### What This Step Does

Build WHERE clauses for vintages that exclude mismatched dates. This ensures we only compare data that has matching row counts on both sides.

### Function Definition

```python
def sql_literal(val):
    import re
    #>>> implementation <<<#
    pass

def build_where(vintage, where_clause, date_var, excl_values):
    #>>> implementation <<<#
    pass
```

### How It Will Be Used

```python
vintage = {
    'where_clause': "TXN_DATE >= DATE '2023-01-01' AND TXN_DATE <= DATE '2023-01-31'"
}
where_clause = "STATUS = 'ACTIVE'"
date_var = 'TXN_DATE'
excl_values = ['2023-01-15', '2023-01-20']

final_where = build_where(vintage, where_clause, date_var, excl_values)
print(final_where)
```

### Expected Output

```
(TXN_DATE >= DATE '2023-01-01' AND TXN_DATE <= DATE '2023-01-31') AND (STATUS = 'ACTIVE') AND TXN_DATE NOT IN ('2023-01-15', '2023-01-20')
```

---

## Step 5: Build Consolidated Metadata

### What This Step Does

This is the heart of the comparison step. For each table, it:
1. Compares PCDS and AWS row counts by date
2. Identifies mismatched dates
3. Generates validated vintages (excluding mismatched dates)
4. Builds column mappings
5. Prepares metadata for downstream steps

Tables are validated if they:
- Are accessible on both sides
- Have at least one overlapping vintage
- Have valid column mappings

### Function Definition

```python
def build_consolidated_metadata(pcds_results, aws_results, tables_df, crosswalk_df):
    import pandas as pd
    from loguru import logger
    #>>> implementation <<<#
    pass
```

### Execute This Step

```python
consolidated = build_consolidated_metadata(pcds_results, aws_results, tables_df, crosswalk_df)

validated_tables = consolidated['validated_tables']
excluded_tables = consolidated['excluded_tables']

logger.info(f"Validated {len(validated_tables)} tables, excluded {len(excluded_tables)} tables")
```

### Expected Output

```
2024-01-15 11:05:30 | INFO | Validated 10 tables, excluded 2 tables
```

---

## Step 6: DateParser Utilities

### What This Step Does

DateParser utilities for converting dates and reconstructing DateParser objects from JSON.

### Class Methods

```python
class DateParser:
    def __init__(self, var_name, data_type):
        self._var = var_name
        self._type = data_type
        self._fmt = None

    @staticmethod
    def from_json(json_str):
        import json
        #>>> implementation <<<#
        pass

    def to_original(self, std_date):
        #>>> implementation <<<#
        pass
```

---

## Step 7: Prepare Excel Report Sections

### What This Step Does

For each table, prepare sections to display in Excel detail sheets showing:
- Table information (rows showing PCDS vs AWS)
- Matching days summary
- PCDS-only and AWS-only columns
- Dates with count mismatches

### Function Definition

```python
def prepare_table_sections_from_consolidated(table_meta, pcds_accessible, aws_accessible, pcds_crosswalk, aws_crosswalk):
    import pandas as pd
    #>>> implementation <<<#
    pass
```

### How It Will Be Used

```python
sections = []

for table_meta, pcds, aws in zip(validated_tables, pcds_results, aws_results):
    table_sections = prepare_table_sections_from_consolidated(
        table_meta=table_meta,
        pcds_accessible=pcds['accessible'],
        aws_accessible=aws['accessible'],
        pcds_crosswalk=pcds['crosswalk'],
        aws_crosswalk=aws['crosswalk']
    )
    sections.append(table_sections)
```

---

## Step 8: Generate Excel Report

### What This Step Does

Create an Excel workbook with:
- Summary sheet: Overview of all tables
- Detail sheets: One per table with comparisons

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

with ExcelReporter(report_path) as reporter:
    summary_rows = [
        [
            t['pcds_table'],
            '✓',
            '✓',
            t['total_days_union'],
            t['matched_day_count'],
            '✓' if t['row_match_all'] else '✗'
        ]
        for t in validated_tables
    ]

    reporter.create_summary_sheet(
        title='Meta Check Comparison Summary',
        headers=['Table', 'PCDS Access', 'AWS Access',
                 'Total Distinct Days', 'Matched Day Count', 'All Days Match'],
        data_rows=summary_rows
    )

    for t, pcds, aws in zip(validated_tables, pcds_results, aws_results):
        sections = prepare_table_sections_from_consolidated(
            table_meta=t,
            pcds_accessible=pcds['accessible'],
            aws_accessible=aws['accessible'],
            pcds_crosswalk=pcds['crosswalk'],
            aws_crosswalk=aws['crosswalk']
        )
        reporter.create_detail_sheet(pcds['table'], sections)

logger.info(f"Report saved to {report_path}")
```

---

## Step 9: Save and Upload Consolidated Metadata

### What This Step Does

Save consolidated metadata locally and upload to S3. This JSON file contains all the validated metadata that downstream steps (column check, hash check) will use.

### Execute This Step

```python
local_path = os.path.join(output_folder, f'{step_name}.json')
s3.write_json(consolidated, UPath(local_path))
s3.write_json(consolidated, f'{step_name}.json')

logger.info(f"Uploaded consolidated metadata to S3")
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

    run_name, category, config_path = get_env('RUN_NAME', 'CATEGORY', 'META_STEP')

    cfg = load_config(config_path)
    step_name = cfg.output.summary.format(s='meta')
    output_folder = cfg.output.disk.format(name=run_name)

    add_logger(output_folder, name=step_name)
    logger.info(f"Starting comparison report: {run_name} | {category}")

    s3_bucket = cfg.output.s3.format(name=run_name)
    s3 = S3Manager(s3_bucket)

    logger.info("Downloading PCDS results from S3")
    pcds_results = s3.read_json(f"{cfg.output.step_name.format(p='pcds')}.json")

    logger.info("Downloading AWS results from S3")
    aws_results = s3.read_json(f"{cfg.output.step_name.format(p='aws')}.json")

    logger.info("Downloading input tables from S3")
    tables_df = s3.read_df('input_tables.csv')

    logger.info("Downloading crosswalk tables from S3")
    crosswalk_df = s3.read_df('crosswalk.csv')

    consolidated = build_consolidated_metadata(pcds_results, aws_results, tables_df, crosswalk_df)
    logger.info(f"Validated {len(consolidated['validated_tables'])} tables, excluded {len(consolidated['excluded_tables'])} tables")

    local_path = os.path.join(output_folder, f'{step_name}.json')
    s3.write_json(consolidated, UPath(local_path))
    s3.write_json(consolidated, f'{step_name}.json')

    report_path = os.path.join(output_folder, f'{step_name}.xlsx')
    with ExcelReporter(report_path) as reporter:
        summary_rows = [
            [
                t['pcds_table'],
                '✓',
                '✓',
                t['total_days_union'],
                t['matched_day_count'],
                '✓' if t['row_match_all'] else '✗'
            ]
            for t in consolidated['validated_tables']
        ]

        reporter.create_summary_sheet(
            title='Meta Check Comparison Summary',
            headers=['Table', 'PCDS Access', 'AWS Access',
                     'Total Distinct Days', 'Matched Day Count', 'All Days Match'],
            data_rows=summary_rows
        )

        for t, pcds, aws in zip(consolidated['validated_tables'], pcds_results, aws_results):
            sections = prepare_table_sections_from_consolidated(
                table_meta=t,
                pcds_accessible=pcds['accessible'],
                aws_accessible=aws['accessible'],
                pcds_crosswalk=pcds['crosswalk'],
                aws_crosswalk=aws['crosswalk']
            )
            reporter.create_detail_sheet(pcds['table'], sections)

    logger.info(f"Report saved to {report_path}")

    return report_path


if __name__ == '__main__':
    main()
```

---

## Summary

This tutorial covered the Meta Check Comparison step which:

1. **Downloads PCDS and AWS metadata** from S3
2. **Compares row counts** by date between PCDS and AWS
3. **Identifies mismatched dates** where counts don't match
4. **Builds validated vintages** excluding mismatched dates
5. **Creates column mappings** from crosswalk
6. **Generates consolidated metadata** for downstream steps
7. **Produces Excel report** showing comparison results

The consolidated metadata contains all information needed for column and hash check steps to validate actual data values.
