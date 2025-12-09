# Tutorial: AWS Meta Check

## Overview

This tutorial walks through the **AWS Meta Check** step, which mirrors the PCDS Meta Check but runs against AWS (Athena) databases. This step validates AWS database tables by checking their accessibility, extracting column metadata, validating crosswalk mappings, and confirming date vintages match the PCDS side.

### What This Step Produces

- **Metadata JSON file** containing:
  - AWS table accessibility status
  - Column names and data types
  - Crosswalk mapping validation
  - Row counts grouped by date variable
  - Date vintages that overlap with PCDS

### Prerequisites

1. **PCDS Meta Check completed** - This step depends on outputs from PCDS Meta Check
2. **S3 access** to download input_tables.csv and crosswalk.csv uploaded by PCDS step
3. **AWS/Athena access** to query AWS databases
4. **Environment variables** configured

> **Note**: This tutorial assumes you have AWS query functions available. The PCDS-side functions (like proc_pcds) are not available here - this runs independently on a different machine.

---

## Environment Variables Required

```bash
RUN_NAME=my_validation_run     # Same as PCDS step
CATEGORY=dpst                   # Same as PCDS step
META_STEP=/path/to/config.toml  # Path to TOML configuration file
```

---

## Step 1: Load Environment Variables

### What This Step Does

Retrieve environment variables that control execution, matching what was used in the PCDS step.

### Function Definition

```python
def get_env(*var_names):
    import os
    #>>> implementation <<<#
    pass
```

### Execute This Step

```python
run_name, category, config_path = get_env('RUN_NAME', 'CATEGORY', 'META_STEP')
print(f"Run: {run_name}, Category: {category}, Config: {config_path}")
```

---

## Step 2: Load Configuration from TOML File

### What This Step Does

Load the same configuration used by PCDS step. The AWS step uses the same config structure but formats paths differently (p='aws' instead of p='pcds').

### Function Definition

```python
def load_config(config_path):
    #>>> implementation <<<#
    pass
```

### Execute This Step

```python
cfg = load_config(config_path)

step_name = cfg.output.step_name.format(p='aws')
output_folder = cfg.output.disk.format(name=run_name)

print(f"Step name: {step_name}")
print(f"Output folder: {output_folder}")
```

### Expected Output

```
Step name: meta_check_aws
Output folder: output/my_validation_run
```

---

## Step 3: Setup Logging

### What This Step Does

Configure logging to track AWS meta check operations.

### Function Definition

```python
def add_logger(output_folder, name):
    import os
    from loguru import logger
    #>>> implementation <<<#
    pass
```

### Execute This Step

```python
from loguru import logger

add_logger(output_folder, name=step_name)
logger.info(f"Starting AWS meta check: {run_name} | {category}")
```

---

## Step 4: Initialize S3 Manager and Download Inputs

### What This Step Does

Unlike PCDS which reads from local Excel files, AWS downloads the filtered input_tables.csv and crosswalk.csv that PCDS uploaded to S3. This ensures both sides process the exact same set of tables and mappings.

### S3Manager Class Definition

```python
class S3Manager:
    def __init__(self, s3_bucket):
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

tables_df = s3.read_df('input_tables.csv')
crosswalk_df = s3.read_df('crosswalk.csv')

logger.info(f"Downloaded from S3: {len(tables_df)} tables to validate, {len(crosswalk_df)} crosswalk mappings")
```

### Expected Output

```
2024-01-15 10:40:12 | INFO | Downloaded from S3: 12 tables to validate, 380 crosswalk mappings
```

---

## Step 5: Check AWS Table Accessibility

### What This Step Does

Verify AWS table exists and is queryable. Uses Athena SQL syntax with LIMIT instead of Oracle's ROWNUM.

### Function Definition

```python
def check_accessible(database, table_name):
    from loguru import logger
    #>>> implementation <<<#
    pass
```

### How It Will Be Used

```python
database = 'mydb'
table_name = 'database.table_a'
is_accessible = check_accessible(database, table_name)
print(f"Table accessible: {is_accessible}")
```

---

## Step 6: Get AWS Table Columns and Data Types

### What This Step Does

Query AWS information_schema to retrieve column metadata. This is different from Oracle's all_tab_columns.

### Function Definition

```python
def get_columns(database, table_name):
    #>>> implementation <<<#
    pass
```

### How It Will Be Used

```python
columns_df = get_columns(database, table_name)
print(f"Found {len(columns_df)} columns")
print(columns_df.head())
```

### Expected Output

```
Found 45 columns
  column_name        data_type
0 customer_id        bigint
1 txn_date           date
2 txn_amt            double
3 description        varchar
...
```

---

## Step 7: Check Crosswalk Mapping Completeness

### What This Step Does

Validate crosswalk mappings for AWS side. Categorizes columns as comparable, tokenized, AWS-only, or unmapped. Note that AWS column names are typically lowercase.

### Function Definition

```python
def check_crosswalk(table_name, columns_df, crosswalk_df, col_map_name):
    import pandas as pd
    #>>> implementation <<<#
    pass
```

### How It Will Be Used

```python
col_map_name = 'table_a'
crosswalk_result = check_crosswalk(table_name, columns_df, crosswalk_df, col_map_name)

print(f"Comparable columns: {len(crosswalk_result['comparable'])}")
print(f"Tokenized columns: {len(crosswalk_result['tokenized'])}")
print(f"AWS-only columns: {len(crosswalk_result['aws_only'])}")
print(f"Unmapped columns: {len(crosswalk_result['unmapped'])}")
```

---

## Step 8: AWS Database Query Function

### What This Step Does

Execute SQL queries against AWS (Athena) database and return results as pandas DataFrame.

### Function Definition

```python
def proc_aws(sql, data_base):
    import pandas as pd
    #>>> implementation <<<#
    pass
```

### How It Will Be Used

```python
sql = "SELECT COUNT(*) as cnt FROM mydb.table_a LIMIT 10"
result = proc_aws(sql, data_base='mydb')
print(result)
```

---

## Step 9: Parse Date Formats (AWS Version)

### What This Step Does

Similar to PCDS DateParser but adapted for AWS data types (date, varchar, timestamp, etc.).

### Class Definition

```python
class DateParser:
    def __init__(self, var_name, data_type):
        self._var = var_name
        self._type = data_type
        self._fmt = None

    def get_fmt(self, table_name, data_base):
        #>>> implementation <<<#
        pass

    def merge_where(self, start_dt, end_dt, where_clause):
        #>>> implementation <<<#
        pass

    def get_cnt(self, table_name, where_clause, data_base):
        #>>> implementation <<<#
        pass

    def to_json(self):
        import json
        #>>> implementation <<<#
        pass
```

---

## Step 10: Supporting Functions

### Function Definitions

```python
def is_missing(value):
    import pandas as pd
    #>>> implementation <<<#
    pass

def parse_date_to_std(date_value):
    #>>> implementation <<<#
    pass

def generate_vintages(min_date, max_date, partition_type, date_parser):
    #>>> implementation <<<#
    pass

def get_vintages(row_counts_df, date_parser, partition_type):
    #>>> implementation <<<#
    pass
```

---

## Step 11: Main Processing Loop

### What This Step Does

Process each table from the tables_df downloaded from S3. The structure mirrors PCDS but uses AWS-specific functions.

### Execute This Step

```python
from operator import itemgetter
import os
from upath import UPath
import pandas as pd

results = []

for _, table in tables_df.iterrows():
    data_base, table_name = table['aws_tbl'].split('.')
    fetch = itemgetter('aws_tbl', 'aws_var', 'aws_where', 'partition', 'col_map', 'start_dt', 'end_dt')
    aws_tbl, date_var, where_clause, partition_type, col_map_name, start_dt, end_dt = fetch(table)
    logger.info(f"Processing {table_name}")

    result = {
        'table': aws_tbl,
        'database': data_base,
        'accessible': check_accessible(data_base, aws_tbl),
        'row_counts': None,
        'crosswalk': None,
        'column_types': None,
        'date_var': None,
        'where_clause': '',
        'vintages': []
    }

    if result['accessible']:
        columns_df = get_columns(data_base, table_name)
        result['crosswalk'] = check_crosswalk(table_name, columns_df, crosswalk_df, col_map_name)

        column_types = dict(zip(columns_df.iloc[:, 0].str.lower(), columns_df.iloc[:, 1]))
        result['column_types'] = column_types

        if date_var and not is_missing(date_var):
            date_parser = DateParser(date_var, column_types[date_var])
            date_parser.get_fmt(aws_tbl, data_base=data_base)
            where_clause = date_parser.merge_where(start_dt, end_dt, where_clause)
            row_counts_df = date_parser.get_cnt(aws_tbl, where_clause, data_base=data_base)
            result['where_clause'] = where_clause
            result['row_counts'] = row_counts_df.to_dict('records')
            result['vintages'] = get_vintages(row_counts_df, date_parser, partition_type)
            result['date_var'] = date_parser.to_json()

    results.append(result)
```

---

## Step 12: Save and Upload Results

### What This Step Does

Save AWS meta check results locally and upload to S3.

### Execute This Step

```python
local_path = os.path.join(output_folder, f'{step_name}.json')
s3.write_json(results, UPath(local_path))
logger.info(f"Saved local copy to {local_path}")

s3_path = s3.write_json(results, f'{step_name}.json')
logger.info(f"Uploaded AWS meta check results to {s3_path}")
```

---

## Putting It All Together

### Complete main() Function

```python
def main():
    from operator import itemgetter
    import os
    from upath import UPath
    import pandas as pd
    from loguru import logger

    run_name, category, config_path = get_env('RUN_NAME', 'CATEGORY', 'META_STEP')

    cfg = load_config(config_path)
    step_name = cfg.output.step_name.format(p='aws')
    output_folder = cfg.output.disk.format(name=run_name)

    add_logger(output_folder, name=step_name)
    logger.info(f"Starting AWS meta check: {run_name} | {category}")

    s3_bucket = cfg.output.s3.format(name=run_name)
    s3 = S3Manager(s3_bucket)

    tables_df = s3.read_df('input_tables.csv')
    crosswalk_df = s3.read_df('crosswalk.csv')
    logger.info(f"Downloaded from S3: {len(tables_df)} tables to validate, {len(crosswalk_df)} crosswalk mappings")

    results = []

    for _, table in tables_df.iterrows():
        data_base, table_name = table['aws_tbl'].split('.')
        fetch = itemgetter('aws_tbl', 'aws_var', 'aws_where', 'partition', 'col_map', 'start_dt', 'end_dt')
        aws_tbl, date_var, where_clause, partition_type, col_map_name, start_dt, end_dt = fetch(table)
        logger.info(f"Processing {table_name}")

        result = {
            'table': aws_tbl,
            'database': data_base,
            'accessible': check_accessible(data_base, aws_tbl),
            'row_counts': None,
            'crosswalk': None,
            'column_types': None,
            'date_var': None,
            'where_clause': '',
            'vintages': []
        }

        if result['accessible']:
            columns_df = get_columns(data_base, table_name)
            result['crosswalk'] = check_crosswalk(table_name, columns_df, crosswalk_df, col_map_name)

            column_types = dict(zip(columns_df.iloc[:, 0].str.lower(), columns_df.iloc[:, 1]))
            result['column_types'] = column_types

            if date_var and not is_missing(date_var):
                date_parser = DateParser(date_var, column_types[date_var])
                date_parser.get_fmt(aws_tbl, data_base=data_base)
                where_clause = date_parser.merge_where(start_dt, end_dt, where_clause)
                row_counts_df = date_parser.get_cnt(aws_tbl, where_clause, data_base=data_base)
                result['where_clause'] = where_clause
                result['row_counts'] = row_counts_df.to_dict('records')
                result['vintages'] = get_vintages(row_counts_df, date_parser, partition_type)
                result['date_var'] = date_parser.to_json()

        results.append(result)

    local_path = os.path.join(output_folder, f'{step_name}.json')
    s3.write_json(results, UPath(local_path))
    logger.info(f"Saved local copy to {local_path}")

    s3_path = s3.write_json(results, f'{step_name}.json')
    logger.info(f"Uploaded AWS meta check results to {s3_path}")

    return results


if __name__ == '__main__':
    main()
```

---

## Summary

This tutorial covered the AWS Meta Check step which:

1. **Downloads configuration from S3** (input_tables.csv, crosswalk.csv)
2. **Validates AWS table accessibility**
3. **Extracts metadata** from AWS/Athena tables
4. **Validates crosswalk mappings** on AWS side
5. **Processes date variables** and generates vintages
6. **Uploads results to S3** for comparison step

The output is used by Meta Compare to validate that PCDS and AWS tables match in structure and data distribution.
