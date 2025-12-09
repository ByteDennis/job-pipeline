# Tutorial: AWS Column Check

## Overview

This tutorial walks through the **AWS Column Check** step, which extracts column-level statistics from AWS (Athena) tables for each vintage, mirroring the PCDS Column Check but using AWS-specific SQL and functions.

### What This Step Produces

- **Column statistics JSON file** for AWS side with the same structure as PCDS

### Prerequisites

1. **Meta Check Comparison completed**
2. **S3 access** to download metadata
3. **AWS/Athena database access**

> **Note**: This tutorial assumes you have AWS query functions available. The PCDS-side functions are not available here.

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

run_name, category, config_path = get_env('RUN_NAME', 'CATEGORY', 'COLUMN_STEP')

cfg = load_config(config_path)
step_name = cfg.output.step_name.format(p='aws')
output_folder = cfg.output.disk.format(name=run_name)

add_logger(output_folder, name=step_name)
logger.info(f"Starting AWS column check: {run_name} | {category}")
```

---

## Step 2: Download Consolidated Metadata

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

consolidated = s3.read_json(f'{cfg.output.summary.format(s="meta")}.json')
validated_tables = consolidated.get('validated_tables', [])

logger.info(f"Processing {len(validated_tables)} validated tables")
```

---

## Step 3: Build Column Statistics SQL (AWS Version)

### What This Step Does

Build Athena/Presto SQL for extracting statistics. Similar to PCDS but uses Athena syntax (STDDEV instead of STDDEV, APPROX_DISTINCT for better performance, etc.).

### Function Definition

```python
def build_column_sql(table_name, col_name, col_type, where_clause, is_oracle=False):
    #>>> implementation <<<#
    pass
```

---

## Step 4: Parse Statistics Row

### Function Definition

```python
def parse_stats_row(row_dict):
    #>>> implementation <<<#
    pass
```

---

## Step 5: Get Statistics for Single Column

### Function Definition

```python
def get_column_stats(args):
    from loguru import logger
    #>>> implementation <<<#
    pass
```

### How It Will Be Used

```python
def proc_aws(sql, data_base):
    import pandas as pd
    #>>> implementation <<<#
    pass

database = 'mydb'
table_name = 'table_a'
col_name = 'txn_amt'
col_type = 'double'
where_clause = "txn_date >= DATE '2023-01-01'"

args = (database, table_name, col_name, col_type, where_clause)
stats = get_column_stats(args)
```

---

## Step 6: Get Statistics for All Columns in a Vintage

### Function Definition

```python
def get_vintage_stats(database, table_name, columns_with_types, vintage):
    #>>> implementation <<<#
    pass
```

---

## Step 7: Main Processing Loop

### What This Step Does

Process each validated table. Note key differences from PCDS:
- Use `aws_table` and `aws_database` from metadata
- Use AWS column names (lowercase) from column_mapping values
- Use `aws_column_types` for type information
- Use `aws_where_clause` from vintages

### Execute This Step

```python
results = []

for table_info in validated_tables:
    aws_table = table_info['aws_table']
    database = table_info['aws_database']
    tbl = aws_table.split('.')[1] if '.' in aws_table else aws_table

    comparable = table_info.get('column_mapping', {}).values()
    aws_types = table_info.get('aws_column_types', {})
    validated_vintages = table_info.get('validated_vintages', [])

    if not comparable:
        logger.warning(f"No comparable columns for {aws_table}")
        continue

    columns_with_types = {col: aws_types.get(col, 'string') for col in comparable}
    logger.info(f"Processing {aws_table}: {len(comparable)} columns, {len(validated_vintages)} vintages")

    table_result = {
        'table': aws_table,
        'columns': list(comparable),
        'vintage_stats': []
    }

    for vintage in validated_vintages:
        logger.info(f"  Vintage {vintage['vintage']}: {len(comparable)} columns")
        where_clause = vintage.get('aws_where_clause', '1=1')

        vintage_obj = {'where_clause': where_clause}
        stats = get_vintage_stats(database, tbl, columns_with_types, vintage_obj)

        table_result['vintage_stats'].append({
            'vintage': vintage['vintage'],
            'start_date': vintage['start_date'],
            'end_date': vintage['end_date'],
            'stats': stats
        })

    results.append(table_result)
```

---

## Step 8: Save and Upload Results

### Execute This Step

```python
local_path = os.path.join(output_folder, f'{step_name}.json')
s3.write_json(results, UPath(local_path))
logger.info(f"Saved local copy to {local_path}")

s3_path = s3.write_json(results, f'{step_name}.json')
logger.info(f"Uploaded AWS column stats to {s3_path}")
```

---

## Putting It All Together

### Complete main() Function

```python
def main():
    import os
    from upath import UPath
    from loguru import logger

    run_name, category, config_path = get_env('RUN_NAME', 'CATEGORY', 'COLUMN_STEP')

    cfg = load_config(config_path)
    step_name = cfg.output.step_name.format(p='aws')
    output_folder = cfg.output.disk.format(name=run_name)

    add_logger(output_folder, name=step_name)
    logger.info(f"Starting AWS column check: {run_name} | {category}")

    s3_bucket = cfg.output.s3.format(name=run_name)
    s3 = S3Manager(s3_bucket)

    consolidated = s3.read_json(f'{cfg.output.summary.format(s="meta")}.json')
    validated_tables = consolidated.get('validated_tables', [])
    logger.info(f"Processing {len(validated_tables)} validated tables")

    results = []

    for table_info in validated_tables:
        aws_table = table_info['aws_table']
        database = table_info['aws_database']
        tbl = aws_table.split('.')[1] if '.' in aws_table else aws_table

        comparable = table_info.get('column_mapping', {}).values()
        aws_types = table_info.get('aws_column_types', {})
        validated_vintages = table_info.get('validated_vintages', [])

        if not comparable:
            logger.warning(f"No comparable columns for {aws_table}")
            continue

        columns_with_types = {col: aws_types.get(col, 'string') for col in comparable}
        logger.info(f"Processing {aws_table}: {len(comparable)} columns, {len(validated_vintages)} vintages")

        table_result = {
            'table': aws_table,
            'columns': list(comparable),
            'vintage_stats': []
        }

        for vintage in validated_vintages:
            logger.info(f"  Vintage {vintage['vintage']}: {len(comparable)} columns")
            where_clause = vintage.get('aws_where_clause', '1=1')

            vintage_obj = {'where_clause': where_clause}
            stats = get_vintage_stats(database, tbl, columns_with_types, vintage_obj)

            table_result['vintage_stats'].append({
                'vintage': vintage['vintage'],
                'start_date': vintage['start_date'],
                'end_date': vintage['end_date'],
                'stats': stats
            })

        results.append(table_result)

    local_path = os.path.join(output_folder, f'{step_name}.json')
    s3.write_json(results, UPath(local_path))
    logger.info(f"Saved local copy to {local_path}")

    s3_path = s3.write_json(results, f'{step_name}.json')
    logger.info(f"Uploaded AWS column stats to {s3_path}")

    return results


if __name__ == '__main__':
    main()
```

---

## Summary

This tutorial covered the AWS Column Check step which mirrors PCDS Column Check but for AWS tables, using Athena SQL syntax and AWS data types.
