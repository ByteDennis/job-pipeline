# Tutorial: AWS Hash Check

## Overview

This tutorial walks through the **AWS Hash Check** step, which computes row-level hashes for AWS tables, mirroring the PCDS Hash Check but using Athena/Presto hash functions.

### What This Step Produces

- **Hash results JSON** for AWS side with same structure as PCDS

### Prerequisites

1. **Column Check Comparison completed**
2. **S3 access**
3. **AWS/Athena database access**

> **Note**: This tutorial assumes you have AWS query functions available.

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

run_name, category, config_path = get_env('RUN_NAME', 'CATEGORY', 'HASH_STEP')

cfg = load_config(config_path)
step_name = cfg.output.step_name.format(p='aws')
output_folder = cfg.output.disk.format(name=run_name)

add_logger(output_folder, name=step_name)
logger.info(f"Starting AWS hash check: {run_name} | {category}")
```

---

## Step 2: Download Consolidated Column Check Metadata

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

consolidated = s3.read_json(f'{cfg.output.summary.format(s="column")}.json')
validated_tables = consolidated.get('validated_tables', [])

logger.info(f"Processing {len(validated_tables)} validated tables")
```

---

## Step 3: Build Athena Hash Expression

### What This Step Does

Build Athena/Presto SQL expression to compute hash. Uses MD5 or SHA functions available in Athena.

### Function Definition

```python
def build_athena_hash_expr(col_specs):
    #>>> implementation <<<#
    pass
```

### How It Will Be Used

```python
col_specs = [
    {'column_name': 'customer_id', 'data_type': 'bigint'},
    {'column_name': 'txn_date', 'data_type': 'date'},
    {'column_name': 'txn_amt', 'data_type': 'double'}
]

hash_result = build_athena_hash_expr(col_specs)

print("Hash expression:", hash_result['hash_expr'])
```

### Expected Output

```
Hash expression: to_hex(md5(to_utf8(
    COALESCE(CAST(customer_id AS VARCHAR), 'NULL') || '|' ||
    COALESCE(CAST(txn_date AS VARCHAR), 'NULL') || '|' ||
    COALESCE(CAST(txn_amt AS VARCHAR), 'NULL')
)))
```

---

## Step 4: Compute Hash for Single Vintage

### Function Definition

```python
def compute_vintage_hash(args):
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
columns_with_types = {'customer_id': 'bigint', 'txn_amt': 'double'}
key_columns = ['customer_id']
vintage = {'where_clause': "txn_date >= DATE '2023-01-01'"}

args = (database, table_name, columns_with_types, key_columns, vintage, False)
result = compute_vintage_hash(args)
```

---

## Step 5: Compute Hashes for All Vintages (Sequential)

### Function Definition

```python
def compute_table_hashes(database, table_name, columns_with_types, key_columns, vintages):
    #>>> implementation <<<#
    pass
```

---

## Step 6: Main Processing Loop

### What This Step Does

Process each table. Key differences from PCDS:
- Use `aws_table` and `aws_database`
- Use AWS column names (lowercase)
- Use `clean_aws_column_types`
- Use `aws_where_clause`

### Execute This Step

```python
results = []

for table_info in validated_tables:
    aws_table = table_info['aws_table']
    database = table_info['aws_database']
    tbl = aws_table.split('.')[1] if '.' in aws_table else aws_table

    clean_columns = table_info.get('clean_columns', [])
    key_columns = table_info.get('key_columns', [])
    aws_types = table_info.get('clean_aws_column_types', {})
    validated_vintages = table_info.get('validated_vintages', [])

    if not clean_columns:
        logger.warning(f"No clean columns for {aws_table}")
        continue

    columns_with_types = {col: aws_types.get(col, 'string') for col in clean_columns}
    logger.info(f"Processing {aws_table}: {len(clean_columns)} clean columns, key: {key_columns}")

    table_result = {
        'table': aws_table,
        'clean_columns': clean_columns,
        'key_columns': key_columns,
        'mismatched_columns': table_info.get('mismatched_columns', []),
        'vintage_hashes': []
    }

    vintage_objs = [{'where_clause': v.get('aws_where_clause', '1=1')} for v in validated_vintages]
    hash_results = compute_table_hashes(database, tbl, columns_with_types, key_columns, vintage_objs)

    for vintage, hash_data in zip(validated_vintages, hash_results):
        logger.info(f"  Vintage {vintage['vintage']}: {hash_data.get('total_rows', 'N/A')} rows")
        table_result['vintage_hashes'].append({
            'vintage': vintage['vintage'],
            'start_date': vintage['start_date'],
            'end_date': vintage['end_date'],
            'hash_data': hash_data
        })

    results.append(table_result)
```

---

## Step 7: Save and Upload Results

### Execute This Step

```python
local_path = os.path.join(output_folder, f'{step_name}.json')
s3.write_json(results, UPath(local_path))
logger.info(f"Saved local copy to {local_path}")

s3_path = s3.write_json(results, f'{step_name}.json')
logger.info(f"Uploaded AWS hash check results to {s3_path}")
```

---

## Putting It All Together

### Complete main() Function

```python
def main():
    import os
    from upath import UPath
    from loguru import logger

    run_name, category, config_path = get_env('RUN_NAME', 'CATEGORY', 'HASH_STEP')

    cfg = load_config(config_path)
    step_name = cfg.output.step_name.format(p='aws')
    output_folder = cfg.output.disk.format(name=run_name)

    add_logger(output_folder, name=step_name)
    logger.info(f"Starting AWS hash check: {run_name} | {category}")

    s3_bucket = cfg.output.s3.format(name=run_name)
    s3 = S3Manager(s3_bucket)

    consolidated = s3.read_json(f'{cfg.output.summary.format(s="column")}.json')
    validated_tables = consolidated.get('validated_tables', [])
    logger.info(f"Processing {len(validated_tables)} validated tables")

    results = []

    for table_info in validated_tables:
        aws_table = table_info['aws_table']
        database = table_info['aws_database']
        tbl = aws_table.split('.')[1] if '.' in aws_table else aws_table

        clean_columns = table_info.get('clean_columns', [])
        key_columns = table_info.get('key_columns', [])
        aws_types = table_info.get('clean_aws_column_types', {})
        validated_vintages = table_info.get('validated_vintages', [])

        if not clean_columns:
            logger.warning(f"No clean columns for {aws_table}")
            continue

        columns_with_types = {col: aws_types.get(col, 'string') for col in clean_columns}
        logger.info(f"Processing {aws_table}: {len(clean_columns)} clean columns, key: {key_columns}")

        table_result = {
            'table': aws_table,
            'clean_columns': clean_columns,
            'key_columns': key_columns,
            'mismatched_columns': table_info.get('mismatched_columns', []),
            'vintage_hashes': []
        }

        vintage_objs = [{'where_clause': v.get('aws_where_clause', '1=1')} for v in validated_vintages]
        hash_results = compute_table_hashes(database, tbl, columns_with_types, key_columns, vintage_objs)

        for vintage, hash_data in zip(validated_vintages, hash_results):
            logger.info(f"  Vintage {vintage['vintage']}: {hash_data.get('total_rows', 'N/A')} rows")
            table_result['vintage_hashes'].append({
                'vintage': vintage['vintage'],
                'start_date': vintage['start_date'],
                'end_date': vintage['end_date'],
                'hash_data': hash_data
            })

        results.append(table_result)

    local_path = os.path.join(output_folder, f'{step_name}.json')
    s3.write_json(results, UPath(local_path))
    logger.info(f"Saved local copy to {local_path}")

    s3_path = s3.write_json(results, f'{step_name}.json')
    logger.info(f"Uploaded AWS hash check results to {s3_path}")

    return results


if __name__ == '__main__':
    main()
```

---

## Summary

This tutorial covered the AWS Hash Check step which mirrors PCDS Hash Check but for AWS tables using Athena hash functions.
