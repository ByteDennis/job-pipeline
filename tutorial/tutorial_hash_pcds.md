# Tutorial: PCDS Hash Check

## Overview

This tutorial walks through the **PCDS Hash Check** step, which computes row-level hashes for clean columns in each vintage. These hashes allow us to detect mismatched rows between PCDS and AWS at the individual row level.

### What This Step Produces

- **Hash results JSON** containing for each table/vintage:
  - Total rows processed
  - Unique hashes count
  - Hash values with key columns for row identification

### Prerequisites

1. **Column Check Comparison completed** - column_check.json with clean columns and key columns
2. **S3 access**
3. **PCDS database access**

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
step_name = cfg.output.step_name.format(p='pcds')
output_folder = cfg.output.disk.format(name=run_name)

add_logger(output_folder, name=step_name)
logger.info(f"Starting PCDS hash check: {run_name} | {category}")
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

## Step 3: Build Oracle Hash Expression

### What This Step Does

Build Oracle SQL expression to compute a hash over multiple columns. The hash is built by:
1. Converting each column to string
2. Handling NULL values (convert to 'NULL')
3. Concatenating all column strings
4. Computing MD5 or SHA hash

### Function Definition

```python
def build_oracle_hash_expr(col_specs):
    #>>> implementation <<<#
    pass
```

### How It Will Be Used

```python
col_specs = [
    {'column_name': 'CUST_ID', 'data_type': 'NUMBER'},
    {'column_name': 'TXN_DATE', 'data_type': 'DATE'},
    {'column_name': 'TXN_AMT', 'data_type': 'NUMBER'}
]

hash_result = build_oracle_hash_expr(col_specs)

print("Hash expression:", hash_result['hash_expr'])
```

### Expected Output

```
Hash expression: RAWTOHEX(DBMS_CRYPTO.HASH(
    NVL(TO_CHAR(CUST_ID), 'NULL') || '|' ||
    NVL(TO_CHAR(TXN_DATE, 'YYYY-MM-DD HH24:MI:SS'), 'NULL') || '|' ||
    NVL(TO_CHAR(TXN_AMT), 'NULL'),
    2
))
```

---

## Step 4: Compute Hash for Single Vintage

### What This Step Does

For one vintage of a table:
1. Build hash SQL expression
2. Select key columns + hash value
3. Execute query
4. Return results (total rows, unique hashes, hash data)

### Function Definition

```python
def compute_vintage_hash(args):
    from loguru import logger
    #>>> implementation <<<#
    pass
```

### How It Will Be Used

```python
def proc_pcds(sql, service_name):
    import pandas as pd
    #>>> implementation <<<#
    pass

svc = 'service1'
table_name = 'TABLE_A'
columns_with_types = {'CUST_ID': 'NUMBER', 'TXN_AMT': 'NUMBER'}
key_columns = ['CUST_ID']
vintage = {'where_clause': "TXN_DATE >= DATE '2023-01-01'"}

args = (svc, table_name, columns_with_types, key_columns, vintage, False)
result = compute_vintage_hash(args)

print(f"Total rows: {result['total_rows']}")
print(f"Unique hashes: {result['unique_hashes']}")
```

---

## Step 5: Compute Hashes for All Vintages (Sequential)

### What This Step Does

Process all vintages for a table sequentially (production mode, no parallel execution). Loop through vintages and compute hash for each.

### Function Definition

```python
def compute_table_hashes(svc, table_name, columns_with_types, key_columns, vintages):
    #>>> implementation <<<#
    pass
```

### How It Will Be Used

```python
vintages = [
    {'where_clause': "TXN_DATE >= DATE '2023-01-01' AND TXN_DATE < DATE '2023-02-01'"},
    {'where_clause': "TXN_DATE >= DATE '2023-02-01' AND TXN_DATE < DATE '2023-03-01'"}
]

results = compute_table_hashes(svc, table_name, columns_with_types, key_columns, vintages)

for i, result in enumerate(results):
    print(f"Vintage {i+1}: {result['total_rows']} rows, {result['unique_hashes']} unique hashes")
```

---

## Step 6: Main Processing Loop

### What This Step Does

Process each validated table:
1. Get clean columns (where statistics matched)
2. Get key columns (for row identification)
3. Get column types
4. For each vintage:
   - Compute hashes for all rows
   - Store results

### Execute This Step

```python
results = []

for table_info in validated_tables:
    pcds_table = table_info['pcds_table']
    svc = table_info['pcds_svc']
    clean_columns = table_info.get('clean_columns', [])
    key_columns = table_info.get('key_columns', [])
    pcds_types = table_info.get('clean_pcds_column_types', {})
    validated_vintages = table_info.get('validated_vintages', [])

    if not clean_columns:
        logger.warning(f"No clean columns for {pcds_table}")
        continue

    columns_with_types = {col: pcds_types.get(col, 'VARCHAR2') for col in clean_columns}
    logger.info(f"Processing {pcds_table}: {len(clean_columns)} clean columns, key: {key_columns}")

    table_result = {
        'table': pcds_table,
        'clean_columns': clean_columns,
        'key_columns': key_columns,
        'mismatched_columns': table_info.get('mismatched_columns', []),
        'vintage_hashes': []
    }

    vintage_objs = [{'where_clause': v.get('pcds_where_clause', '1=1')} for v in validated_vintages]
    hash_results = compute_table_hashes(svc, pcds_table, columns_with_types, key_columns, vintage_objs)

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
logger.info(f"Uploaded PCDS hash check results to {s3_path}")
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
    step_name = cfg.output.step_name.format(p='pcds')
    output_folder = cfg.output.disk.format(name=run_name)

    add_logger(output_folder, name=step_name)
    logger.info(f"Starting PCDS hash check: {run_name} | {category}")

    s3_bucket = cfg.output.s3.format(name=run_name)
    s3 = S3Manager(s3_bucket)

    consolidated = s3.read_json(f'{cfg.output.summary.format(s="column")}.json')
    validated_tables = consolidated.get('validated_tables', [])
    logger.info(f"Processing {len(validated_tables)} validated tables")

    results = []

    for table_info in validated_tables:
        pcds_table = table_info['pcds_table']
        svc = table_info['pcds_svc']
        clean_columns = table_info.get('clean_columns', [])
        key_columns = table_info.get('key_columns', [])
        pcds_types = table_info.get('clean_pcds_column_types', {})
        validated_vintages = table_info.get('validated_vintages', [])

        if not clean_columns:
            logger.warning(f"No clean columns for {pcds_table}")
            continue

        columns_with_types = {col: pcds_types.get(col, 'VARCHAR2') for col in clean_columns}
        logger.info(f"Processing {pcds_table}: {len(clean_columns)} clean columns, key: {key_columns}")

        table_result = {
            'table': pcds_table,
            'clean_columns': clean_columns,
            'key_columns': key_columns,
            'mismatched_columns': table_info.get('mismatched_columns', []),
            'vintage_hashes': []
        }

        vintage_objs = [{'where_clause': v.get('pcds_where_clause', '1=1')} for v in validated_vintages]
        hash_results = compute_table_hashes(svc, pcds_table, columns_with_types, key_columns, vintage_objs)

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
    logger.info(f"Uploaded PCDS hash check results to {s3_path}")

    return results


if __name__ == '__main__':
    main()
```

---

## Summary

This tutorial covered the PCDS Hash Check step which:

1. **Downloads column check metadata** with clean columns
2. **Builds hash expressions** for Oracle
3. **Computes row hashes** for each vintage
4. **Uploads results to S3** for AWS comparison

The hashes enable row-level comparison to identify specific rows that differ between PCDS and AWS.
