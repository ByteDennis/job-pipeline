# Tutorial: PCDS Column Check

## Overview

This tutorial walks through the **PCDS Column Check** step, which extracts column-level statistics from PCDS tables for each vintage. These statistics include count, distinct values, min/max, averages, and frequency distributions that will be compared with AWS to validate data accuracy.

### What This Step Produces

- **Column statistics JSON file** containing for each table and vintage:
  - Count (total rows)
  - Distinct (unique values)
  - Min/Max values
  - Average and standard deviation (numeric columns)
  - Missing value count
  - Top 10 frequency distribution (categorical columns)

### Prerequisites

1. **Meta Check Comparison completed** - meta_check.json with validated tables and vintages
2. **S3 access** to download metadata
3. **PCDS database access**

---

## Environment Variables Required

```bash
RUN_NAME=my_validation_run
CATEGORY=dpst
COLUMN_STEP=/path/to/config.toml
```

> **Note**: This step uses COLUMN_STEP instead of META_STEP for the config path.

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
step_name = cfg.output.step_name.format(p='pcds')
output_folder = cfg.output.disk.format(name=run_name)

add_logger(output_folder, name=step_name)
logger.info(f"Starting PCDS column check: {run_name} | {category}")
```

---

## Step 2: Download Consolidated Metadata from S3

### What This Step Does

Download the consolidated metadata produced by Meta Check Comparison. This contains validated tables, column mappings, and validated vintages to process.

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

## Step 3: Build Column Statistics SQL

### What This Step Does

Build SQL query to extract statistics for a single column. The SQL differs based on column type:
- **Categorical** (string/varchar): Count, distinct, min, max, missing, top 10 frequencies
- **Continuous** (numeric/date): Count, distinct, min, max, average, std deviation, missing

### Function Definition

```python
def build_column_sql(table_name, col_name, col_type, where_clause, is_oracle=True):
    #>>> implementation <<<#
    pass
```

### How It Will Be Used

```python
table_name = 'SERVICE1.TABLE_A'
col_name = 'TXN_AMT'
col_type = 'NUMBER'
where_clause = "TXN_DATE >= DATE '2023-01-01'"

sql = build_column_sql(table_name, col_name, col_type, where_clause, is_oracle=True)
print(sql)
```

### Expected Output (Example)

```sql
SELECT
    COUNT(*) as count,
    COUNT(DISTINCT TXN_AMT) as distinct,
    MIN(TXN_AMT) as min,
    MAX(TXN_AMT) as max,
    AVG(TXN_AMT) as avg,
    STDDEV(TXN_AMT) as std,
    SUM(CASE WHEN TXN_AMT IS NULL THEN 1 ELSE 0 END) as missing
FROM SERVICE1.TABLE_A
WHERE TXN_DATE >= DATE '2023-01-01'
```

---

## Step 4: Parse Statistics Row

### What This Step Does

Parse the result row from the statistics query into a structured dictionary.

### Function Definition

```python
def parse_stats_row(row_dict):
    #>>> implementation <<<#
    pass
```

### How It Will Be Used

```python
row = {
    'COUNT': 10000,
    'DISTINCT': 5000,
    'MIN': 10.5,
    'MAX': 9999.99,
    'AVG': 150.25,
    'STD': 75.5,
    'MISSING': 0
}

stats = parse_stats_row(row)
print(stats)
```

### Expected Output

```python
{
    'count': 10000,
    'distinct': 5000,
    'min': 10.5,
    'max': 9999.99,
    'avg': 150.25,
    'std': 75.5,
    'missing': 0
}
```

---

## Step 5: Get Statistics for Single Column

### What This Step Does

Execute the statistics query for a single column and return parsed results.

### Function Definition

```python
def get_column_stats(args):
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
col_name = 'TXN_AMT'
col_type = 'NUMBER'
where_clause = "TXN_DATE >= DATE '2023-01-01'"

args = (svc, table_name, col_name, col_type, where_clause)
stats = get_column_stats(args)
print(stats)
```

---

## Step 6: Get Statistics for All Columns in a Vintage

### What This Step Does

Process all columns for a single vintage sequentially (production mode, no parallel execution). Build statistics for each column and return a dictionary mapping column names to their statistics.

### Function Definition

```python
def get_vintage_stats(svc, table_name, columns_with_types, vintage):
    #>>> implementation <<<#
    pass
```

### How It Will Be Used

```python
svc = 'service1'
table_name = 'TABLE_A'
columns_with_types = {
    'CUST_ID': 'NUMBER',
    'TXN_DATE': 'DATE',
    'TXN_AMT': 'NUMBER',
    'DESCRIPTION': 'VARCHAR2'
}
vintage = {
    'where_clause': "TXN_DATE >= DATE '2023-01-01' AND TXN_DATE <= DATE '2023-01-31'"
}

stats = get_vintage_stats(svc, table_name, columns_with_types, vintage)
print(f"Collected stats for {len(stats)} columns")
```

---

## Step 7: Main Processing Loop

### What This Step Does

Process each validated table:
1. Get list of comparable columns from metadata
2. Get column types from metadata
3. For each vintage:
   - Build where clause
   - Get statistics for all columns
   - Store results

### Execute This Step

```python
results = []

for table_info in validated_tables:
    pcds_table = table_info['pcds_table']
    svc = table_info['pcds_svc']
    comparable = table_info.get('column_mapping', {}).keys()
    pcds_types = table_info.get('pcds_column_types', {})
    validated_vintages = table_info.get('validated_vintages', [])

    if not comparable:
        logger.warning(f"No comparable columns for {pcds_table}")
        continue

    columns_with_types = {col: pcds_types.get(col, 'VARCHAR2') for col in comparable}
    logger.info(f"Processing {pcds_table}: {len(comparable)} columns, {len(validated_vintages)} vintages")

    table_result = {
        'table': pcds_table,
        'columns': list(comparable),
        'vintage_stats': []
    }

    for vintage in validated_vintages:
        logger.info(f"  Vintage {vintage['vintage']}: {len(comparable)} columns")
        where_clause = vintage.get('pcds_where_clause', '1=1')

        vintage_obj = {'where_clause': where_clause}
        stats = get_vintage_stats(svc, pcds_table, columns_with_types, vintage_obj)

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

### What This Step Does

Save column statistics locally and upload to S3 for AWS step and comparison step.

### Execute This Step

```python
local_path = os.path.join(output_folder, f'{step_name}.json')
s3.write_json(results, UPath(local_path))
logger.info(f"Saved local copy to {local_path}")

s3_path = s3.write_json(results, f'{step_name}.json')
logger.info(f"Uploaded PCDS column stats to {s3_path}")
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
    step_name = cfg.output.step_name.format(p='pcds')
    output_folder = cfg.output.disk.format(name=run_name)

    add_logger(output_folder, name=step_name)
    logger.info(f"Starting PCDS column check: {run_name} | {category}")

    s3_bucket = cfg.output.s3.format(name=run_name)
    s3 = S3Manager(s3_bucket)

    consolidated = s3.read_json(f'{cfg.output.summary.format(s="meta")}.json')
    validated_tables = consolidated.get('validated_tables', [])
    logger.info(f"Processing {len(validated_tables)} validated tables")

    results = []

    for table_info in validated_tables:
        pcds_table = table_info['pcds_table']
        svc = table_info['pcds_svc']
        comparable = table_info.get('column_mapping', {}).keys()
        pcds_types = table_info.get('pcds_column_types', {})
        validated_vintages = table_info.get('validated_vintages', [])

        if not comparable:
            logger.warning(f"No comparable columns for {pcds_table}")
            continue

        columns_with_types = {col: pcds_types.get(col, 'VARCHAR2') for col in comparable}
        logger.info(f"Processing {pcds_table}: {len(comparable)} columns, {len(validated_vintages)} vintages")

        table_result = {
            'table': pcds_table,
            'columns': list(comparable),
            'vintage_stats': []
        }

        for vintage in validated_vintages:
            logger.info(f"  Vintage {vintage['vintage']}: {len(comparable)} columns")
            where_clause = vintage.get('pcds_where_clause', '1=1')

            vintage_obj = {'where_clause': where_clause}
            stats = get_vintage_stats(svc, pcds_table, columns_with_types, vintage_obj)

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
    logger.info(f"Uploaded PCDS column stats to {s3_path}")

    return results


if __name__ == '__main__':
    main()
```

---

## Summary

This tutorial covered the PCDS Column Check step which:

1. **Downloads validated metadata** from Meta Check Comparison
2. **Extracts column statistics** for each comparable column
3. **Processes each vintage** sequentially
4. **Uploads results to S3** for AWS comparison

The statistics are used by Column Check Comparison to identify which columns match between PCDS and AWS and which need investigation.
