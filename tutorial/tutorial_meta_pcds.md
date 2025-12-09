# Tutorial: PCDS Meta Check

## Overview

This tutorial walks through the **PCDS Meta Check** step, which is the first step in the data validation pipeline. This step validates PCDS (Oracle) database tables by checking their accessibility, extracting metadata about columns and row counts, and generating date-based partitions (vintages) for downstream processing.

### What This Step Produces

- **Metadata JSON file** containing:
  - Table accessibility status
  - Column names and data types
  - Crosswalk mapping validation (which columns map between PCDS and AWS)
  - Row counts grouped by date variable
  - Date vintages (partitions) for data extraction
- **Uploaded to S3**:
  - Filtered input tables CSV
  - Filtered crosswalk CSV
  - Meta check results JSON

### Prerequisites

Before running this tutorial, ensure you have:

1. **Environment variables** configured
2. **TOML configuration file** with paths to input files
3. **Input Excel files**:
   - Tables definition file (which tables to process)
   - Column mappings file (crosswalk between PCDS and AWS columns)
4. **Database access** to PCDS (Oracle) databases
5. **S3 bucket** for storing results

---

## Environment Variables Required

Set these environment variables before running:

```bash
RUN_NAME=my_validation_run     # Unique identifier for this run
CATEGORY=dpst                   # Data category: 'dpst' or 'loan'
META_STEP=/path/to/config.toml  # Path to TOML configuration file
```

---

## Step 1: Load Environment Variables

### What This Step Does

The first step retrieves environment variables that control the execution. These variables tell us which run we're executing, what data category to process, and where to find the configuration file.

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

### Expected Output

```
Run: my_validation_run, Category: dpst, Config: /path/to/config.toml
```

---

## Step 2: Load Configuration from TOML File

### What This Step Does

The configuration file (TOML format) contains all paths and settings needed for the pipeline. This includes where to find input Excel files, where to save output, S3 bucket paths, and more. We load this file and parse it into a configuration object that we can easily access throughout the script.

### Function Definition

```python
def load_config(config_path):
    #>>> implementation <<<#
    pass
```

### Execute This Step

```python
cfg = load_config(config_path)

step_name = cfg.output.step_name.format(p='pcds')
output_folder = cfg.output.disk.format(name=run_name)

print(f"Step name: {step_name}")
print(f"Output folder: {output_folder}")
```

### Expected Output

```
Step name: meta_check_pcds
Output folder: output/my_validation_run
```

---

## Step 3: Setup Logging

### What This Step Does

Logging helps us track what the script is doing and debug any issues. This step creates the output folder (if it doesn't exist) and configures the logger to write all events to a log file.

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
logger.info(f"Starting PCDS meta check: {run_name} | {category}")
```

### Expected Output

```
2024-01-15 10:30:45 | INFO | Starting PCDS meta check: my_validation_run | dpst
```

---

## Step 4: Read Input Tables from Excel

### What This Step Does

The input tables Excel file defines which database tables we need to validate. Each row specifies a table name, date variable, partition type, and other metadata. We read this Excel file and load it into a pandas DataFrame for processing.

### Function Definition

```python
def read_input_tables(table_config):
    import pandas as pd
    #>>> implementation <<<#
    pass
```

### Execute This Step

```python
import pandas as pd

tables_df = read_input_tables(cfg.table)
print(f"Loaded {len(tables_df)} tables")
print(tables_df.head())
```

### Expected Output

```
Loaded 15 tables
  pcds_tbl              pcds_var    partition  enabled  ...
0 service1.table_a     TXN_DATE    month      True     ...
1 service1.table_b     POST_DATE   year       True     ...
...
```

---

## Step 5: Load Column Mappings (Crosswalk) from Excel

### What This Step Does

The crosswalk Excel file maps columns between PCDS (Oracle) and AWS (Athena) tables. It tells us which columns should match, which are tokenized (hashed for privacy), and which exist only on one side. We read this file filtered by the current category (dpst or loan).

### Function Definition

```python
def load_column_mappings(column_maps_config, category):
    import pandas as pd
    #>>> implementation <<<#
    pass
```

### Execute This Step

```python
crosswalk_df = load_column_mappings(cfg.column_maps, category)
print(f"Loaded {len(crosswalk_df)} column mappings")
print(crosswalk_df.head())
```

### Expected Output

```
Loaded 450 column mappings
  pcds_col      aws_col       is_tokenized  col_map    ...
0 CUST_ID       customer_id   False         table_a    ...
1 SSN           ssn_hash      True          table_a    ...
...
```

---

## Step 6: Filter for Enabled Tables Only

### What This Step Does

Not all tables in the input file need to be processed in every run. Tables have an "enabled" flag that indicates whether they should be included. We filter to only enabled tables and then filter the crosswalk to match only those tables.

### Execute This Step

```python
enabled_tables = tables_df[tables_df['enabled']].copy()
logger.info(f"Going to validate {len(enabled_tables)} tables out of {len(tables_df)} total")

filtered_crosswalk = crosswalk_df[
    crosswalk_df["col_map"].str.lower().isin(
        (
            enabled_tables["col_map"]
            if "col_map" in enabled_tables.columns
            else enabled_tables["pcds_tbl"].str.extract(r"([^.]+)$", expand=False)
        ).str.lower().unique()
    )
].copy()

print(f"Filtered crosswalk has {len(filtered_crosswalk)} mappings")
```

### Expected Output

```
2024-01-15 10:30:46 | INFO | Going to validate 12 tables out of 15 total
Filtered crosswalk has 380 mappings
```

---

## Step 7: Initialize S3 Manager

### What This Step Does

We'll be uploading results to Amazon S3 for storage and for downstream steps to access. This step initializes the S3 manager with the bucket path from our configuration.

### Class Definition

```python
class S3Manager:
    def __init__(self, s3_bucket):
        #>>> implementation <<<#
        pass

    def write_df(self, dataframe, filename):
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
print(f"S3 bucket: {s3_bucket}")
```

### Expected Output

```
S3 bucket: s3://my-bucket/validations/my_validation_run
```

---

## Step 8: Upload Configuration Files to S3

### What This Step Does

We upload the filtered input tables and crosswalk to S3 so that other machines (running AWS and compare steps) can access the same configuration. This ensures all steps work with the same set of tables and mappings.

### Execute This Step

```python
s3.write_df(enabled_tables, 'input_tables.csv')
s3.write_df(filtered_crosswalk, 'crosswalk.csv')
logger.info(f"Uploaded input_tables.csv and crosswalk.csv to S3")
```

### Expected Output

```
2024-01-15 10:30:47 | INFO | Uploaded input_tables.csv and crosswalk.csv to S3
```

---

## Step 9: Check Table Accessibility

### What This Step Does

Before we can extract metadata from a table, we need to verify that the table exists and we have permission to query it. This function attempts to select one row from the table. If it succeeds, the table is accessible. If it fails, we log the error and mark the table as inaccessible.

### Function Definition

```python
def check_accessible(conn, table_name):
    from loguru import logger
    #>>> implementation <<<#
    pass
```

### How It Will Be Used

```python
service_name = 'service1'
table_name = 'TABLE_A'
is_accessible = check_accessible(service_name, table_name)
print(f"Table accessible: {is_accessible}")
```

### Expected Output

```
Table accessible: True
```

---

## Step 10: Get Table Columns and Data Types

### What This Step Does

For accessible tables, we need to know what columns exist and their data types. This information is critical for:
- Building SQL queries
- Detecting date formats (DATE vs VARCHAR2)
- Validating crosswalk completeness

This function queries Oracle's system catalog (all_tab_columns) to retrieve column metadata.

### Function Definition

```python
def get_columns(service_name, table_name):
    #>>> implementation <<<#
    pass
```

### How It Will Be Used

```python
columns_df = get_columns(service_name, table_name)
print(f"Found {len(columns_df)} columns")
print(columns_df.head())
```

### Expected Output

```
Found 45 columns
  COLUMN_NAME        DATA_TYPE
0 CUST_ID            NUMBER
1 TXN_DATE           DATE
2 TXN_AMT            NUMBER
3 DESCRIPTION        VARCHAR2
...
```

---

## Step 11: Check Crosswalk Mapping Completeness

### What This Step Does

Now that we know which columns exist in the table, we need to validate the crosswalk mappings. This function categorizes each column as:

- **Comparable**: Has a mapping to AWS and can be compared
- **Tokenized**: Exists in both but is hashed/encrypted (can't compare values directly)
- **PCDS-only**: Exists only in PCDS, no AWS equivalent
- **Unmapped**: Exists in the table but not mentioned in the crosswalk

This validation helps us understand data coverage and identify missing mappings.

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
crosswalk_result = check_crosswalk(table_name, columns_df, filtered_crosswalk, col_map_name)

print(f"Comparable columns: {len(crosswalk_result['comparable'])}")
print(f"Tokenized columns: {len(crosswalk_result['tokenized'])}")
print(f"PCDS-only columns: {len(crosswalk_result['pcds_only'])}")
print(f"Unmapped columns: {len(crosswalk_result['unmapped'])}")
```

### Expected Output

```
Comparable columns: 38
Tokenized columns: 3
PCDS-only columns: 2
Unmapped columns: 2
```

---

## Step 12: Database Query Function

### What This Step Does

We need a function to execute SQL queries against the PCDS (Oracle) database and return results as a pandas DataFrame. This function handles connection management, query execution, and result formatting.

### Function Definition

```python
def proc_pcds(sql, service_name):
    import pandas as pd
    #>>> implementation <<<#
    pass
```

### How It Will Be Used

```python
sql = "SELECT COUNT(*) as cnt FROM service1.table_a WHERE ROWNUM <= 10"
result = proc_pcds(sql, service_name='service1')
print(result)
```

### Expected Output

```
   cnt
0   10
```

---

## Step 13: Parse Date Formats

### What This Step Does

Tables often have date columns that we use for partitioning and filtering data. However, date columns can be stored as either:
- Oracle DATE type (native date)
- VARCHAR2 type (string in various formats like 'YYYY-MM-DD', 'DD-MON-YY', etc.)

We need to detect the format so we can properly parse dates for comparisons. This involves sampling a row from the table and analyzing the date value.

### Class Definition

```python
class DateParser:
    def __init__(self, var_name, data_type):
        self._var = var_name
        self._type = data_type
        self._fmt = None

    def get_fmt(self, table_name, service_name):
        #>>> implementation <<<#
        pass

    def merge_where(self, start_dt, end_dt, where_clause):
        #>>> implementation <<<#
        pass

    def get_cnt(self, table_name, where_clause, service_name):
        #>>> implementation <<<#
        pass

    def to_json(self):
        import json
        #>>> implementation <<<#
        pass
```

### How It Will Be Used

```python
date_var = 'TXN_DATE'
column_types = {'TXN_DATE': 'DATE', 'CUST_ID': 'NUMBER'}

date_parser = DateParser(date_var, column_types[date_var])
date_parser.get_fmt(table_name, service_name)
print(f"Date format detected: {date_parser._fmt}")
```

### Expected Output

```
Date format detected: None
```
(None means it's a native DATE type, no parsing needed)

---

## Step 14: Build WHERE Clause with Date Filters

### What This Step Does

We often want to validate data within a specific date range (e.g., only 2023 data). This step takes a start date, end date, and any existing WHERE clause, and merges them into a complete WHERE clause that we can use in SQL queries.

### How It Will Be Used

```python
start_dt = '2023-01-01'
end_dt = '2023-12-31'
existing_where = 'STATUS = \'ACTIVE\''

where_clause = date_parser.merge_where(start_dt, end_dt, existing_where)
print(f"WHERE clause: {where_clause}")
```

### Expected Output

```
WHERE clause: TXN_DATE >= DATE '2023-01-01' AND TXN_DATE <= DATE '2023-12-31' AND STATUS = 'ACTIVE'
```

---

## Step 15: Get Row Counts by Date

### What This Step Does

To understand data distribution and generate vintages (partitions), we need to count how many rows exist for each date value. This function executes a GROUP BY query to get counts per date and standardizes the date format to YYYY-MM-DD for consistent processing.

### How It Will Be Used

```python
row_counts_df = date_parser.get_cnt(table_name, where_clause, service_name=service_name)
print(f"Found data for {len(row_counts_df)} distinct dates")
print(row_counts_df.head())
```

### Expected Output

```
Found data for 365 distinct dates
  TXN_DATE    CNT
0 2023-01-01  1250
1 2023-01-02  1389
2 2023-01-03  1405
...
```

---

## Step 16: Generate Date Vintages (Partitions)

### What This Step Does

Vintages are date-based partitions of data (e.g., monthly, yearly, or snapshot). Based on the min and max dates in the data and the partition type specified in the configuration, we generate a list of vintages. Each vintage includes a start date, end date, and WHERE clause for extracting that partition.

### Function Definition

```python
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

### How It Will Be Used

```python
partition_type = 'month'
vintages = get_vintages(row_counts_df, date_parser, partition_type)
print(f"Generated {len(vintages)} vintages")
for v in vintages[:3]:
    print(f"  {v['vintage']}: {v['start_date']} to {v['end_date']}")
```

### Expected Output

```
Generated 12 vintages
  2023-01: 2023-01-01 to 2023-01-31
  2023-02: 2023-02-01 to 2023-02-28
  2023-03: 2023-03-01 to 2023-03-31
...
```

---

## Step 17: Main Processing Loop - Setup

### What This Step Does

Now we're ready to process each table. We'll loop through all enabled tables and collect metadata for each one. We start by initializing an empty results list to store the metadata for each table.

### Execute This Step

```python
from operator import itemgetter

results = []

for _, table in enabled_tables.iterrows():
    service_name, table_name = table['pcds_tbl'].split('.')
    fetch = itemgetter('pcds_var', 'pcds_where', 'partition', 'col_map', 'start_dt', 'end_dt')
    date_var, where_clause, partition_type, col_map_name, start_dt, end_dt = fetch(table)

    logger.info(f"Processing {table_name}")

    result = {
        'table': table_name.upper(),
        'service': service_name,
        'accessible': False,
        'row_counts': None,
        'crosswalk': None,
        'column_types': None,
        'date_var': None,
        'where_clause': '',
        'vintages': []
    }

    # Continue to next steps...
```

---

## Step 18: Main Processing Loop - Check Accessibility

### What This Step Does

For each table, first check if we can access it. If the table is not accessible (doesn't exist, no permissions, etc.), we skip metadata extraction and just record that it's inaccessible.

### Execute This Step

```python
    result['accessible'] = check_accessible(service_name, table_name)

    if not result['accessible']:
        logger.warning(f"Table {table_name} not accessible, skipping metadata extraction")
        results.append(result)
        continue
```

---

## Step 19: Main Processing Loop - Get Column Metadata

### What This Step Does

For accessible tables, retrieve column information and validate crosswalk mappings. We store both the crosswalk validation results and a dictionary of column names to data types.

### Execute This Step

```python
    columns_df = get_columns(service_name, table_name)
    result['crosswalk'] = check_crosswalk(table_name, columns_df, filtered_crosswalk, col_map_name)

    column_types = dict(zip(columns_df['COLUMN_NAME'].str.upper(), columns_df['DATA_TYPE']))
    result['column_types'] = column_types
```

---

## Step 20: Main Processing Loop - Process Date Variable (If Exists)

### What This Step Does

If the table has a date variable configured (not all tables do), we need to:
1. Create a DateParser object
2. Detect the date format
3. Merge date range filters into WHERE clause
4. Get row counts by date
5. Generate vintages

This is the most complex part of the metadata extraction. The helper function `is_missing()` checks if a value is None, empty string, or NaN.

### Function Definition

```python
def is_missing(value):
    import pandas as pd
    #>>> implementation <<<#
    pass
```

### Execute This Step

```python
    if date_var and not is_missing(date_var):
        date_parser = DateParser(date_var, column_types[date_var])

        date_parser.get_fmt(table_name, service_name)

        where_clause = date_parser.merge_where(start_dt, end_dt, where_clause)

        row_counts_df = date_parser.get_cnt(table_name, where_clause, service_name=service_name)

        result['where_clause'] = where_clause
        result['row_counts'] = row_counts_df.to_dict('records')
        result['vintages'] = get_vintages(row_counts_df, date_parser, partition_type)
        result['date_var'] = date_parser.to_json()

    results.append(result)
```

---

## Step 21: Save Results Locally

### What This Step Does

After processing all tables, we have a complete results list with metadata for each table. We save this to a local JSON file for review and as a backup before uploading to S3.

### Execute This Step

```python
import os
from upath import UPath

local_path = os.path.join(output_folder, f'{step_name}.json')
s3.write_json(results, UPath(local_path))
logger.info(f"Saved local copy to {local_path}")
```

### Expected Output

```
2024-01-15 10:35:22 | INFO | Saved local copy to output/my_validation_run/meta_check_pcds.json
```

---

## Step 22: Upload Results to S3

### What This Step Does

Finally, upload the results JSON to S3 so that downstream steps (AWS meta check, comparison) can access this metadata. The S3 path is constructed from the bucket and step name.

### Execute This Step

```python
s3_path = s3.write_json(results, f'{step_name}.json')
logger.info(f"Uploaded PCDS meta check results to {s3_path}")
```

### Expected Output

```
2024-01-15 10:35:25 | INFO | Uploaded PCDS meta check results to s3://my-bucket/validations/my_validation_run/meta_check_pcds.json
```

---

## Putting It All Together

### Complete main() Function

Here's how all the pieces fit together in the main() function:

```python
def main():
    from operator import itemgetter
    import os
    from upath import UPath
    import pandas as pd
    from loguru import logger

    run_name, category, config_path = get_env('RUN_NAME', 'CATEGORY', 'META_STEP')

    cfg = load_config(config_path)
    step_name = cfg.output.step_name.format(p='pcds')
    output_folder = cfg.output.disk.format(name=run_name)

    add_logger(output_folder, name=step_name)
    logger.info(f"Starting PCDS meta check: {run_name} | {category}")

    tables_df = read_input_tables(cfg.table)
    crosswalk_df = load_column_mappings(cfg.column_maps, category)

    enabled_tables = tables_df[tables_df['enabled']].copy()
    logger.info(f"Going to validate {len(enabled_tables)} tables out of {len(tables_df)} total")

    filtered_crosswalk = crosswalk_df[
        crosswalk_df["col_map"].str.lower().isin(
            (
                enabled_tables["col_map"]
                if "col_map" in enabled_tables.columns
                else enabled_tables["pcds_tbl"].str.extract(r"([^.]+)$", expand=False)
            ).str.lower().unique()
        )
    ].copy()

    s3_bucket = cfg.output.s3.format(name=run_name)
    s3 = S3Manager(s3_bucket)

    s3.write_df(enabled_tables, 'input_tables.csv')
    s3.write_df(filtered_crosswalk, 'crosswalk.csv')
    logger.info(f"Uploaded input_tables, crosswalk document to S3")

    results = []

    for _, table in enabled_tables.iterrows():
        service_name, table_name = table['pcds_tbl'].split('.')
        fetch = itemgetter('pcds_var', 'pcds_where', 'partition', 'col_map', 'start_dt', 'end_dt')
        date_var, where_clause, partition_type, col_map_name, start_dt, end_dt = fetch(table)
        logger.info(f"Processing {table_name}")

        result = {
            'table': table_name.upper(),
            'service': service_name,
            'accessible': check_accessible(service_name, table_name),
            'row_counts': None,
            'crosswalk': None,
            'column_types': None,
            'date_var': None,
            'where_clause': '',
            'vintages': []
        }

        if result['accessible']:
            columns_df = get_columns(service_name, table_name)
            result['crosswalk'] = check_crosswalk(table_name, columns_df, filtered_crosswalk, col_map_name)

            column_types = dict(zip(columns_df['COLUMN_NAME'].str.upper(), columns_df['DATA_TYPE']))
            result['column_types'] = column_types

            if date_var and not is_missing(date_var):
                date_parser = DateParser(date_var, column_types[date_var])
                date_parser.get_fmt(table_name, service_name)
                where_clause = date_parser.merge_where(start_dt, end_dt, where_clause)
                row_counts_df = date_parser.get_cnt(table_name, where_clause, service_name=service_name)
                result['where_clause'] = where_clause
                result['row_counts'] = row_counts_df.to_dict('records')
                result['vintages'] = get_vintages(row_counts_df, date_parser, partition_type)
                result['date_var'] = date_parser.to_json()

        results.append(result)

    local_path = os.path.join(output_folder, f'{step_name}.json')
    s3.write_json(results, UPath(local_path))
    logger.info(f"Saved local copy to {local_path}")

    s3_path = s3.write_json(results, f'{step_name}.json')
    logger.info(f"Uploaded PCDS meta check results to {s3_path}")

    return results


if __name__ == '__main__':
    main()
```

---

## Summary

This tutorial covered the PCDS Meta Check step which:

1. **Loads configuration** from environment variables and TOML file
2. **Reads input files** (tables and crosswalk Excel files)
3. **Validates table accessibility** in PCDS databases
4. **Extracts metadata** (columns, data types)
5. **Validates crosswalk mappings** (which columns map between PCDS and AWS)
6. **Processes date variables** (detect format, get row counts)
7. **Generates vintages** (date partitions for data extraction)
8. **Uploads results to S3** for downstream processing

The output metadata is used by:
- **AWS Meta Check** to compare with AWS table metadata
- **Meta Compare** to generate a comparison report
- **Column Check** to determine which columns to validate
- **Hash Check** to extract and compare actual data

You should now have a complete understanding of how the PCDS Meta Check works and can implement each function to customize it for your specific environment!
