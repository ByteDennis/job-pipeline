# Data Validation Pipeline - Implementation Plan

## Overview
Create three validation scripts that run on PCDS (Windows/Oracle) with S3 integration for intermediate results.

## Project Structure
```
jobs_pipeline/
├── meta_check/
│   ├── meta_check.py
│   └── utils.py
├── column_check/
│   ├── column_check.py
│   └── utils.py
├── hash_check/
│   ├── hash_check.py
│   └── utils.py
├── common/
│   ├── excel_reporter.py
│   ├── s3_utils.py
│   └── config_loader.py
└── TEST_PLAN.md
```

---

## Step 1: Meta Check (`meta_check/meta_check.py`)

### Purpose
Validate table accessibility, row counts, column mappings, and obtain vintage lists.

### Inputs
- `src/input_pcds` - PCDS configuration
- Crosswalk Excel (via `load_column_mappings()`)
- Input table Excel (via `read_input_tables()`)

### Configuration Variables
```python
# From input_pcds
RUN_NAME = "demo"
S3_BUCKET = "s3://your-bucket-to-save-intermediate-results"
CATEGORY = "dpst"  # or "loan"
```

### Processing Steps

#### 1.1 Table Accessibility Check
```python
for each table in input_tables:
    - Test PCDS connection via pcds_connect(service_name)
    - Test AWS connection via athena_connect(database)
    - Record: accessible_pcds (bool), accessible_aws (bool)
    - Save errors if any
```

#### 1.2 Row Count Validation by Date Variable
```python
for each table:
    - Get date_var, date_type, partition_type from config
    - Query PCDS: SELECT date_partition, COUNT(*) GROUP BY date_partition WHERE pcds_where
    - Query AWS: SELECT date_partition, COUNT(*) GROUP BY date_partition WHERE aws_where
    - Compare counts by partition
    - Record: equal_partitions[], unequal_partitions[{partition, pcds_count, aws_count, diff}]
```

#### 1.3 Column Classification
```python
comparable_columns = []      # In both platforms, not tokenized
tokenized_columns = []       # In both platforms, is_tokenized=True
pcds_only_columns = []       # Only in PCDS
aws_only_columns = []        # Only in AWS

for each column in crosswalk:
    pcds_exists = column.pcds_col in actual_pcds_columns
    aws_exists = column.aws_col in actual_aws_columns

    if pcds_exists and aws_exists:
        if column.is_tokenized:
            tokenized_columns.append(column)
        else:
            comparable_columns.append(column)
    elif pcds_exists:
        pcds_only_columns.append(column)
    elif aws_exists:
        aws_only_columns.append(column)
```

#### 1.4 Vintage List Generation
```python
vintages = get_vintages_from_data(
    info_str=f"{pcds_svc}.{pcds_table}",
    date_var=date_var,
    date_type=date_type,
    date_format='YYYY-MM-DD',  # PCDS format
    partition_type=partition_type,
    where_clause=pcds_where
)
# Ensure week format aligns: 'YYYY-Www' (e.g., '2025-W01')
```

### Excel Output (`{RUN_NAME}_meta_check.xlsx`)

**Sheet 1: SUMMARY**
| Table | PCDS Access | AWS Access | Total Vintages | Equal Vintages | Unequal Vintages | Comparable Cols | Tokenized Cols | PCDS Only | AWS Only |
|-------|-------------|------------|----------------|----------------|------------------|-----------------|----------------|-----------|----------|
| ...   | ✓           | ✓          | 52             | 50             | 2                | 25              | 5              | 3         | 2        |

**Sheet per Table: {TABLE_NAME}**
- Section 1: Table Info (service, table names, where clauses)
- Section 2: Row Count by Vintage
  - Highlighted rows where counts differ
- Section 3: Column Classification
  - Comparable (green)
  - Tokenized (yellow)
  - PCDS Only (orange) - FLAG for manual review
  - AWS Only (orange) - FLAG for manual review
- Section 4: Vintage List

### S3 Outputs
```
s3://{S3_BUCKET}/{RUN_NAME}/meta_check/
  - {table_name}_meta_results.json
  - {RUN_NAME}_meta_check.xlsx
```

---

## Step 2: Column Check (`column_check/column_check.py`)

### Purpose
Compare comprehensive column statistics between PCDS and AWS, focusing on frequency distributions.

### Inputs
- Meta check results from S3
- List of comparable columns (excluding unequal vintages)
- Exclude clauses for rows with count mismatches

### Configuration
```python
PCDS_PARALLEL = 3  # Max parallel queries for PCDS Oracle
AWS_PARALLEL = 5   # Max parallel queries for AWS Athena
```

### Processing Steps

#### 2.1 Build Exclude Clauses
```python
# From meta check results
for vintage in unequal_vintages:
    exclude_clauses.append(build_exclude_for_vintage(vintage, date_var, date_type))
```

#### 2.2 Generate Column Statistics Queries

**For NUMERIC columns:**
```sql
-- PCDS Oracle
SELECT
    '{column}' AS col_name,
    '{data_type}' AS col_type,
    COUNT({column}) AS col_count,
    COUNT(DISTINCT {column}) AS col_distinct,
    TO_CHAR(MIN({column})) AS col_min,
    TO_CHAR(MAX({column})) AS col_max,
    AVG({column}) AS col_avg,
    STDDEV({column}) AS col_std,
    SUM({column}) AS col_sum,
    SUM({column} * {column}) AS col_sum_sq,
    '' AS col_freq,
    COUNT(*) - COUNT({column}) AS col_missing
FROM {table}
WHERE {where_clause} AND {exclude_clauses}

-- AWS Athena (similar structure)
```

**For CATEGORICAL columns:**
```sql
-- PCDS Oracle
WITH FreqTable_RAW AS (
    SELECT {column} AS p_col, COUNT(*) AS value_freq
    FROM {table}
    WHERE {where_clause} AND {exclude_clauses}
    GROUP BY {column}
),
FreqTable AS (
    SELECT p_col, value_freq, ROW_NUMBER() OVER (ORDER BY value_freq DESC, p_col ASC) AS rn
    FROM FreqTable_RAW
)
SELECT
    '{column}' AS col_name,
    '{data_type}' AS col_type,
    SUM(value_freq) AS col_count,
    COUNT(value_freq) AS col_distinct,
    TO_CHAR(MIN(value_freq)) AS col_min,
    TO_CHAR(MAX(value_freq)) AS col_max,
    AVG(value_freq) AS col_avg,
    STDDEV(value_freq) AS col_std,
    SUM(value_freq) AS col_sum,
    SUM(value_freq * value_freq) AS col_sum_sq,
    LISTAGG(p_col || '(' || value_freq || ')', '; ') WITHIN GROUP (ORDER BY rn) AS col_freq, -- Top 20
    COALESCE((SELECT value_freq FROM FreqTable WHERE p_col IS NULL AND ROWNUM = 1), 0) AS col_missing
FROM FreqTable
WHERE rn <= 20
```

#### 2.3 Execute Parallel Queries
```python
# PCDS - Oracle with connection pooling (max 3)
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    pcds_futures = [executor.submit(query_pcds, col) for col in comparable_cols]
    pcds_results = [f.result() for f in concurrent.futures.as_completed(pcds_futures)]

# AWS - Athena (max 5)
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    aws_futures = [executor.submit(query_aws, col) for col in comparable_cols]
    aws_results = [f.result() for f in concurrent.futures.as_completed(aws_futures)]
```

#### 2.4 Compare Statistics
```python
for col in comparable_cols:
    pcds_stat = pcds_results[col]
    aws_stat = aws_results[col]

    comparison = {
        'column': col,
        'count_match': pcds_stat.col_count == aws_stat.col_count,
        'distinct_match': pcds_stat.col_distinct == aws_stat.col_distinct,
        'min_match': compare_values(pcds_stat.col_min, aws_stat.col_min),
        'max_match': compare_values(pcds_stat.col_max, aws_stat.col_max),
        'avg_diff': abs(pcds_stat.col_avg - aws_stat.col_avg) if numeric else None,
        'freq_match': compare_frequency_distribution(pcds_stat.col_freq, aws_stat.col_freq)
    }
```

### Excel Output (`{RUN_NAME}_column_check.xlsx`)

**Sheet 1: SUMMARY**
| Table | Total Cols | Matched | Mismatched | Freq Matches | Match Rate % |
|-------|------------|---------|------------|--------------|--------------|
| ...   | 25         | 23      | 2          | 22           | 92%          |

**Sheet per Table: {TABLE_NAME}**
- Section 1: Vintage Summary (which vintages included/excluded)
- Section 2: Column Statistics Comparison
  - Columns: col_name, pcds_count, aws_count, pcds_distinct, aws_distinct, pcds_freq (top 10), aws_freq (top 10)
  - Highlighting:
    - GREEN: All metrics match
    - YELLOW: Minor differences (avg/std within tolerance)
    - RED: Major differences (count/distinct mismatch, frequency mismatch)
- Section 3: Failed Columns (detailed breakdown)

### S3 Outputs
```
s3://{S3_BUCKET}/{RUN_NAME}/column_check/
  - {table_name}_pcds_stats.csv
  - {table_name}_aws_stats.csv
  - {table_name}_comparison.json
  - {RUN_NAME}_column_check.xlsx
```

---

## Step 3: Hash Check (`hash_check/hash_check.py`)

### Purpose
Validate row-level data integrity using hash comparison on normalized columns.

### Inputs
- Column check results from S3
- Columns that passed step 2
- Plus 2 columns with largest n_distinct values from step 2

### Configuration
```python
PCDS_PARALLEL = 3
AWS_PARALLEL = 5
VINTAGE_BATCH_SIZE = 1000000  # Split large vintages into batches
```

### Processing Steps

#### 3.1 Select Columns for Hashing
```python
# Columns that passed column_check
passed_columns = [col for col in comparable_cols if col.column_check_status == 'passed']

# Add 2 columns with highest n_distinct
high_cardinality_cols = sorted(comparable_cols, key=lambda x: x.col_distinct, reverse=True)[:2]

hash_columns = list(set(passed_columns + high_cardinality_cols))
```

#### 3.2 Split Vintages into Batches
```python
for vintage in vintages:
    row_count = get_vintage_row_count(vintage)
    if row_count > VINTAGE_BATCH_SIZE:
        # Split using ROWNUM or partition key ranges
        batches = split_vintage_into_batches(vintage, row_count, VINTAGE_BATCH_SIZE)
    else:
        batches = [vintage]
```

#### 3.3 Generate Hash Queries

**PCDS Oracle - Data Normalization + Hash:**
```sql
SELECT
    {key_column},
    ORA_HASH(
        CONCAT(
            NVL(TO_CHAR(TRUNC(timestamp_col)), ''),
            NVL(TO_CHAR(numeric_col), ''),
            NVL(UPPER(TRIM(string_col)), ''),
            ...
        )
    ) AS row_hash
FROM {table}
WHERE {vintage_where_clause} AND {exclude_clauses}
ORDER BY {key_column}
```

**AWS Athena - Data Normalization + Hash:**
```sql
SELECT
    {key_column},
    xxhash64(
        CONCAT(
            COALESCE(CAST(DATE(timestamp_col) AS VARCHAR), ''),
            COALESCE(CAST(numeric_col AS VARCHAR), ''),
            COALESCE(UPPER(TRIM(string_col)), ''),
            ...
        )
    ) AS row_hash
FROM {table}
WHERE {vintage_where_clause} AND {exclude_clauses}
ORDER BY {key_column}
```

**Normalization Rules:**
- TIMESTAMP: Truncate to DATE
- NUMERIC: TO_CHAR / CAST AS VARCHAR
- STRING: UPPER(TRIM(...))
- NULL: Empty string ''

#### 3.4 Execute Parallel Hash Queries
```python
# For each vintage batch
for batch in vintage_batches:
    # PCDS
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        pcds_hash_futures = [executor.submit(query_pcds_hash, batch) for batch in batches]
        pcds_hashes = {f.batch_id: f.result() for f in as_completed(pcds_hash_futures)}

    # AWS
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        aws_hash_futures = [executor.submit(query_aws_hash, batch) for batch in batches]
        aws_hashes = {f.batch_id: f.result() for f in as_completed(aws_hash_futures)}
```

#### 3.5 Compare Hashes
```python
for batch in batches:
    pcds_df = pcds_hashes[batch.id]
    aws_df = aws_hashes[batch.id]

    # Merge on key column
    merged = pd.merge(pcds_df, aws_df, on='key_column', suffixes=('_pcds', '_aws'))

    # Find mismatches
    mismatches = merged[merged['row_hash_pcds'] != merged['row_hash_aws']]

    # Save only mismatched rows
    if len(mismatches) > 0:
        mismatch_results.append({
            'vintage': batch.vintage,
            'batch': batch.id,
            'mismatches': mismatches[['key_column', 'row_hash_pcds', 'row_hash_aws']]
        })
```

### Excel Output (`{RUN_NAME}_hash_check.xlsx`)

**Sheet 1: SUMMARY**
| Table | Vintage | Total Rows PCDS | Total Rows AWS | Matched | Mismatched | Match Rate % |
|-------|---------|-----------------|----------------|---------|------------|--------------|
| ...   | 2025-W01| 1,250,000       | 1,250,000      | 1,249,998| 2         | 99.9998%     |

**Sheet per Table: {TABLE_NAME}**
- Section 1: Hash Configuration (columns hashed, normalization rules)
- Section 2: Summary by Vintage
- Section 3: Mismatched Rows ONLY
  - Columns: vintage, batch, key_column, row_hash_pcds, row_hash_aws
  - Limited to first 10,000 mismatches per table for Excel limits

### S3 Outputs
```
s3://{S3_BUCKET}/{RUN_NAME}/hash_check/
  - {table_name}_{vintage}_pcds_hashes.parquet
  - {table_name}_{vintage}_aws_hashes.parquet
  - {table_name}_{vintage}_mismatches.csv (only if mismatches exist)
  - {RUN_NAME}_hash_check.xlsx
```

---

## Common Utilities

### `common/excel_reporter.py`
```python
class ExcelReporter:
    def create_summary_sheet(data: dict) -> None
    def create_table_sheet(table_name: str, data: dict) -> None
    def apply_conditional_formatting(sheet, rules) -> None
    def highlight_differences(sheet, pcds_col, aws_col, comparison_type) -> None
    def auto_format_columns(sheet) -> None
```

### `common/s3_utils.py`
```python
def upload_to_s3(local_path: str, s3_path: str) -> bool
def download_from_s3(s3_path: str, local_path: str) -> bool
def list_s3_files(s3_prefix: str) -> List[str]
```

### `common/config_loader.py`
```python
def load_input_config(category: str) -> dict
def load_crosswalk(category: str) -> pd.DataFrame
def load_input_tables(category: str) -> pd.DataFrame
```

---

## Execution Flow

```bash
# Step 1: Meta Check
cd meta_check
python meta_check.py --run-name demo --category dpst

# Step 2: Column Check (depends on step 1)
cd ../column_check
python column_check.py --run-name demo --category dpst

# Step 3: Hash Check (depends on step 2)
cd ../hash_check
python hash_check.py --run-name demo --category dpst
```

---

## Key Implementation Notes

### Excel Library
- Use **xlwings** for styled Excel output
- Reuse ExcelReporter class from `src/end2end.py:536-680` (may need optimization)

### Frequency Comparison (Column Check)
- **Top 20** most frequent items (not 10)
- **Exact match** required
- Sort by key **alphanumerically** before comparison
- Store as **list of tuples**: `[(value1, freq1), (value2, freq2), ...]`

### Hash Algorithms
- **Oracle**: `STANDARD_HASH(concat_expr, 'MD5')` (from `src/utils.py:231`)
- **Athena**: `TO_HEX(MD5(TO_UTF8(concat_expr)))` (from `src/utils.py:266`)
- Use existing functions: `build_oracle_hash_expr()` and `build_athena_hash_expr()`

### Key Column Selection (Hash Check)
- From Step 2 (column_check) results, select **2 columns with highest n_distinct**
- These become the composite key for hash comparison
- Hash all comparable columns that **passed** column_check

### File Formats
- **Parquet (.pq)**: All intermediate data results
- **JSON**: Metadata and comparison results
- **No CSV**: Eliminates CSV files entirely
- **Excel (.xlsx)**: Final reports only

### Future Workflow Split (Post-Testing)
```
Part 1 (PCDS Server):
  - Generate queries
  - Execute PCDS queries
  - Upload results to S3: s3://{S3_BUCKET}/{RUN_NAME}/pcds_results/

Part 2 (AWS Server):
  - Download from S3: s3://{S3_BUCKET}/{RUN_NAME}/pcds_results/
  - Execute AWS queries
  - Upload results to S3: s3://{S3_BUCKET}/{RUN_NAME}/aws_results/

Part 3 (PCDS Server):
  - Download from S3: both pcds_results/ and aws_results/
  - Compare and generate Excel reports
  - Upload final reports to S3: s3://{S3_BUCKET}/{RUN_NAME}/reports/
```

### Code Reuse Strategy
- **Import directly** from `src/end2end.py` when possible:
  - `aws_creds_renew()` (lines 88-146)
  - `pcds_connect()` (lines 178-199)
  - `athena_connect()` (lines 209-216)
  - `SQLengine` class (lines 231-266)
  - `is_missing()` (lines 290-304)
  - `load_config()` (lines 323-333)
  - `read_input_tables()` (lines 409-453)
  - `load_column_mappings()` (lines 481-525)
  - `get_vintages_from_data()` (lines 834-896)
- **Refactor** into common utilities:
  - S3 upload/download functions (lines 682-702)
  - `ExcelReporter` class (lines 536-680) - may need optimization
- **Import** from `src/utils.py`:
  - `build_oracle_hash_expr()` (line 199)
  - `build_athena_hash_expr()` (line 236)
  - JSON IO: `read_json()`, `write_json()`

### AWS Credential Management
```python
# At start of each AWS query block
if inWindows:
    aws_creds_renew(delta=300)  # Renew if expires in <5 min
```

### Week Partition Alignment
```python
# PCDS: TO_CHAR(date_col, 'IYYY') || '-W' || LPAD(TO_CHAR(date_col, 'IW'), 2, '0')
# AWS: format_datetime(date_col, 'xxxx-''W''ww')
# Both produce: '2025-W01', '2025-W52', etc.
```

### Error Handling
```python
try:
    result = query_function()
except oracledb.Error as e:
    log_error(f"PCDS Error: {e}")
    save_partial_results()
except pyathena.Error as e:
    log_error(f"AWS Error: {e}")
    save_partial_results()
```

### Progress Tracking
```python
# Use tqdm for long-running operations
from tqdm import tqdm

for vintage in tqdm(vintages, desc="Processing vintages"):
    process_vintage(vintage)
```

---

## Testing Strategy (See TEST_PLAN.md)

1. **Unit Tests**: Individual functions (normalization, hash generation, query building)
2. **Integration Tests**: End-to-end for small sample table
3. **Performance Tests**: Parallel execution timing
4. **Data Quality Tests**: Known good/bad data scenarios

---

## Next Steps

1. ✅ Review and approve this implementation plan
2. Create folder structure and common utilities
3. Implement meta_check.py
4. Implement column_check.py
5. Implement hash_check.py
6. Generate TEST_PLAN.md
7. Run end-to-end test with sample data
