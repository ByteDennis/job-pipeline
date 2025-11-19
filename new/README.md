# Data Validation Pipeline - Jupyter Notebook Implementation

A simplified, notebook-based implementation of the PCDS to AWS data validation pipeline.

## Overview

This pipeline validates data migration from PCDS (Oracle) to AWS (Athena) through 5 sequential steps implemented as Jupyter notebooks.

## Directory Structure

```
new/
├── README.md                           # This file
├── 01_load_config.ipynb               # Step 1: Load config from XLSX
├── 02_row_meta_check.ipynb            # Step 2: Row count validation
├── 03_column_mapping.ipynb            # Step 3: Column crosswalk
├── 04_column_statistics.ipynb         # Step 4: Column statistics
├── 05_value_to_value_check.ipynb      # Step 5: Hash comparison
├── data/                              # JSON state files
│   └── config.json                    # Shared configuration
├── output/                            # Generated SAS/SQL files
│   ├── pcds_stats_*.sas              # SAS code for PCDS stats
│   ├── aws_stats_*.sql               # Athena SQL for AWS stats
│   ├── pcds_hash_*.sas               # SAS code for PCDS hashes
│   └── aws_hash_*.sql                # Athena SQL for AWS hashes
└── utils/                             # Utility modules
    ├── __init__.py
    └── sql_normalization.py           # Normalization functions
```

## Workflow

### Step 1: Load Configuration (`01_load_config.ipynb`)

**Purpose**: Load table comparison list and configuration from XLSX

**Input**:
- `../files/inputs/compare_list.xlsx` - Excel file with table pairs

**Output**:
- `data/config.json` - Configuration with all table pairs

**Required XLSX Columns**:
- `pcds_tbl` (required): PCDS table name
- `aws_tbl` (required): AWS table name
- `group` (optional): Group name
- `pcds_dt` (optional): PCDS date column
- `aws_dt` (optional): AWS date column
- `pcds_where` (optional): Additional PCDS WHERE clause
- `aws_where` (optional): Additional AWS WHERE clause
- `start_dt` (optional): Start date for date range
- `end_dt` (optional): End date for date range
- `partition` (optional): Partition type ('whole', 'month', 'year')
- `col_map` (optional): Column mapping reference
- `unique_id_cols` (optional): Comma-separated unique ID columns
- `enabled` (optional): Enable/disable table (default: True)

### Step 2: Row Meta Check (`02_row_meta_check.ipynb`)

**Purpose**: Compare row counts by partition/vintage

**Input**:
- `data/config.json`

**Output**:
- Updated `data/config.json` with row counts

**What it does**:
1. For each table pair, determines vintages to check based on partition type
2. Queries PCDS for row count per vintage
3. Queries AWS for row count per vintage
4. Compares counts and flags mismatches
5. Updates JSON with results

**Functions to implement**:
- `get_pcds_row_count()` - Execute Oracle query
- `get_aws_row_count()` - Execute Athena query
- `get_vintages()` - Determine date partitions to check

### Step 3: Column Mapping (`03_column_mapping.ipynb`)

**Purpose**: Map comparable columns between PCDS and AWS

**Input**:
- `data/config.json`
- `../files/inputs/crosswalk.xlsx` (optional)

**Output**:
- Updated `data/config.json` with column mappings

**What it does**:
1. Queries PCDS schema metadata (column names and types)
2. Queries AWS schema metadata
3. Matches columns by name (case-insensitive)
4. Validates type compatibility
5. Identifies:
   - Perfect matches (name + compatible type)
   - Type mismatches (name matches, type incompatible)
   - PCDS-only columns
   - AWS-only columns

**Functions to implement**:
- `get_pcds_columns()` - Query Oracle schema
- `get_aws_columns()` - Query Athena schema

### Step 4: Column Statistics (`04_column_statistics.ipynb`)

**Purpose**: Compare column-level statistics

**Input**:
- `data/config.json` with column mappings

**Output**:
- `output/pcds_stats_*.sas` - SAS code for PCDS
- `output/aws_stats_*.sql` - Athena SQL for AWS
- Updated `data/config.json`

**What it does**:

**For PCDS**:
1. Generates SAS code to compute statistics for all comparable columns
2. Statistics include:
   - count, distinct_count
   - min, max
   - mean, std (for numeric)
   - missing count
   - sum, sum_sq (for numeric)
3. Outputs one SAS file for all tables
4. SAS runs on server, emails CSV with results
5. Load CSV back in notebook

**For AWS**:
1. Generates Athena SQL for statistics
2. Can execute queries directly from notebook (optional)
3. Collects results programmatically or via console

**Comparison**:
- Merge PCDS and AWS stats by (table, vintage, column)
- Flag significant differences

### Step 5: Value-to-Value Check (`05_value_to_value_check.ipynb`)

**Purpose**: Row-level hash comparison to find data differences

**Input**:
- `data/config.json` with column mappings

**Output**:
- `output/pcds_hash_*.sas` - SAS code for PCDS hashes
- `output/aws_hash_*.sql` - Athena SQL for AWS hashes
- Mismatch report with unique IDs

**What it does**:

**Hash Computation**:
1. For comparable columns, applies normalization:
   - Trim whitespace
   - Convert strings to uppercase
   - Normalize dates to ISO format
   - Normalize numbers to fixed precision
   - Handle NULLs consistently
2. Concatenates normalized values with separator: `col1|col2|col3`
3. Computes MD5 hash of concatenated string
4. Includes unique ID columns for backtracking

**PCDS**:
```sas
_concat_str = normalize(col1) || '|' || normalize(col2) || '|' || ...;
_hash = MD5(_concat_str);
```

**AWS**:
```sql
SELECT
  MD5(TO_UTF8(normalize(col1) || '|' || normalize(col2) || '|' || ...)) AS _hash,
  unique_id1, unique_id2
FROM table
```

**Comparison**:
1. Load PCDS and AWS hash CSVs
2. Merge on unique ID columns
3. Compare hashes
4. Identify:
   - Matching hashes (data identical)
   - Mismatched hashes (data differs)
   - PCDS-only rows
   - AWS-only rows
5. Export mismatch report with unique IDs for investigation

**Performance Options**:
- Filter specific tables (`FILTER_TABLE_INDICES`)
- Filter specific vintages (`FILTER_VINTAGES`)
- Filter specific columns (`FILTER_COLUMNS`)
- Limit rows per vintage (`MAX_ROWS_PER_VINTAGE`)

## Normalization Functions

The `utils/sql_normalization.py` module provides robust normalization for accurate cross-platform comparison:

### Oracle Normalization (`normalize_oracle_column`)
- **String**: `COALESCE(UPPER(TRIM(col)), '')`
- **Number**: `COALESCE(TO_CHAR(col, 'FM999999999999999.999999'), '0')`
- **Date**: `COALESCE(TO_CHAR(col, 'YYYY-MM-DD'), '')`
- **Timestamp**: `COALESCE(TO_CHAR(col, 'YYYY-MM-DD HH24:MI:SS.FF3'), '')`

### Athena Normalization (`normalize_athena_column`)
- **String**: `COALESCE(UPPER(TRIM(col)), '')`
- **Integer**: `COALESCE(CAST(col AS VARCHAR), '0')`
- **Float/Double**: `COALESCE(FORMAT('%0.6f', col), '0.000000')`
- **Date**: `COALESCE(DATE_FORMAT(col, '%Y-%m-%d'), '')`
- **Timestamp**: `COALESCE(DATE_FORMAT(col, '%Y-%m-%d %H:%i:%s.%f'), '')`

### Testing Normalization

```bash
cd new/utils
python sql_normalization.py
```

## Prerequisites

### Python Packages
```bash
pip install pandas openpyxl jupyter
```

### Database Connections
You need to implement connections to:
- **PCDS (Oracle)**: Use `cx_Oracle` or similar
- **AWS Athena**: Use `boto3` and `pyathena`

### Example: Oracle Connection
```python
import cx_Oracle

conn = cx_Oracle.connect('user/password@host:port/service')
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM table")
```

### Example: Athena Connection
```python
import boto3
from pyathena import connect

conn = connect(
    s3_staging_dir='s3://your-bucket/athena-results/',
    region_name='us-east-1'
)
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM table")
```

## Usage

### Quick Start

1. **Prepare input file**:
   ```bash
   # Create compare_list.xlsx with table pairs
   # Columns: pcds_tbl, aws_tbl, partition, unique_id_cols, etc.
   ```

2. **Run notebooks in order**:
   ```bash
   jupyter notebook
   # Open and run: 01_load_config.ipynb
   # Then: 02_row_meta_check.ipynb
   # Then: 03_column_mapping.ipynb
   # Then: 04_column_statistics.ipynb
   # Then: 05_value_to_value_check.ipynb
   ```

3. **Implement database connections**:
   - Update query functions in notebooks 2-5
   - Add your Oracle and Athena connection logic

4. **Execute SAS code**:
   - Copy generated `.sas` files to SAS server
   - Run SAS programs
   - Collect CSV outputs

5. **Review results**:
   - Load CSVs back into notebooks
   - Compare statistics and hashes
   - Investigate mismatches

### Workflow Diagram

```
[XLSX Input]
     ↓
[01_load_config] → config.json
     ↓
[02_row_meta_check] → config.json (+ row_meta)
     ↓
[03_column_mapping] → config.json (+ column_mappings)
     ↓
[04_column_statistics]
     ↓
     ├─→ pcds_stats.sas → [SAS Server] → CSV
     └─→ aws_stats.sql → [Athena] → Results
     ↓
[Compare Statistics]
     ↓
[05_value_to_value_check]
     ↓
     ├─→ pcds_hash.sas → [SAS Server] → CSV
     └─→ aws_hash.sql → [Athena] → Results
     ↓
[Compare Hashes] → Mismatch Report
```

## Configuration JSON Structure

The `data/config.json` file maintains state across notebooks:

```json
{
  "run_name": "validation_run_20241119_120000",
  "category": "dpst",
  "created_at": "2024-11-19T12:00:00",
  "table_pairs": [
    {
      "pcds_tbl": "SCHEMA.TABLE1",
      "aws_tbl": "schema.table1",
      "partition": "month",
      "pcds_dt": "CREATE_DT",
      "aws_dt": "create_dt",
      "unique_id_cols": "CUSTOMER_ID,TRANSACTION_ID",
      "enabled": true
    }
  ],
  "row_meta": [...],           // Added by Step 2
  "column_mappings": [...],    // Added by Step 3
  "column_statistics": {...},  // Added by Step 4
  "row_hash_check": {...},     // Added by Step 5
  "status": {
    "step1_completed": true,
    "step2_completed": true,
    "step3_completed": true,
    "step4_completed": true,
    "step5_completed": true
  }
}
```

## Customization

### Adding New Normalization Rules

Edit `utils/sql_normalization.py`:

```python
def normalize_oracle_column(column_name, data_type):
    if 'YOUR_TYPE' in data_type:
        return f"YOUR_NORMALIZATION({column_name})"
    # ... rest of function
```

### Adding New Statistics

Edit the `generate_sas_statistics_code()` or `generate_athena_statistics_sql()` functions in Step 4:

```python
# Add new statistic
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col}) AS median
```

### Filtering Data

In any notebook, add filters to reduce scope:

```python
# Step 2: Filter vintages
vintages = ['2024-01-01', '2024-02-01']  # Only these dates

# Step 5: Filter columns
FILTER_COLUMNS = ['CUSTOMER_ID', 'AMOUNT']  # Only these columns
```

## Troubleshooting

### Issue: "Function not implemented"
- Check that you've updated placeholder functions with actual database queries
- Implement `get_pcds_row_count()`, `get_aws_row_count()`, etc.

### Issue: Hash mismatches but data looks identical
- Check normalization rules in `sql_normalization.py`
- Ensure date formats are consistent
- Verify numeric precision matches
- Check for hidden whitespace or special characters

### Issue: SAS code fails
- Verify SAS syntax for your version
- Check table/column names are correct
- Ensure proper quoting of strings
- Test with single table first

### Issue: Athena queries timeout
- Reduce MAX_ROWS_PER_VINTAGE
- Add proper partitioning
- Filter to specific vintages
- Consider sampling for testing

## Best Practices

1. **Start Small**: Test with 1-2 tables before running all tables
2. **Use Filters**: Filter by vintage/columns to reduce data volume
3. **Version Control**: Commit config.json after each step
4. **Document Assumptions**: Add markdown cells to notebooks
5. **Test Normalization**: Run `sql_normalization.py` tests
6. **Review SQL**: Check generated SAS/SQL files before execution
7. **Parallel Execution**: Run Athena queries in parallel when possible
8. **Error Handling**: Add try-catch blocks for production use

## Future Enhancements

- [ ] Add Athena query execution (boto3 integration)
- [ ] Implement Oracle connection (cx_Oracle)
- [ ] Add data profiling visualizations
- [ ] Export results to Excel/HTML reports
- [ ] Add automated testing
- [ ] Implement parallel processing
- [ ] Add progress bars for long-running operations
- [ ] Create reusable query templates
- [ ] Add email notifications
- [ ] Implement incremental validation

## Support

For issues or questions:
1. Check troubleshooting section above
2. Review generated SQL/SAS files
3. Test normalization functions independently
4. Verify database connections

## License

Internal use only.
