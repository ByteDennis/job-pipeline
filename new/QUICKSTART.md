# Quick Start Guide

Get started with the data validation pipeline in 5 minutes.

## Prerequisites

```bash
# Install required packages
pip install pandas openpyxl jupyter

# Optional: for database connections
pip install cx_Oracle boto3 pyathena
```

## Step-by-Step Setup

### 1. Create Input File

Create `../files/inputs/compare_list.xlsx` with these columns:

| Column | Required | Example | Description |
|--------|----------|---------|-------------|
| pcds_tbl | Yes | SCHEMA.CUSTOMERS | PCDS (Oracle) table name |
| aws_tbl | Yes | schema.customers | AWS (Athena) table name |
| partition | No | month | 'whole', 'month', or 'year' |
| pcds_dt | No | CREATE_DT | PCDS date column for partitioning |
| aws_dt | No | create_dt | AWS date column for partitioning |
| unique_id_cols | No | CUSTOMER_ID,TRANS_ID | Comma-separated unique IDs |
| pcds_where | No | STATUS='ACTIVE' | Additional PCDS filter |
| aws_where | No | status='ACTIVE' | Additional AWS filter |
| start_dt | No | 2024-01-01 | Start date for date range |
| end_dt | No | 2024-12-31 | End date for date range |
| enabled | No | TRUE | Enable/disable this table |

**Minimal Example**:
```
pcds_tbl,aws_tbl,unique_id_cols
SCHEMA.CUSTOMERS,schema.customers,CUSTOMER_ID
SCHEMA.ORDERS,schema.orders,"ORDER_ID,LINE_NUM"
```

### 2. Launch Jupyter

```bash
cd new
jupyter notebook
```

### 3. Run Notebooks in Order

#### Notebook 1: Load Configuration
1. Open `01_load_config.ipynb`
2. Update `COMPARE_LIST_XLSX` path if needed
3. Run all cells
4. Verify `data/config.json` is created

**Expected output**:
```
Loaded X table pairs from compare_list.xlsx
Configuration saved to: data/config.json
✓ Step 1 Complete - Ready for Step 2
```

#### Notebook 2: Row Meta Check
1. Open `02_row_meta_check.ipynb`
2. **IMPORTANT**: Implement these functions first:
   ```python
   def get_pcds_row_count(table_name, date_col, vintage, partition_type, where_clause):
       # Add your Oracle query here
       # Example:
       # conn = cx_Oracle.connect(...)
       # cursor = conn.cursor()
       # cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE ...")
       # return cursor.fetchone()[0], query_time
       pass

   def get_aws_row_count(table_name, date_col, vintage, partition_type, where_clause):
       # Add your Athena query here
       # Example:
       # conn = connect(s3_staging_dir='s3://...')
       # cursor = conn.cursor()
       # cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE ...")
       # return cursor.fetchone()[0], query_time
       pass
   ```
3. Run all cells
4. Review row count comparison results

**Expected output**:
```
Row Meta Check Summary:
Total checks: X
Matches: Y
Mismatches: Z
✓ Step 2 Complete - Ready for Step 3
```

#### Notebook 3: Column Mapping
1. Open `03_column_mapping.ipynb`
2. **IMPORTANT**: Implement schema query functions:
   ```python
   def get_pcds_columns(table_name):
       # Query Oracle schema
       # Example SQL:
       # SELECT column_name, data_type FROM all_tab_columns
       # WHERE table_name = '{table_name}'
       pass

   def get_aws_columns(table_name):
       # Query Athena schema
       # Example SQL:
       # DESCRIBE {table_name}
       pass
   ```
3. Run all cells
4. Review column mapping results

**Expected output**:
```
Column Mapping Summary:
Total comparable columns: X
Total type mismatches: Y
✓ Step 3 Complete - Ready for Step 4
```

#### Notebook 4: Column Statistics
1. Open `04_column_statistics.ipynb`
2. Run all cells to generate SAS and SQL files
3. **Action required**:
   - Copy `output/pcds_stats_*.sas` to SAS server
   - Run SAS program
   - Wait for email with CSV
   - Save CSV to `output/pcds_column_stats.csv`
4. (Optional) Execute Athena queries:
   - Review `output/aws_stats_*.sql`
   - Execute via Athena console or boto3
5. Load results and compare

**Expected output**:
```
✓ SAS code generated: output/pcds_stats_*.sas
✓ Athena SQL generated: output/aws_stats_*.sql

⚠ ACTION REQUIRED:
  1. Copy SAS file to SAS server
  2. Run SAS program
  3. Wait for email with CSV
```

#### Notebook 5: Value-to-Value Check
1. Open `05_value_to_value_check.ipynb`
2. Configure filters (optional):
   ```python
   FILTER_TABLE_INDICES = [0, 1]  # Test with first 2 tables
   MAX_ROWS_PER_VINTAGE = 10000   # Limit rows for testing
   ```
3. Run all cells to generate hash code
4. **Action required**:
   - Copy `output/pcds_hash_*.sas` to SAS server
   - Run SAS program
   - Collect hash CSVs
   - Copy `output/aws_hash_*.sql` to Athena
   - Execute and save results
5. Load CSVs and run comparison:
   ```python
   results = compare_hashes(
       'output/pcds_hash_table1.csv',
       'output/aws_hash_table1.csv',
       ['customer_id']
   )
   ```

**Expected output**:
```
Hash matches: X
Hash mismatches: Y
Rows only in PCDS: Z
Rows only in AWS: W
```

## Testing the Pipeline

### Test with Sample Data

1. Create a small test table (100 rows)
2. Add to compare_list.xlsx:
   ```
   pcds_tbl,aws_tbl,partition,unique_id_cols
   TEST_SCHEMA.TEST_TABLE,test_schema.test_table,whole,ID
   ```
3. Run all 5 notebooks
4. Verify:
   - Row counts match
   - Column mappings are correct
   - Statistics are comparable
   - Hashes match (if data is identical)

### Test Normalization

```bash
cd utils
python sql_normalization.py
```

This will show normalized expressions for various data types.

## Common Issues

### 1. Database Connection Fails
```python
# Oracle
import cx_Oracle
conn = cx_Oracle.connect('user/pass@host:port/service')

# Athena
from pyathena import connect
conn = connect(s3_staging_dir='s3://bucket/path/', region_name='us-east-1')
```

### 2. Vintages Not Generated
Update `get_vintages()` function in Notebook 2:
```python
def get_vintages(table_pair, partition_type):
    if partition_type == 'month':
        # Generate monthly dates from start_dt to end_dt
        return pd.date_range(
            start=table_pair['start_dt'],
            end=table_pair['end_dt'],
            freq='MS'
        ).strftime('%Y-%m-%d').tolist()
    return ['whole']
```

### 3. Hash Mismatches
- Check normalization rules in `utils/sql_normalization.py`
- Verify data types are correct in column mapping
- Test with single column first
- Check for hidden characters or whitespace

### 4. SAS Code Syntax Error
- Verify table names are quoted correctly
- Check date format strings
- Test with single table first
- Ensure MD5 function is available (or use STANDARD_HASH)

## Tips for Production Use

1. **Start Small**: Begin with 1-2 tables, then scale
2. **Use Version Control**: Commit config.json after each step
3. **Document Changes**: Add markdown cells to notebooks
4. **Error Handling**: Wrap queries in try-except blocks
5. **Logging**: Use `print()` statements liberally
6. **Validation**: Check results at each step before proceeding
7. **Backup**: Save generated SAS/SQL files
8. **Testing**: Test normalization functions thoroughly

## Next Steps

After completing all notebooks:

1. **Review Results**:
   - Analyze row count discrepancies
   - Investigate column type mismatches
   - Examine statistics differences
   - Trace hash mismatches using unique IDs

2. **Document Findings**:
   - Create summary report
   - List all identified issues
   - Propose remediation steps

3. **Iterate**:
   - Update compare_list.xlsx with new tables
   - Adjust filters and configurations
   - Rerun specific steps as needed

4. **Automate**:
   - Convert notebooks to Python scripts
   - Schedule periodic validation runs
   - Set up email alerts for issues

## Support

For questions or issues:
- Check the main README.md
- Review troubleshooting section
- Test normalization functions independently
- Verify database connections

## Cheat Sheet

```bash
# Quick commands
cd new
jupyter notebook

# Run notebooks in order:
# 1. 01_load_config.ipynb
# 2. 02_row_meta_check.ipynb
# 3. 03_column_mapping.ipynb
# 4. 04_column_statistics.ipynb
# 5. 05_value_to_value_check.ipynb

# Test normalization
cd utils && python sql_normalization.py

# Check output files
ls -l output/

# View config
cat data/config.json | python -m json.tool
```
