# 📊 Data Validation Pipeline

A comprehensive data validation toolkit for comparing PCDS (Oracle) and AWS (Athena) databases across metadata, column statistics, and row-level hashes.

---

## 🎯 Overview

This pipeline performs three-stage validation between PCDS and AWS databases:

1. **Meta Check** - Validates table accessibility, row counts, column mappings, and vintages
2. **Column Check** - Compares statistical distributions (categorical/continuous) across all comparable columns
3. **Hash Check** - Performs row-level hash comparison using only "clean" columns that passed validation

```
┌─────────────┐
│ Meta Check  │ → Identifies comparable columns & vintages
└──────┬──────┘
       ↓
┌─────────────┐
│Column Check │ → Filters out mismatched columns, selects key columns
└──────┬──────┘
       ↓
┌─────────────┐
│ Hash Check  │ → Row-level comparison using clean columns only
└─────────────┘
```

---

## 📋 Prerequisites

### System Requirements
- **PCDS Machine:** Windows with SAS/Oracle access
- **AWS Machine:** Linux/MacOS with AWS Athena access
- **Python:** 3.8+
- **S3 Bucket:** For cross-machine data exchange

### Python Dependencies
```bash
pip install pandas loguru boto3 awswrangler s3fs xlwings python-dateutil
```

### Additional Requirements
- **Oracle Client:** For PCDS connections (cx_Oracle or oracledb)
- **AWS Credentials:** Properly configured for Athena access
- **Excel:** Microsoft Excel or LibreOffice for viewing reports

---

## 🚀 Installation

### 1. Clone Repository
```bash
git clone <repository-url>
cd jobs_pipeline
```

### 2. Setup Virtual Environment
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Configure Environment Files

Create two configuration files:

#### `input_pcds` (PCDS Configuration)
```bash
RUN_NAME=run_20250107
CATEGORY=customer
S3_BUCKET=s3://your-bucket-name

# PCDS Oracle Connection
PCDS_USR=your_username
PCDS_PWD=your_password
PCDS_HOST=your_oracle_host
PCDS_PORT=1521

# AWS Credentials (for Windows machines)
AWS_USR=your_aws_username
AWS_PWD=your_aws_password
AWS_HOST=your_proxy_host
AWS_TOKEN_URL=https://your-token-url
AWS_ARN_URL=https://your-arn-url
AWS_DEFAULT_REGION=us-east-1
```

#### `input_aws` (AWS Configuration)
```bash
RUN_NAME=run_20250107
CATEGORY=customer
S3_BUCKET=s3://your-bucket-name

# AWS Athena Connection
AWS_DEFAULT_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
```

### 4. Prepare Input Files

Create input configuration files in the `input/` directory:

#### `input/{run_name}_pcds.csv` - PCDS Table Mappings
```csv
table_name,date_var,date_type,date_format,partition_type,crosswalk_file
customer.account,acct_date,DATE,YYYY-MM-DD,year_month,crosswalk_account.csv
```

#### `input/crosswalk_account.csv` - Column Mappings
```csv
pcds_column,aws_column,pcds_type,aws_type,category
acct_id,account_id,NUMBER,bigint,comparable
acct_name,account_name,VARCHAR2,string,comparable
ssn,ssn_token,VARCHAR2,string,tokenized
```

**Category Values:**
- `comparable` - Use for validation (included in all checks)
- `tokenized` - Tokenized columns (excluded from hash check)
- `pcds_only` - Only exists in PCDS
- `aws_only` - Only exists in AWS

---

## 📖 Usage

### Quick Start (End-to-End Pipeline)

Run the complete pipeline with default settings (sequential):
```bash
./end2end.sh
```

Run with parallel execution (5 workers):
```bash
./end2end.sh 5
```

### Step-by-Step Execution

#### **Part 1-3: Meta Check**

**On PCDS Machine:**
```bash
python checks/meta_check_pcds.py
```
- Checks table accessibility
- Counts rows per date
- Validates column mappings
- Generates vintages (year/month/week partitions)
- Uploads: `s3://bucket/run_name/meta_check/pcds_{category}_meta.json`

**On AWS Machine:**
```bash
python checks/meta_check_aws.py
```
- Downloads PCDS results from S3
- Performs same checks on AWS tables
- Uploads: `s3://bucket/run_name/meta_check/aws_{category}_meta.json`

**On Either Machine:**
```bash
python checks/compare_report.py
```
- Downloads both meta results
- Generates comparison Excel report
- Saves locally: `output/{run_name}/meta_check_comparison_{category}_{timestamp}.xlsx`

#### **Part 4-6: Column Check**

**On PCDS Machine:**
```bash
# Sequential execution
python checks/column_check_pcds.py

# Parallel execution (5 workers)
python checks/column_check_pcds.py 5
```
- Computes statistics for all comparable columns per vintage
- Differentiates categorical vs continuous variables
- Uploads: `s3://bucket/run_name/column_check/pcds_{category}_column_stats.json`

**On AWS Machine:**
```bash
python checks/column_check_aws.py 5
```
- Downloads vintages from meta check
- Computes AWS column statistics
- Uploads: `s3://bucket/run_name/column_check/aws_{category}_column_stats.json`

**On Either Machine:**
```bash
python checks/column_check_compare.py
```
- Compares PCDS vs AWS statistics
- Identifies **mismatched columns** (excluded from hash check)
- Identifies **clean columns** (passed validation, used for hash)
- Selects **top 3 key columns** by distinct count
- Uploads: `s3://bucket/run_name/column_check/{category}_column_summary.json`
- Saves Excel: `output/{run_name}/column_check_comparison_{category}_{timestamp}.xlsx`

#### **Part 7-9: Hash Check**

**On PCDS Machine:**
```bash
# Standard mode
python checks/hash_check_pcds.py 5

# Debug mode (shows normalized column values)
python checks/hash_check_pcds.py 5 true
```
- Downloads column summary (clean columns + key columns)
- Computes SHA-256 hash per row using **only clean columns**
- Includes top 3 key columns for row identification
- Uploads: `s3://bucket/run_name/hash_check/pcds_{category}_hash.json`

**On AWS Machine:**
```bash
python checks/hash_check_aws.py 5
```
- Computes hashes for AWS tables
- Uploads: `s3://bucket/run_name/hash_check/aws_{category}_hash.json`

**On Either Machine:**
```bash
python checks/hash_check_compare.py
```
- Merges on key columns
- Identifies: matched rows, hash mismatches, PCDS-only, AWS-only
- Saves Excel: `output/{run_name}/hash_check_comparison_{category}_{timestamp}.xlsx`

---

## 📂 Output Structure

### S3 Bucket Layout
```
S3 Root Directory (s3://bucket/run_name/):
run_name/
├── input_tables.csv              # Filtered enabled tables only
├── crosswalk.csv                 # Filtered crosswalk for enabled tables
├── {category}_meta_check.json    # Consolidated meta check results
├── {category}_column_check.json  # Consolidated column check results
└── {category}_hash_check.json    # Consolidated hash check results

meta_check/
├── pcds_{category}_meta.json     # Original PCDS results
└── aws_{category}_meta.json      # Original AWS results

column_check/
├── pcds_{category}_column_stats.json
└── aws_{category}_column_stats.json

hash_check/
├── pcds_{category}_hash.json
└── aws_{category}_hash.json
```

### Local Output
```
output/run_20250107/
├── events.log                                                      ← Execution log
├── pcds_customer_meta.json                                         ← Local copy
├── aws_customer_meta.json
├── meta_check_comparison_customer_20250107_143022.xlsx             ← Excel reports
├── column_check_comparison_customer_20250107_143045.xlsx
└── hash_check_comparison_customer_20250107_143112.xlsx
```

---

## 🔍 Understanding the Process

### Meta Check Flow
1. **Accessibility Test:** Can we query the table?
2. **Row Counts:** How many rows per date value?
3. **Date Range:** What's the min/max date?
4. **Crosswalk Validation:** Do PCDS and AWS columns match?
5. **Vintage Generation:** Create time partitions (year/month/week)

**Example Vintages:**
- `year_month`: `2024-01`, `2024-02`, `2024-03`
- `year_week`: `2024-W01`, `2024-W02`, `2024-W03`

### Column Check Flow
1. **Type Detection:** Continuous (numeric) vs Categorical (string/low cardinality)
2. **Statistics Computation:**
   - **Continuous:** MIN, MAX, AVG, STDDEV on values
   - **Categorical:** Frequency distribution, top 10 values
3. **Comparison:** PCDS vs AWS stats per column per vintage
4. **Quality Analysis:**
   - **Mismatched columns:** count/distinct differ in ANY vintage → excluded from hash
   - **Clean columns:** matched across ALL vintages → used for hash
   - **Key columns:** Top 3 clean columns by distinct count → used for row matching

### Hash Check Flow
1. **Normalization:** Standardize values (numbers, strings, dates) across Oracle/Athena
   - Numbers: `0` or `NULL` → `'0'`, non-zero → fixed decimal
   - Strings: UPPER, trim, remove control chars
   - Dates: `'YYYY-MM-DD'`
2. **Concatenation:** Join normalized clean columns with `|` separator
3. **Hashing:** SHA-256 hash of concatenated string
4. **Comparison:** Merge on key columns, identify mismatches

**Example:**
```sql
-- PCDS (Oracle)
SELECT acct_id, cust_id, balance,
       RAWTOHEX(STANDARD_HASH(
         normalized_acct_id || '|' ||
         normalized_cust_id || '|' ||
         normalized_balance,
         'SHA256'
       )) AS hash_value
FROM customer.account
WHERE acct_date >= DATE '2024-01-01' AND acct_date <= DATE '2024-01-31'

-- AWS (Athena)
SELECT account_id, customer_id, balance,
       to_hex(sha256(to_utf8(
         normalized_account_id || '|' ||
         normalized_customer_id || '|' ||
         normalized_balance
       ))) AS hash_value
FROM customer_db.account
WHERE DATE_FORMAT(acct_date, '%Y-%m') = '2024-01'
```

---

## 🎨 Excel Report Features

### Summary Sheet
- **Right-aligned numeric columns** for better readability
- **Color-coded headers** (gray background)
- **Match indicators:** ✓ (pass) / ✗ (fail)

### Detail Sheets
Each table gets a dedicated sheet with:
- **Table Information:** Names, column counts, vintages
- **Per-Vintage Comparison:** Side-by-side PCDS vs AWS metrics
- **Sample Mismatches:** First 100 rows with mismatched hashes (with key values)

---

## ⚙️ Advanced Configuration

### Parallel Execution
Control the number of parallel workers for column/hash checks:
```bash
# Sequential (default)
python checks/column_check_pcds.py 1

# Optimal for most systems (5-10 workers)
python checks/column_check_pcds.py 5

# Maximum parallelism (use with caution)
python checks/column_check_pcds.py 20
```

**Performance Guide:**
- **1 worker:** ~5s per column (safe, slow)
- **5 workers:** ~1-3s per column (recommended)
- **10+ workers:** May hit database connection limits

### Debug Mode (Hash Check)
Enable debug mode to see normalized column values:
```bash
python checks/hash_check_pcds.py 5 true
```

**Warning:** Debug mode generates very large JSON files (use sparingly).

### Custom Vintages
Modify partition types in input CSV:
- `year` - Annual partitions (2023, 2024)
- `year_month` - Monthly (2024-01, 2024-02)
- `year_week` - Weekly (2024-W01, 2024-W02)
- `daily` - Daily (2024-01-15)
- `whole` - No partitioning (entire dataset)

---

## 🐛 Troubleshooting

### Common Issues

#### ❌ "No comparable columns found"
**Cause:** Crosswalk file doesn't mark any columns as `comparable`
**Solution:** Check `crosswalk_{table}.csv` and ensure columns have `category=comparable`

#### ❌ "S3 upload failed"
**Cause:** AWS credentials expired or bucket doesn't exist
**Solution:**
- Verify S3_BUCKET in env file
- Check AWS credentials: `aws s3 ls s3://your-bucket/`

#### ❌ "No clean columns for hash check"
**Cause:** All comparable columns have mismatched statistics
**Solution:** Review column check Excel report to identify root cause (data quality issues, type mismatches)

#### ❌ "LISTAGG overflow" (Oracle)
**Cause:** Categorical column has >4000 chars in top 10 frequency string
**Solution:** This is logged as a warning; use Athena stats instead

#### ❌ "Worker timeout"
**Cause:** Database query taking too long
**Solution:**
- Reduce workers: `python checks/column_check_pcds.py 3`
- Check database performance
- Add indexes on date columns

### Debug Checklist
1. ✓ Check `output/{run_name}/events.log` for detailed errors
2. ✓ Verify environment files (`input_pcds`, `input_aws`) have correct credentials
3. ✓ Test database connectivity manually
4. ✓ Confirm S3 bucket exists and is accessible
5. ✓ Review crosswalk files for correct column mappings

---

## 📊 Performance Benchmarks

| Task | Sequential (1 worker) | Parallel (5 workers) | Dataset |
|------|----------------------|---------------------|---------|
| Meta Check | ~10s per table | N/A | 10 tables |
| Column Check | ~5s per column | ~1-3s per column | 100 columns |
| Hash Check | ~2s per 1M rows | ~0.5s per 1M rows | 10M rows |

**Memory Usage:**
- Meta Check: <50MB
- Column Check: <100MB per worker
- Hash Check: ~1-2GB for 10M row comparison

---

## 🔐 Security Notes

- **Credentials:** Never commit `input_pcds` or `input_aws` to version control
- **S3 Access:** Use least-privilege IAM policies
- **Tokenized Columns:** Automatically excluded from hash check
- **Excel Reports:** Contain actual data, handle according to data governance policies

---

## 📞 Support

For issues or questions:
1. Check the test plan: `checks/meta_check_test.md`
2. Review execution logs: `output/{run_name}/events.log`
3. Open an issue in the repository

---

## 📝 Quick Reference

### Environment Variables
| Variable | Required | Description |
|----------|----------|-------------|
| `RUN_NAME` | Yes | Unique identifier for this run |
| `CATEGORY` | Yes | Data category (e.g., customer, account) |
| `S3_BUCKET` | Yes | S3 bucket for cross-machine exchange |
| `PCDS_USR/PWD/HOST` | PCDS only | Oracle connection credentials |
| `AWS_ACCESS_KEY_ID/SECRET` | AWS only | AWS Athena credentials |

### File Naming Convention
- Input: `input/{run_name}_{environment}.csv`
- Crosswalk: `input/crosswalk_{table}.csv`
- Output: `output/{run_name}/{step}_{category}_{timestamp}.xlsx`
- S3: `s3://{bucket}/{run_name}/{step}/{environment}_{category}_{type}.json`

### Command Syntax
```bash
# Meta check
python checks/meta_check_pcds.py
python checks/meta_check_aws.py
python checks/compare_report.py

# Column check
python checks/column_check_pcds.py [workers]
python checks/column_check_aws.py [workers]
python checks/column_check_compare.py

# Hash check
python checks/hash_check_pcds.py [workers] [debug:true/false]
python checks/hash_check_aws.py [workers] [debug:true/false]
python checks/hash_check_compare.py

# Full pipeline
./end2end.sh [workers]
```

---

**Version:** 1.0
**Last Updated:** 2025-01-07
**Author:** Data Validation Team
