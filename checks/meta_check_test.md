# Data Validation Pipeline Test Plan

## Part 1: meta_check_pcds.py

### `add_logger(folder)`
**Params:** folder (str)
**Purpose:** Setup logger to output folder with events.log
**Edge Cases:** Folder doesn't exist, existing log file, permission denied
**Caveat:** Removes all existing logger handlers

### `check_accessible(conn, table_name)`
**Params:** conn (service_name), table_name (svc.table format)
**Purpose:** Test if PCDS table accessible via SELECT 1 WHERE ROWNUM=1
**Edge Cases:** Invalid service, table not exists, no permissions, network timeout
**Caveat:** Returns False on any exception, error logged but not raised

### `get_row_counts(conn, table_name, date_var, where_clause=None)`
**Params:** conn, table_name, date_var, where_clause (optional)
**Purpose:** Raw SELECT date_var, COUNT(*) GROUP BY, parse dates in Python using dateparser
**Edge Cases:** date_var NULL/NaT, invalid date format, empty table, WHERE clause syntax error, mixed date formats
**Caveat:** Adds 'date_std' column via parse_date_to_std(), unparseable dates become None

### `get_columns(conn, table_name)`
**Params:** conn, table_name (svc.table format)
**Purpose:** Query all_tab_columns for COLUMN_NAME, DATA_TYPE metadata
**Edge Cases:** Schema name case mismatch, table doesn't exist, system tables
**Caveat:** Requires svc.table format, splits on '.', uppercases schema/table

### `check_crosswalk(table_name, columns_df, crosswalk_df, col_map_name)`
**Params:** table_name, columns_df (from get_columns), crosswalk_df, col_map_name
**Purpose:** Classify columns: comparable, tokenized, pcds_only, unmapped
**Edge Cases:** Empty crosswalk, case mismatches, col_map_name not in crosswalk, NaN values
**Caveat:** PCDS cols uppercased, crosswalk filtered by col_map_name.lower()

### `get_vintages(row_counts_df, date_var, partition_type, db_type='oracle')`
**Params:** row_counts_df (with date_std), date_var, partition_type (day/week/month), db_type
**Purpose:** Generate vintage list with start/end dates and SQL WHERE clauses
**Edge Cases:** Empty df, all dates NULL, partition_type invalid, single date, date gaps
**Caveat:** Requires 'date_std' column, calls detect_date_format() then generate_vintages()

### `main()`
**Params:** None (reads input_pcds env file)
**Purpose:** Orchestrate all checks, save local JSON, upload to S3
**Edge Cases:** Missing env vars, S3 bucket doesn't exist, no enabled tables, all tables inaccessible
**Caveat:** Loads env once, creates output/{run_name}/ folder, requires S3_BUCKET, RUN_NAME, CATEGORY

---

## Part 2: meta_check_aws.py

### `check_accessible(database, table_name)`
**Params:** database (str), table_name (db.table format)
**Purpose:** Test AWS table accessible via SELECT 1 LIMIT 1
**Edge Cases:** Database doesn't exist, table not exists, Athena timeout, no credentials
**Caveat:** Different signature than PCDS version (database vs conn)

### `get_row_counts(database, table_name, date_var, where_clause=None)`
**Params:** database, table_name, date_var, where_clause
**Purpose:** Same as PCDS but for Athena
**Edge Cases:** Partitioned tables, date_var with format specifier e.g. "col (%Y%m%d)", string dates
**Caveat:** Athena case-sensitive for column names (lowercase), slower than PCDS

### `get_columns(database, table_name)`
**Params:** database, table_name
**Purpose:** DESCRIBE table to get column metadata
**Edge Cases:** External tables, partitioned columns, complex types (array/struct)
**Caveat:** Returns different format than all_tab_columns, column names lowercase

### `check_crosswalk(table_name, columns_df, crosswalk_df, col_map_name)`
**Params:** Same as PCDS
**Purpose:** Classify columns for AWS: comparable, tokenized, aws_only, unmapped
**Edge Cases:** Same as PCDS but AWS cols lowercased
**Caveat:** Uses columns_df.iloc[:, 0] to get column names from DESCRIBE output

### `main()`
**Params:** None (reads input_aws env file)
**Purpose:** Download PCDS results from S3, check AWS tables, upload results
**Edge Cases:** S3 download fails, PCDS results not found, AWS credentials expired (Windows)
**Caveat:** Requires PCDS results already uploaded, must match category

---

## Part 3: compare_report.py

### `prepare_table_sections(pcds_result, aws_result)`
**Params:** pcds_result (dict), aws_result (dict)
**Purpose:** Build Excel sections: table info, row counts, crosswalk, vintages
**Edge Cases:** Missing keys, NULL row_counts, empty crosswalk, no vintages
**Caveat:** Expects specific dict structure from Parts 1&2, zips PCDS/AWS results

### `main()`
**Params:** None (reads input_pcds for run_name/S3 bucket)
**Purpose:** Download both results, generate Excel using ExcelReporter, upload
**Edge Cases:** Results count mismatch, tables in different order, S3 download fails, xlwings not installed
**Caveat:** Requires xlwings for Excel generation, zips results (assumes same order)

---

## Utilities: utils_date.py

### `parse_date_to_std(date_val)`
**Params:** date_val (any type)
**Purpose:** Parse to %Y-%m-%d string using dateutil.parser
**Edge Cases:** pd.NaT, datetime, Timestamp, string "20250101", "2025-01-01", invalid format
**Caveat:** Returns None on parse failure, no exception raised

### `detect_date_format(date_vals)`
**Params:** date_vals (list)
**Purpose:** Detect date type and format from first 10 samples
**Edge Cases:** Empty list, all NULL, mixed formats, datetime objects vs strings
**Caveat:** Returns ('unknown', None) if no samples, ('date', None) for datetime objects, ('string', fmt) for strings

### `date_to_sql_literal(date_str, var_type, var_format, db_type)`
**Params:** date_str (%Y-%m-%d), var_type (date/string), var_format (%Y%m%d/%Y-%m-%d), db_type (oracle/athena)
**Purpose:** Convert standard date string to SQL literal for WHERE clauses
**Edge Cases:** var_type='date' uses DATE 'YYYY-MM-DD', string formats require conversion
**Caveat:** Oracle and Athena syntax identical for DATE literals

### `generate_vintages(min_date, max_date, partition_type, date_var, var_type, var_format, db_type)`
**Params:** min_date (str), max_date (str), partition_type (day/week/month), date_var, var_type, var_format, db_type
**Purpose:** Generate vintage dicts with vintage label, start_date, end_date, where_clause
**Edge Cases:** min_date > max_date, single date, week spanning year boundary, month with different days
**Caveat:** Week starts Monday (ISO), vintage format: D20250101, W2025W01, M202501

---

## Utilities: utils_config.py

### `load_env(env_file)`
**Params:** env_file (str, relative path)
**Purpose:** Parse key=value from file, set os.environ, return dict
**Edge Cases:** File not exists, malformed lines, missing '=', commented lines (#)
**Caveat:** Overwrites existing env vars, strips whitespace

### `load_toml(toml_file)`
**Params:** toml_file (str)
**Purpose:** Load TOML config using tomli
**Edge Cases:** File not exists, invalid TOML syntax
**Caveat:** Returns dict, requires tomli library

### `load_table_list(excel_path, sheet_name)`
**Params:** excel_path, sheet_name
**Purpose:** Load Excel, filter enabled='yes' (case-insensitive)
**Edge Cases:** Sheet doesn't exist, no Enabled column, all rows disabled
**Caveat:** Returns empty DataFrame if no enabled rows

### `load_crosswalk(excel_path, sheet_name)`
**Params:** excel_path, sheet_name
**Purpose:** Load crosswalk mapping Excel
**Edge Cases:** Multiple sheets, missing columns, NaN values
**Caveat:** No filtering applied, returns raw DataFrame

---

## Utilities: utils_db.py

### `get_oracle_conn()`
**Params:** None (uses env vars PCDS_USR, PCDS_PWD, PCDS_PATH)
**Purpose:** Create cx_Oracle connection
**Edge Cases:** Missing env vars, invalid credentials, network unreachable, TNS name error
**Caveat:** Raises exception on failure, no retry logic

### `get_athena_conn()`
**Params:** None (uses env AWS_REGION, AWS_S3_STAGING_DIR, AWS_WORK_GROUP)
**Purpose:** Create pyathena connection
**Edge Cases:** Invalid region, S3 staging dir doesn't exist, work group not found, expired credentials
**Caveat:** Windows requires credential renewal, SESSION global variable

### `query_to_df(conn, sql)`
**Params:** conn, sql (str)
**Purpose:** Execute SQL, return pandas DataFrame
**Edge Cases:** SQL syntax error, timeout, empty result, very large result
**Caveat:** Loads entire result into memory

### `check_table_accessible(conn, table_name, db_type)`
**Params:** conn, table_name, db_type ('oracle'/'athena')
**Purpose:** Test table accessible with ROWNUM=1 or LIMIT 1
**Edge Cases:** Same as meta_check functions
**Caveat:** Returns bool, swallows exceptions

### `get_table_columns(conn, table_name, db_type)`
**Params:** conn, table_name, db_type
**Purpose:** Get column metadata from all_tab_columns or DESCRIBE
**Edge Cases:** Schema/table case mismatch, system tables, partitioned columns
**Caveat:** Different query structure for oracle vs athena

### `get_row_counts_by_date(conn, table_name, date_var, where_clause=None)`
**Params:** conn, table_name, date_var, where_clause
**Purpose:** Raw count query grouped by date
**Edge Cases:** Same as meta_check versions
**Caveat:** Returns raw date values, no parsing

---

## Utilities: utils_s3.py

### `S3Manager.__init__(s3_bucket, run_name)`
**Params:** s3_bucket (s3://bucket format), run_name
**Purpose:** Initialize S3 manager, renew credentials if Windows
**Edge Cases:** Invalid bucket format, bucket doesn't exist, no credentials
**Caveat:** Windows calls aws_creds_renew(), sets base_path={bucket}/{run_name}

### `upload_json(data, step, filename)`
**Params:** data (dict), step (str), filename (str)
**Purpose:** Upload dict as JSON to s3://{bucket}/{run_name}/{step}/{filename}
**Edge Cases:** Data not JSON serializable, S3 write permissions denied, network failure
**Caveat:** Auto-appends .json if missing, uses json.dumps with indent=2

### `download_json(step, filename)`
**Params:** step, filename
**Purpose:** Download JSON from S3, parse to dict
**Edge Cases:** File doesn't exist, invalid JSON, expired credentials
**Caveat:** Raises exception if not found

### `upload_file(local_path, step, filename)`
**Params:** local_path (str), step, filename
**Purpose:** Upload any file to S3
**Edge Cases:** Local file doesn't exist, large files (>5GB need multipart), permissions
**Caveat:** No progress indicator

### `download_file(step, filename, local_path)`
**Params:** step, filename, local_path
**Purpose:** Download file from S3 to local path
**Edge Cases:** S3 file not found, local path not writable, parent dir doesn't exist
**Caveat:** Creates parent dirs with mkdir(parents=True)

### `aws_creds_renew(delta=0)`
**Params:** delta (int, seconds buffer before expiry)
**Purpose:** Windows-only credential renewal via token API
**Edge Cases:** No internet, API down, invalid credentials, expired session
**Caveat:** Only runs if inWindows=True and s3_is_expired(delta), requires env vars AWS_USR, AWS_PWD, AWS_HOST

---

## Integration Tests

### End-to-End Flow
1. **Setup:** Create test env files (input_pcds, input_aws), mock S3 bucket, test TOML/Excel
2. **Part 1:** Run meta_check_pcds, verify local JSON created, S3 upload succeeded, logger file exists
3. **Part 2:** Run meta_check_aws, verify downloads PCDS results, creates AWS JSON, uploads
4. **Part 3:** Run compare_report, verify downloads both, generates Excel with ExcelReporter, uploads
5. **Validation:** Check Excel has SUMMARY sheet + detail sheets, JSON structure correct, S3 paths match

### Critical Edge Cases (Cross-Function)
- **Empty tables:** All row counts = 0, vintages = []
- **Mismatched tables:** PCDS has 5 tables, AWS has 3 (zip will truncate)
- **Date variable NULL:** Skip vintage generation, row_counts should work without dates
- **Network failures:** S3 upload/download retries, AWS credential refresh
- **Concurrent runs:** Same run_name overwrites S3 files
- **Missing crosswalk:** Unmapped = all actual columns
- **Case sensitivity:** PCDS uppercase vs AWS lowercase column names

### Performance Benchmarks
- **PCDS query time:** <10s per table for 1M rows
- **Athena query time:** <30s per table (cold start)
- **S3 upload/download:** <5s per JSON file
- **Excel generation:** <1min for 10 tables with xlwings
- **Date parsing:** <1s for 365 dates with dateparser

### Validation Criteria
- ✓ All enabled tables processed
- ✓ Accessible tables have row_counts, crosswalk, vintages
- ✓ Inaccessible tables flagged, not skipped entirely
- ✓ Local JSON + S3 JSON identical
- ✓ Excel report opens without errors
- ✓ Logger captures all info/error messages
- ✓ No hardcoded paths (use env vars)
- ✓ PYTHONPATH works (no sys.path.insert)

---

## Part 4: column_check_pcds.py

### `get_column_stats(args)`
**Params:** args tuple (svc, table_name, col_name, col_type, where_clause)
**Purpose:** Worker function to get single column stats (categorical/continuous based on type)
**Edge Cases:** Column type unknown, TIMESTAMP columns (uses TRUNC), NULL values, division by zero for categorical
**Caveat:** Returns None on any exception, uses build_column_sql() which differentiates categorical vs continuous

### `get_vintage_stats(svc, table_name, columns_with_types, vintage, max_workers=1)`
**Params:** svc, table_name, columns_with_types (dict col->type), vintage (dict with where_clause), max_workers
**Purpose:** Get stats for all columns in vintage, optionally parallel using ThreadPoolExecutor
**Edge Cases:** max_workers=1 (sequential), max_workers>len(columns), worker exceptions, timeout
**Caveat:** ThreadPoolExecutor for I/O-bound tasks, collects results via as_completed(), failed columns return None

### `main(max_workers=1)`
**Params:** max_workers (default 1 for sequential, CLI arg for parallel e.g. python script.py 5)
**Purpose:** Orchestrate column stats for all tables/vintages from meta_check, upload to S3
**Edge Cases:** Meta results not found in S3, no comparable columns, crosswalk missing pcds_types, workers > available cores
**Caveat:** Uses meta_check results (vintages + comparable columns + types), json.dump with default=str for serialization

---

## Part 5: column_check_aws.py

### `get_column_stats(args)`
**Params:** args tuple (database, table_name, col_name, col_type, where_clause)
**Purpose:** Worker function for AWS column stats (categorical/continuous)
**Edge Cases:** Athena timeout (30s default), APPROX_DISTINCT vs COUNT DISTINCT, ARRAY_JOIN for top 10 frequencies
**Caveat:** Uses is_oracle=False in build_column_sql(), Athena-specific syntax (STDDEV_SAMP, CAST AS DOUBLE)

### `get_vintage_stats(database, table_name, columns_with_types, vintage, max_workers=1)`
**Params:** Same as PCDS but database instead of svc
**Purpose:** Parallel stats collection for AWS columns
**Edge Cases:** Same as PCDS, plus AWS credential expiry (Windows), Athena query queue limits
**Caveat:** ThreadPoolExecutor safe for Athena (creates separate connections per thread)

### `main(max_workers=1)`
**Params:** max_workers (CLI arg)
**Purpose:** Download meta results, compute AWS column stats, upload
**Edge Cases:** Meta results mismatch with PCDS, aws_types dict missing columns
**Caveat:** Reads input_aws (not input_pcds), downloads aws_{category}_meta.json

---

## Part 6: column_check_compare.py

### `compare_column_stats(col_name, pcds_stats, aws_stats)`
**Params:** col_name, pcds_stats (dict), aws_stats (dict)
**Purpose:** Compare single column stats across PCDS/AWS
**Edge Cases:** NULL stats, floating point precision differences for avg/std, string vs numeric min/max
**Caveat:** Returns None if either stats missing, match flags (count_match, distinct_match, min_match, max_match)

### `prepare_table_sections(pcds_result, aws_result)`
**Params:** pcds_result, aws_result (from column_check JSON)
**Purpose:** Build Excel sections with per-vintage comparisons
**Edge Cases:** Vintage mismatch (PCDS has vintages AWS doesn't), empty stats dict
**Caveat:** Only compares common vintages (intersection), creates DataFrame per vintage

### `main()`
**Params:** None
**Purpose:** Download both column stats, compare, generate Excel with ExcelReporter
**Edge Cases:** Results length mismatch, column order different, xlwings not installed
**Caveat:** Summary shows total mismatches across all vintages

---

## Utilities: utils_stats.py

### `is_numeric_type(data_type, is_oracle=True)`
**Params:** data_type (str), is_oracle (bool)
**Purpose:** Determine if column is continuous (numeric) vs categorical
**Edge Cases:** Complex types (NUMBER(10,2)), case sensitivity, unknown types default to categorical
**Caveat:** Oracle: NUMBER/FLOAT/BINARY_*, Athena: int/double/decimal/float/bigint

### `build_continuous_sql_oracle(table_name, col_name, col_type, where_clause)`
**Params:** table_name, col_name, col_type, where_clause
**Purpose:** Generate Oracle SQL for continuous variable (direct MIN/MAX/AVG/STDDEV)
**Edge Cases:** NULL values (COUNT excludes, avg/stddev ignore), empty table, division by zero
**Caveat:** Returns col_category='continuous', col_freq='' (no frequency distribution)

### `build_categorical_sql_oracle(table_name, col_name, col_type, where_clause)`
**Params:** Same as continuous
**Purpose:** Generate Oracle SQL for categorical (frequency distribution first, then stats on frequencies)
**Edge Cases:** TIMESTAMP columns (uses TRUNC), NULL values treated as category, LISTAGG overflow (>4000 chars)
**Caveat:** Returns stats on frequencies (not raw values), col_freq contains top 10 values with counts, uses LISTAGG

### `build_continuous_sql_athena(table_name, col_name, col_type, where_clause)`
**Params:** Same as Oracle
**Purpose:** Athena SQL for continuous variables
**Edge Cases:** CAST AS DOUBLE for avg/stddev, APPROX_DISTINCT faster but approximate
**Caveat:** Uses STDDEV_SAMP (sample stddev, not population), VARCHAR casting for min/max display

### `build_categorical_sql_athena(table_name, col_name, col_type, where_clause)`
**Params:** Same as Oracle
**Purpose:** Athena SQL for categorical (CTE with ROW_NUMBER, ARRAY_JOIN for top 10)
**Edge Cases:** ARRAY_JOIN has no size limit like LISTAGG, NULL handling with COALESCE
**Caveat:** Uses ROW_NUMBER() OVER for ranking, only processes rn <= 10 in final SELECT

### `build_column_sql(table_name, col_name, col_type, where_clause, is_oracle=True)`
**Params:** table_name, col_name, col_type, where_clause, is_oracle
**Purpose:** Router function to build correct SQL based on type and platform
**Edge Cases:** Type detection failure defaults to categorical
**Caveat:** Single entry point for all column stats SQL generation

### `parse_stats_row(row)`
**Params:** row (dict or Series)
**Purpose:** Parse SQL result row into standardized stats dict
**Edge Cases:** Missing columns in result, type mismatches
**Caveat:** Returns dict with keys: col_name, col_type, col_category, count, distinct, max, min, avg, std, sum, sum_sq, freq_top10, missing

---

## Integration Tests: Column Check

### End-to-End Flow
1. **Prerequisite:** Meta check completed (vintages + comparable columns + types available)
2. **Part 4:** Run column_check_pcds with workers=5, verify JSON created with stats per vintage per column
3. **Part 5:** Run column_check_aws with workers=5, verify AWS stats JSON
4. **Part 6:** Run comparison, verify Excel with summary + per-vintage detail sheets
5. **Validation:** Check continuous vs categorical handled correctly, top 10 frequencies for categorical

### Critical Edge Cases (Column Check)
- **151 columns, 5 workers:** Batching works correctly, no race conditions, all columns processed
- **Categorical overflow:** LISTAGG >4000 chars (Oracle), ARRAY_JOIN handles gracefully (Athena)
- **Mixed types:** Same column name different types PCDS vs AWS (e.g., NUMBER vs string)
- **NULL statistics:** Categorical column with NULL as most frequent value
- **Single value column:** distinct=1, stddev=0 or NULL
- **Empty vintage:** WHERE clause filters all rows, count=0
- **Worker failures:** Some columns fail, others succeed, partial results saved
- **Floating point precision:** avg/std comparison tolerance needed (not exact match)

### Performance Benchmarks (Column Check)
- **Sequential (workers=1):** ~5s per column (continuous), ~15s per column (categorical with 1M rows)
- **Parallel (workers=5):** ~1-3s per column (I/O bound, Athena/Oracle query time dominates)
- **Optimal workers:** min(num_columns, num_cores, db_connection_pool_size), typically 5-10
- **Memory usage:** <100MB per worker (result rows small, pagination not needed)
- **S3 JSON size:** ~10KB per table with 100 columns x 10 vintages

### Validation Criteria (Column Check)
- ✓ All comparable columns from meta_check processed
- ✓ Continuous vs categorical correctly identified
- ✓ Categorical columns have freq_top10 populated (up to 10 values)
- ✓ Continuous columns have freq_top10 = ''
- ✓ Parallel execution completes without deadlock
- ✓ Failed columns logged, don't crash entire run
- ✓ Stats match between sequential and parallel runs
- ✓ Excel report includes all vintages from meta_check
- ✓ Comparison shows categorical frequency mismatches

---

# Part 7-9: Hash Check

## Part 7: hash_check_pcds.py

### `add_logger(folder)`
**Params:** folder (output folder path)
**Purpose:** Setup logger to events.log, remove existing handlers
**Edge Cases:** Folder doesn't exist (creates it), permission denied
**Caveat:** Deletes existing events.log if present

### `compute_vintage_hash(args)`
**Params:** args = (svc, table_name, columns_with_types, key_columns, vintage, debug)
**Purpose:** Compute SHA-256 hash for each row in vintage using clean columns only
**Edge Cases:** Empty result, SQL execution failure, missing key columns
**Caveat:** Returns hashes per row (no GROUP BY), includes key columns for row identification, debug mode returns normalized column values

### `compute_table_hashes(svc, table_name, columns_with_types, key_columns, vintages, max_workers=1, debug=False)`
**Params:** svc, table_name, columns_with_types, key_columns, vintages, max_workers, debug
**Purpose:** Compute hashes for all vintages with parallel execution
**Edge Cases:** Worker failures, empty vintages, key column missing from data
**Caveat:** Sequential if max_workers=1, parallel with ThreadPoolExecutor otherwise

### `main(max_workers=1, debug=False)`
**Params:** max_workers (int), debug (bool)
**Purpose:** Download meta + column summary, compute hashes for clean columns only, upload to S3
**Edge Cases:** No clean columns (all mismatched), missing column summary JSON, S3 upload failure
**Caveat:** Uses only clean_columns (excludes mismatched from column_check), selects top 3 key columns by distinct count, saves local + S3

---

## Part 8: hash_check_aws.py

### `add_logger(folder)`
**Params:** folder
**Purpose:** Same as PCDS
**Edge Cases:** Same as PCDS
**Caveat:** Same as PCDS

### `compute_vintage_hash(args)`
**Params:** args = (database, table_name, columns_with_types, key_columns, vintage, debug)
**Purpose:** Compute SHA-256 hash per row using Athena SQL (to_hex(sha256(to_utf8(...))))
**Edge Cases:** Same as PCDS, CAST AS DOUBLE failures, DATE_PARSE issues
**Caveat:** Uses row_number() OVER () if no key columns, Athena-specific hash function

### `compute_table_hashes(database, table_name, columns_with_types, key_columns, vintages, max_workers=1, debug=False)`
**Params:** database, table_name, columns_with_types, key_columns, vintages, max_workers, debug
**Purpose:** Same as PCDS but for Athena
**Edge Cases:** Same as PCDS
**Caveat:** Same as PCDS

### `main(max_workers=1, debug=False)`
**Params:** max_workers, debug
**Purpose:** Same as PCDS but for AWS tables
**Edge Cases:** Same as PCDS
**Caveat:** Uses column summary to get clean_columns and key_columns, reads from input_aws env

---

## Part 9: hash_check_compare.py

### `add_logger(folder)`
**Params:** folder
**Purpose:** Same as PCDS
**Edge Cases:** Same as PCDS
**Caveat:** Same as PCDS

### `compare_vintage_hashes(pcds_hashes, aws_hashes, key_columns)`
**Params:** pcds_hashes, aws_hashes (hash_data dicts), key_columns (list)
**Purpose:** Merge on key columns, identify matched/mismatched hashes, PCDS-only, AWS-only rows
**Edge Cases:** Empty DataFrames, key column mismatch (same key different hash), merge on multiple keys
**Caveat:** Returns first 100 sample mismatches, uses outer merge with indicator

### `prepare_table_sections(pcds_result, aws_result)`
**Params:** pcds_result, aws_result (from hash_check JSON)
**Purpose:** Build Excel sections with per-vintage hash comparison
**Edge Cases:** Vintage mismatch, no key columns (uses row_id)
**Caveat:** Shows mismatched/clean columns, sample mismatch rows with key+hash values

### `main()`
**Params:** None
**Purpose:** Download both hashes, compare, generate Excel with right-aligned summary
**Edge Cases:** Results length mismatch, key columns missing in hash data
**Caveat:** Summary right-aligned (numeric columns), Excel saved locally only (no S3 upload)

---

## Utilities: utils_hash.py

### `normalize_oracle_column(column_name, data_type, decimals=3)`
**Params:** column_name, data_type, decimals
**Purpose:** Generate Oracle SQL expression to normalize column value (numbers, strings, dates, timestamps)
**Edge Cases:** Unknown data types (defaults to string), NULL values, pseudo-null strings ('NULL', 'N/A', etc.)
**Caveat:** Numbers: 0 or NULL -> '0', non-zero -> fixed decimal with NLS-independent format; Strings: UPPER/TRIM, remove control chars, collapse spaces; Dates: 'YYYY-MM-DD'; Timestamps: 'YYYY-MM-DD HH24:MI:SS.FF3'

### `normalize_athena_column(column_name, data_type, decimals=3)`
**Params:** column_name, data_type, decimals
**Purpose:** Generate Athena SQL expression to normalize column value
**Edge Cases:** Same as Oracle, CAST AS DOUBLE for numeric operations
**Caveat:** Uses FORMAT() for decimals, DATE_FORMAT/format_datetime for dates, regexp_replace for string cleanup

### `build_oracle_hash_expr(columns, separator='|')`
**Params:** columns (list of {'column_name', 'data_type'}), separator
**Purpose:** Build Oracle RAWTOHEX(STANDARD_HASH(concat_expr, 'SHA256'))
**Edge Cases:** Empty columns list (returns NULL), data type unknown
**Caveat:** Returns dict with hash_expr, concat_expr, debug_select (shows each normalized column + concat + hash)

### `build_athena_hash_expr(columns, separator='|', decimals=3, null_sentinel='')`
**Params:** columns, separator, decimals, null_sentinel
**Purpose:** Build Athena to_hex(sha256(to_utf8(concat_expr)))
**Edge Cases:** Same as Oracle, COALESCE with null_sentinel
**Caveat:** Uses array_join with COALESCE to avoid NULL propagation, returns same dict structure as Oracle

---

## Utilities: column_check_compare.py (Updated)

### `analyze_column_quality(pcds_result, aws_result)`
**Params:** pcds_result, aws_result (from column_check JSON)
**Purpose:** Identify mismatched columns (count/distinct differ in ANY vintage), clean columns (matched), top 3 key columns by distinct count
**Edge Cases:** All columns mismatch (no clean columns), distinct counts tied, less than 3 columns
**Caveat:** Mismatch if count OR distinct differ, top_key_columns selected from clean_columns only, sorted by distinct descending

### `main()` (Updated)
**Purpose:** Now saves column_summary.json with mismatched/clean/key columns for hash check
**Edge Cases:** Same as before
**Caveat:** Uploads {category}_column_summary.json to S3 column_check folder

---

## Utilities: utils_s3.py (Updated)

### `get_s3_path(step, filename)` (Updated)
**Params:** step (can be empty string or None), filename
**Purpose:** Normalize S3 path construction, allow empty step
**Edge Cases:** step=None, step='', step with leading/trailing slashes
**Caveat:** If step empty: s3://bucket/run_name/filename, else: s3://bucket/run_name/step/filename

### `upload_json(data, step, filename)` (Updated)
**Purpose:** Normalize key construction using step (handles empty step)
**Edge Cases:** Same as get_s3_path
**Caveat:** Consistent path normalization before upload

### `download_json(step, filename)` (Updated)
**Purpose:** Same as upload, handles empty step
**Edge Cases:** Same as get_s3_path
**Caveat:** Same as upload

---

## Utilities: utils_xlsx.py (Updated)

### `create_summary_sheet(title, headers, data_rows)` (Updated)
**Purpose:** Now applies right-alignment to numeric columns (B onwards)
**Edge Cases:** Empty data_rows, single column (no numeric columns)
**Caveat:** Right-aligns range B4:{last_col}{last_row} using xlRight (-4152), first column (A) left-aligned

---

## Integration Tests: Hash Check

### End-to-End Flow
1. **Prerequisite:** Column check completed (column summary with mismatched/clean/key columns available)
2. **Part 7:** Run hash_check_pcds with workers=5, verify JSON with hashes per row per vintage using clean columns
3. **Part 8:** Run hash_check_aws with workers=5, verify AWS hash JSON
4. **Part 9:** Run comparison, verify Excel with summary (right-aligned) + per-vintage hash comparison
5. **Validation:** Check only clean columns used, key columns selected (top 3 by distinct), hash mismatches identified

### Critical Edge Cases (Hash Check)
- **No clean columns:** All comparable columns have mismatches -> hash check skipped
- **<3 columns:** Less than 3 clean columns -> use all available as key columns
- **Large result set:** 10M rows x 3 key columns x hash -> DataFrame memory usage, pagination needed
- **Hash collision:** Different rows produce same hash (astronomically unlikely with SHA-256)
- **Key collision:** Multiple rows same key values but different data -> hash mismatch detected
- **PCDS-only rows:** Key exists in PCDS not AWS -> reported as pcds_only_rows
- **AWS-only rows:** Key exists in AWS not PCDS -> reported as aws_only_rows
- **Debug mode:** Returns all normalized column values -> very large JSON (use sparingly)
- **NULL in key column:** Merge behavior undefined, may cause false mismatches

### Performance Benchmarks (Hash Check)
- **Hash computation:** ~0.5-2s per 1M rows (database CPU intensive, SHA-256 calculation)
- **Parallel execution:** workers=5 reduces total time by ~3-4x (I/O bound, network latency)
- **JSON size:** ~500KB per 100K rows with 3 key columns + hash (base64 64 chars)
- **Comparison memory:** Merge of 10M rows x 3 keys -> ~1-2GB RAM (pandas DataFrame)
- **Optimal workers:** Same as column_check (5-10), limited by DB connection pool

### Validation Criteria (Hash Check)
- ✓ Only clean columns used for hash (mismatched columns excluded)
- ✓ Top 3 key columns by distinct count selected
- ✓ Hash computed per row (no GROUP BY)
- ✓ Debug mode shows normalized column values
- ✓ PCDS and AWS use same normalization rules (Oracle vs Athena SQL)
- ✓ Hash comparison matches on key columns
- ✓ Mismatch sample shows key values + both hashes
- ✓ Summary sheet right-aligned (numeric columns)
- ✓ PCDS-only and AWS-only rows identified
- ✓ Excel report includes all vintages from meta_check

---

## Complete Pipeline Flow

```
Meta Check (Parts 1-3)
  ↓ vintages, comparable columns, types
Column Check (Parts 4-6)
  ↓ column stats, mismatched columns, clean columns, top 3 key columns
Hash Check (Parts 7-9)
  ↓ row-level hash comparison using clean columns + key columns
```

**Key Dependencies:**
- Hash check requires column summary (mismatched/clean/key columns)
- Column check requires meta check (vintages, comparable columns, types)
- All steps upload JSON to S3 for cross-machine workflow

**S3 Structure:**
```
s3://bucket/run_name/
  meta_check/
    pcds_{category}_meta.json
    aws_{category}_meta.json
  column_check/
    pcds_{category}_column_stats.json
    aws_{category}_column_stats.json
    {category}_column_summary.json          # NEW
  hash_check/
    pcds_{category}_hash.json               # NEW
    aws_{category}_hash.json                # NEW
```

**Local Excel Reports (not uploaded to S3):**
```
output/{run_name}/
  meta_check_comparison_{category}_{timestamp}.xlsx
  column_check_comparison_{category}_{timestamp}.xlsx
  hash_check_comparison_{category}_{timestamp}.xlsx
```
