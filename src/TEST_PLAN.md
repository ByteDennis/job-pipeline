# Pipeline Test Plan

## Step 1: Load Config (`test_01_load_config.py`)

### Config Loading
- `test_config_loader_parse_cfg()` - Parses .cfg file sections correctly
- `test_config_loader_missing_file()` - Raises FileNotFoundError for missing config
- `test_config_loader_get_section()` - Retrieves config sections correctly

### Excel Input Processing
- `test_read_excel_with_column_map()` - Reads Excel and applies COLUMN_MAP transformation
- `test_read_excel_missing_columns()` - Handles missing required columns gracefully
- `test_merge_pcds_svc_tbl()` - Merges PCDS service and table name correctly

### Pipeline State Management
- `test_pipeline_state_init()` - Initializes state with correct structure
- `test_pipeline_state_add_table()` - Adds table info to state
- `test_pipeline_state_mark_complete()` - Marks steps as completed
- `test_pipeline_state_save_load()` - Saves and loads state from JSON

### End-to-End Step 1
- `test_step1_main_success()` - Creates state file with all tables loaded
- `test_step1_already_initialized()` - Skips if state file already exists
- `test_step1_output_folder_creation()` - Creates output/{run_name}/ folder structure

---

## Step 2: Row Count Check (`test_02_row_count_check.py`)

### Date Variable Handling
- `test_extract_date_var_with_format()` - Extracts variable and format from "var (%Y%m%d)"
- `test_extract_date_var_no_format()` - Handles plain variable name without format
- `test_extract_date_var_empty()` - Returns empty for NaT or empty string

### Query Generation
- `@pytest.mark.parametrize("has_date_var", [True, False])`
  `test_build_pcds_count_query()` - Generates Oracle query with/without partition
- `@pytest.mark.parametrize("date_format", [None, "%Y%m%d", "%Y-%m-%d"])`
  `test_build_aws_count_query()` - Generates Athena query with correct DATE_FORMAT

### Error Handling
- `test_check_table_inaccessible_pcds()` - Handles PCDS connection errors
- `test_check_table_missing_date_var()` - Handles missing partition variable
- `test_check_table_row_counts_success()` - Returns complete result dict with all fields

### State Updates
- `test_process_all_tables_updates_state()` - Updates state with row count results
- `test_step2_marks_global_complete()` - Marks step2_row_count in global_steps_completed

---

## Step 3: Column Mapping (`test_03_column_mapping.py`)

### Crosswalk Processing
- `test_load_crosswalk()` - Loads Excel and cleans strings
- `test_get_table_crosswalk()` - Filters crosswalk by OnPremView name
- `test_extract_pii_columns()` - Extracts columns with PII_Encryption='Yes'

### Column Matching
- `@pytest.mark.parametrize("pcds_col,aws_col,expected", [("ACCT_BID", "bank_acct_nbr", True)])`
  `test_build_column_mapping()` - Identifies comparable/only/tokenized columns
- `test_find_undocumented_matches()` - Fuzzy matches PCDS-only and AWS-only columns

### Type Validation
- `@pytest.mark.parametrize("oracle_type,athena_type,compatible", [("NUMBER(12)", "bigint", True), ("VARCHAR2(100)", "string", True)])`
  `test_types_compatible()` - Validates Oracle-Athena type compatibility
- `test_validate_data_types()` - Detects type mismatches in crosswalk

### Metadata Queries
- `test_get_pcds_meta_query()` - Generates Oracle metadata query with data types
- `test_get_aws_meta_query()` - Generates Athena information_schema query

---

## Step 4: Column Statistics (`test_04_column_statistics.py`)

### Type Detection
- `@pytest.mark.parametrize("data_type,is_oracle,expected", [("NUMBER(12)", True, True), ("varchar(100)", False, False)])`
  `test_is_numeric_type()` - Correctly identifies numeric vs categorical types

### PCDS SAS Generation
- `test_generate_pcds_stats_sas_continuous()` - Generates direct stats for numeric columns
- `test_generate_pcds_stats_sas_categorical()` - Generates frequency-based stats for categorical
- `test_pcds_sas_includes_email_notification()` - SAS code includes FILENAME outbox EMAIL

### AWS SQL Generation
- `test_generate_aws_stats_sql_continuous()` - Generates direct stats query (AVG, STDDEV_SAMP, etc.)
- `test_generate_aws_stats_sql_categorical()` - Generates CTE with FreqTable and top 10 values
- `test_aws_sql_includes_col_name()` - Query outputs col_name column

### Query Execution
- `test_execute_all_aws_queries_parallel()` - Executes queries with ThreadPoolExecutor
- `test_execute_handles_failures()` - Continues on query failure, returns empty DataFrame

### Comparison
- `test_mode_compare()` - Compares PCDS and AWS stats, detects mismatches

### Workflow
- `test_process_table_no_column_mapping()` - Returns error status when mapping unavailable
- `test_step4_skip_if_files_exist()` - Skips generation if .sas/.sql files exist
- `test_step4_prompts_user_before_execution()` - Requires Y/Yes confirmation (mock input)

---

## Step 5: Hash Comparison (`test_05_value_to_value_check.py`)

### Column Batching
- `@pytest.mark.parametrize("total_cols,batch_size,expected_batches", [(25, 10, 3), (20, 20, 1)])`
  `test_batch_columns()` - Splits columns into correct batch sizes

### Hash Query Generation
- `test_generate_pcds_hash_sas()` - Generates SAS with STANDARD_HASH and batching
- `test_generate_aws_hash_sql()` - Generates Athena with TO_HEX(MD5()) and batching
- `test_hash_queries_include_key_column()` - Both queries include key column for merging

### SQL Normalization
- `@pytest.mark.parametrize("value,expected", [("  ABC  ", "ABC"), ("123.000", "123")])`
  `test_normalize_oracle_column()` - TRIM, UPPER, normalize decimals
- `test_normalize_athena_column()` - Same normalization for Athena
- `test_build_hash_expr()` - Builds consistent hash expressions for both platforms

### Parallel Execution
- `test_execute_all_aws_hash_queries()` - Executes batches in parallel with ThreadPoolExecutor
- `test_execution_saves_batch_results()` - Saves each batch to separate CSV

### Hash Comparison
- `test_mode_compare()` - Compares PCDS and AWS hashes by key_col
- `test_compare_detects_mismatches()` - Identifies rows with different hashes
- `test_compare_detects_only_in_pcds()` - Identifies rows only in PCDS (not in AWS)
- `test_compare_detects_only_in_aws()` - Identifies rows only in AWS (not in PCDS)

### Workflow
- `test_process_table_generates_batches()` - Creates multiple .sas/.sql batch files
- `test_step5_skip_if_batch_files_exist()` - Skips generation if batch files exist

---

## Integration Tests (`test_integration.py`)

### End-to-End Workflow
- `test_e2e_step1_to_step3()` - Runs steps 1-3 sequentially, verifies state progression
- `test_e2e_all_steps_with_mock_db()` - Full pipeline with mocked DB queries
- `test_resume_from_step3()` - Resumes pipeline from step 3 after step 2 completion

### State Persistence
- `test_state_survives_reload()` - State file persists across step executions
- `test_global_steps_completed_tracking()` - Global steps list accumulates correctly

### Error Recovery
- `test_step_fails_if_previous_incomplete()` - Step N fails if step N-1 not complete
- `test_skip_already_completed_steps()` - Already-completed steps return early with warning

### File Management
- `test_output_folder_structure()` - Verifies output/{run_name}/ contains all expected files
- `test_config_copied_to_output()` - config.cfg copied to output folder

---

## Utilities Tests (`test_utils.py`)

### String Cleaning
- `@pytest.mark.parametrize("input,expected", [("  test  ", "test"), (None, ""), (123, "123")])`
  `test_clean_string()` - Cleans and normalizes strings

### SQL Normalization
- `test_normalize_oracle_preserves_order()` - Normalized columns maintain order
- `test_normalize_athena_handles_nulls()` - NULL values handled correctly
- `test_build_hash_expr_consistency()` - Same input produces same hash across platforms

### JSON I/O
- `test_write_read_json()` - Writes and reads JSON with correct structure
- `test_read_json_missing_file()` - Raises FileNotFoundError for missing JSON

---

## Parametrized Test Examples

```python
@pytest.mark.parametrize("table_name,expected_mapping_count", [
    ("V_ACCT_DTL_R_CURR", 6),
    ("V_CUST_INFO", 4)
])
def test_column_mapping_counts(table_name, expected_mapping_count):
    """Each table has expected number of mapped columns"""
    pass

@pytest.mark.parametrize("partition_type,expected_format", [
    ("daily", "%Y-%m-%d"),
    ("year_month", "%Y-%m"),
    ("snapshot", None)
])
def test_partition_date_format(partition_type, expected_format):
    """Partition types generate correct date format strings"""
    pass
```

---

## Test Fixtures (`conftest.py`)

```python
@pytest.fixture
def temp_dir():
    """Temporary directory for test outputs"""

@pytest.fixture
def sample_config_file(temp_dir):
    """Creates sample config.cfg"""

@pytest.fixture
def sample_excel_file(temp_dir):
    """Creates sample input_table_list.xlsx"""

@pytest.fixture
def sample_crosswalk_file(temp_dir):
    """Creates sample crosswalk_doc.xlsx"""

@pytest.fixture
def sample_state_file(temp_dir):
    """Creates sample pipeline_state.json"""

@pytest.fixture
def mock_oracle_connection():
    """Mocked Oracle connection for PCDS queries"""

@pytest.fixture
def mock_athena_connection():
    """Mocked Athena connection for AWS queries"""
```

---

## Coverage Goals

- **Unit Tests**: 80%+ coverage for each step module
- **Integration Tests**: Cover happy path + 2-3 error scenarios per step
- **Edge Cases**: NULL values, empty tables, missing columns, type mismatches
- **Performance**: Parallel execution actually runs concurrently (check timing)
