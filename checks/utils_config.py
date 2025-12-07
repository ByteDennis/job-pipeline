# Configuration Loader for Data Validation Pipeline
# Contains all configuration and database functions (no end2end.py dependency)

import os
import sys
import re
import warnings
from pathlib import Path
from typing import Literal
from datetime import datetime, timedelta, date
import pandas as pd
import pandas.io.sql as psql
from dateutil import parser
from dotenv import load_dotenv
from loguru import logger
import tomllib
import attridict

# Import database libraries
import oracledb
from pyathena import connect as athena_connect_raw

# Import utils
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))
import utils

# Import from s3_utils for AWS credentials
from s3_utils import aws_creds_renew, inWindows

# Constants
NO_DATE = pd.NaT
MIN_TIME = pd.Timestamp.min.time()
DATE_FMT = '%Y-%m-%d'
TIME_FMT = '%Y-%m-%d %H:%M:%S'
PLATFORM = Literal['PCDS', 'AWS']
CATEGORY = Literal['dpst', 'loan']
PARTITION = Literal['snapshot', 'all', 'year', 'month', 'week']

SVC2SERVER = {
    "pcds_svc": {
        "server": "PCDS",
        "auth": "OraPCDSAuth",
    },
    "p_uscb_cnsmrlnd_svc": {
        "server": "PBCS21P",
        "auth": "OraPBCS21PAuth",
    },
    "p_uscb_rft_svc": {
        "server": "PBCS30P",
        "auth": "OraPBCS30PAuth",
    }
}


#>>> Solve LDAP DSN to get TNS connect string <<<#
def solve_ldap(ldap_dsn: str):
    from ldap3 import Server, Connection, ALL
    pattern = r"^ldap:\/\/(.+)\/(.+)\,(cn=OracleContext.*)$"
    x = re.match(pattern, ldap_dsn)
    if not x:
        return None
    else:
        ldap_server, db, ora_context = x.groups()
    server = Server(ldap_server, get_info=ALL)
    conn = Connection(server)
    conn.bind()
    conn.search(ora_context, f"(cn={db})", attributes=['orclNetDescString'])
    tns = conn.entries[0].orclNetDescString.value
    return tns


#>>> Connect to PCDS Oracle database <<<#
def pcds_connect(service_name):
    LDAP_DSN = os.environ['LDAP_DSN']

    # Determine credentials based on service type
    if service_name.startswith('tmp'):
        usr, pwd = os.environ['TMP_USR'], os.environ['TMP_PWD']
        service_name = service_name.replace('tmp_', '')
    else:
        PCDS_PWD = SVC2SERVER.get(service_name)['server']
        usr, pwd = os.environ['PCDS_USR'], os.environ[PCDS_PWD]

    # Validate service name
    if service_name not in SVC2SERVER:
        raise pd.errors.DatabaseError("Service Name Is Not Provided")

    # Resolve LDAP DSN for the given service
    dns_tns = solve_ldap(LDAP_DSN.format(service=service_name))

    # Establish and return Oracle DB connection
    return oracledb.connect(user=usr, password=pwd, dsn=dns_tns)


#>>> Connect to AWS Athena database <<<#
def athena_connect(data_base=None):
    kwargs = {
        'region_name': os.environ['AWS_DEFAULT_REGION'],
        'work_group': os.environ['AWS_S3_WORK_GROUP'],
        's3_staging_dir': os.environ['AWS_S3_STAGING_DIR']
    }
    return athena_connect_raw(schema_name=data_base, **kwargs)


#>>> SQL engine class for PCDS and AWS <<<#
class SQLengine:

    def __init__(self, platform: Literal['PCDS', 'AWS']):
        if platform not in ('PCDS', 'AWS'):
            raise ValueError("Platform must be 'PCDS' or 'AWS'")
        self.platform = platform

    #>>> Execute SQL query and return DataFrame with normalized column names <<<#
    def query(self, query_stmt: str, connection, **kwargs) -> pd.DataFrame:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=UserWarning)
            df = psql.read_sql_query(query_stmt, connection, **kwargs)

        df.columns = [c.upper() if self.platform == 'PCDS' else c.lower() for c in df.columns]
        logger.debug(f"Executed query on {self.platform}: {len(df)} rows")
        return df

    #>>> Run query on PCDS Oracle DB <<<#
    def query_PCDS(self, query_stmt: str, service_name: str, **kwargs) -> pd.DataFrame:
        with pcds_connect(service_name=service_name) as conn:
            return self.query(query_stmt, conn, **kwargs)

    #>>> Run query on AWS Athena <<<#
    def query_AWS(self, query_stmt: str, data_base=None, **kwargs) -> pd.DataFrame:
        aws_creds_renew()
        conn = athena_connect(data_base=data_base)
        return self.query(query_stmt, conn, **kwargs)

    def __call__(self, query_stmt, service_name='', data_base='', **kwargs):
        if self.platform == 'PCDS':
            return self.query_PCDS(query_stmt, service_name=service_name, **kwargs)
        elif self.platform == 'AWS':
            return self.query_AWS(query_stmt, data_base=data_base, **kwargs)


# Global SQL engine instances
proc_pcds = SQLengine('PCDS')
proc_aws = SQLengine('AWS')


#>>> Check if value is missing <<<#
def is_missing(x) -> bool | pd.Series:
    null_like = {"", "nat", "nan", "none", "null"}
    if isinstance(x, pd.Series):
        mask = x.isna()
        str_mask = x.astype(str).str.strip().str.lower().isin(null_like)
        return mask | str_mask
    else:
        if x is None or pd.isna(x):
            return True
        if isinstance(x, str) and x.strip().lower() in null_like:
            return True
        return False


#>>> Check if string contains word <<<#
def contain_word(x='', *value) -> bool:
    pattern = '|'.join(value)
    return bool(re.search(r'(?i)\b(%s)\b' % pattern, str(x)))


#>>> Load configuration from TOML file <<<#
def load_config(category: CATEGORY):
    if category == 'dpst':
        config_file = 'configs/config_meta_dpst.toml'
    else:
        config_file = 'configs/config_meta_loan.toml'

    with open(config_file, "rb") as f:
        config_dict = tomllib.load(f)
    return attridict(config_dict)


#>>> Parse Excel date input <<<#
def parse_excel_date(date_input, fmt: str = "%Y-%m-%d") -> str:
    try:
        if isinstance(date_input, (int, float)):
            dt = datetime(1899, 12, 30) + timedelta(days=date_input)
        else:
            dt = parser.parse(str(date_input))
        return dt.strftime(fmt)
    except Exception:
        return ''


#>>> Parse WHERE clause with variable substitution <<<#
def normalize_timestamp(x):
    if isinstance(x, (pd.Timestamp, datetime)) and all(y == 0 for y in [x.hour, x.minute, x.second, x.microsecond]):
        return x.date()
    return x


def parse_where(x: str, func: callable, **kwargs) -> str:
    if not isinstance(x, str):
        return x

    if not (m := re.search(r"([^=]+)=\s*\$\{([^}]*)\}", x)):
        return x

    key, expr = m.groups()
    value = func(expr, **kwargs).iloc[0, 0]

    if value is None:
        formatted_value = "NULL"
    elif isinstance(value, (int, float)):
        formatted_value = str(value)
    elif isinstance(value, date):
        value = normalize_timestamp(value)
        if isinstance(value, pd.Timestamp):
            formatted_value = f"TIMESTAMP '{value}'"
        else:
            formatted_value = f"DATE '{value}'"
    else:
        formatted_value = f"'{value}'"

    return f"{key.strip()} = {formatted_value}"


#>>> Read input tables from Excel <<<#
def read_input_tables(config: dict) -> pd.DataFrame:
    def extract_name(name):
        if pd.isna(name): return pd.NA
        if not isinstance(name, str): return name
        return re.sub(r'\(.*\)', '', name).strip()

    # Extract config
    file_path = config['file'].strip('"')
    sheet_name = config['sheet'].strip('"')
    select_cols = {k: v.strip('"') for k, v in config['select_cols'].items()}

    # Read Excel
    df = pd.read_excel(file_path, sheet_name=sheet_name, usecols=list(select_cols.keys()))

    # Rename columns and trim all string values
    df = df.rename(columns=select_cols).map(lambda x: x.strip() if isinstance(x, str) else x)

    # Drop invalid columns
    df = df[df['aws_tbl'].str.contains(r'^[^.]+\.[^.]+$', na=False)]

    # Normalize enabled flag
    df["enabled"] = df["enabled"].apply(contain_word, args=('yes', 'y'))

    # Normalize start and end dates
    df["start_dt"] = df["start_dt"].apply(parse_excel_date)
    df["end_dt"] = df["end_dt"].apply(parse_excel_date)

    # Clean service/table names
    df["pcds_tbl"] = df["pcds_tbl"].map(extract_name)
    df["pcds_svc"] = df["pcds_svc"].map(extract_name)
    df["pcds_tbl"] = df["pcds_svc"].fillna("no_server") + "." + df["pcds_tbl"].str.lower()

    # Parse WHERE clauses
    df['pcds_where'] = df.apply(lambda r: parse_where(r['pcds_where'], proc_pcds, service_name=r['pcds_svc']), axis=1)
    df['aws_where'] = df.apply(lambda r: parse_where(r['aws_where'], proc_aws, data_base=r['aws_tbl'].split('.')[0]), axis=1)

    # Validate partition column
    df['partition'] = df['partition'].fillna('all')
    if not df['partition'].isin(valid_partitions := set(PARTITION.__args__)).all():
        raise ValueError(f"Invalid partition value. Must be one of {valid_partitions}")

    return df


#>>> Join columns for column mapping <<<#
def join_columns(row: dict, columns: list) -> str:
    for col in columns:
        parts = [x.strip() for x in col.strip().split('+') if x]
        if len(parts) > 1:
            if any(p not in row for p in parts):
                continue
            return '.'.join(str(row[p]) if pd.notna(row[p]) else '' for p in parts)
        elif col in row:
            return row[col]
    return ''


#>>> Load column mappings from crosswalk Excel <<<#
def load_column_mappings(config: dict, category: str) -> pd.DataFrame:
    skips = {s[1:] for s in config['sheets'] if s.startswith('-')}
    sheets = [s for s in config['sheets'] if not s.startswith('-')]

    df_dict = pd.read_excel(config['file'], sheet_name=None, na_values=config['na_str'])

    result = {}
    for name, df in df_dict.items():
        if (name in skips) or (sheets and name not in sheets):
            continue

        # Strip cell values
        df = df.apply(lambda x: x.map(lambda v: v.strip() if isinstance(v, str) else v))

        # Special handling for loan category
        if category == 'loan' and 'Source' in df.columns:
            df = pd.DataFrame(df.iloc[1:].values, columns=df.iloc[0])

        # Build mapping list
        rows = []
        for _, row in df.iterrows():
            rows.append({
                'pcds_view': join_columns(row, config['pcds_view']),
                'pcds_col': join_columns(row, config['pcds_col']),
                'aws_col': join_columns(row, config['aws_col']),
                'aws_view': join_columns(row, config['aws_view']),
                'comment': join_columns(row, config['comment'])
            })
        result[name.lower()] = pd.DataFrame(rows)

    # Flatten result
    if category == 'dpst':
        combined = pd.concat(result.values(), ignore_index=True)
        combined['col_map'] = combined['pcds_view'].str.lower()
    elif category == 'loan':
        combined = pd.concat([df.assign(**{'col_map': key}) for key, df in result.items()], ignore_index=True)

    # Add tokenization flag
    combined['is_tokenized'] = combined['comment'].apply(
        lambda col: ('tokenise' in col.split('.')[0].lower()) if category == 'dpst' else not is_missing(col)
    )

    return combined


#>>> Get vintages from data based on partition type <<<#
def get_vintages_from_data(info_str, date_var, date_type, date_format, partition_type, where_clause="1=1"):
    import functools as ft

    svc, table_name = info_str.split('.')
    if date_format == 'YYYY-MM-DD':
        sql_engine = ft.partial(proc_pcds.query_PCDS, service_name=svc)
        is_pcds = True
    else:
        sql_engine = ft.partial(proc_aws.query_AWS, data_base=svc)
        table_name = f'{svc}.{table_name}'
        is_pcds = False

    try:
        # Parse format if present
        def parse_format_date(str_w_format):
            pattern = r'^(.+?)(?:\s*\(([^)]+)\))?$'
            return re.match(pattern, str_w_format)

        if date_type and ('char' in date_type.lower() or 'varchar' in date_type.lower()):
            if (m := parse_format_date(date_var)):
                actual_var, format_spec = m.groups()
                date_col = f"DATE_PARSE({actual_var}, '{format_spec}')"
            else:  # PCDS format
                date_col = f"TO_DATE({date_var}, '{date_format}')"
        else:
            date_col = date_var

        match partition_type:
            case 'year':
                if is_pcds:
                    select_clause = f"TO_CHAR({date_col}, 'YYYY')"
                else:
                    select_clause = f"DATE_FORMAT({date_col}, '%Y')"
            case 'year_month':
                if is_pcds:
                    select_clause = f"TO_CHAR({date_col}, 'YYYY-MM')"
                else:
                    select_clause = f"DATE_FORMAT({date_col}, '%Y-%m')"
            case _ if partition_type in ('year_week', 'week'):
                if is_pcds:
                    select_clause = f"TO_CHAR({date_col}, 'IYYY') || '-W' || LPAD(TO_CHAR({date_col}, 'IW'), 2, '0')"
                else:
                    select_clause = f"format_datetime({date_col}, 'xxxx-''W''ww')"
            case 'snapshot':
                return ['snapshot']
            case _:
                return ['all']

        sql_stmt = f"""
        SELECT DISTINCT {select_clause} AS vintage
        FROM {table_name}
        WHERE {where_clause}
        ORDER BY vintage DESC
        """
        df = sql_engine(sql_stmt)
        return df.iloc[:, 0].to_list()
    except Exception as e:
        logger.error(f"Error getting vintages: {e}")
        return ['whole']


#>>> Load environment variables <<<#
def load_environment(use_aws: bool = False):
    if use_aws:
        load_dotenv('./src/input_aws')
    else:
        load_dotenv('./src/input_pcds')

    return {
        'run_name': os.getenv('RUN_NAME', 'demo'),
        's3_bucket': os.getenv('S3_BUCKET'),
        'category': os.getenv('CATEGORY', 'dpst'),
        'aws_region': os.getenv('AWS_REGION') if use_aws else None
    }


#>>> Get configuration for category <<<#
def get_config(category: str = None):
    if category is None:
        category = os.getenv('CATEGORY', 'dpst')
    return load_config(category)


#>>> Get input tables configuration <<<#
def get_input_tables(category: str = None):
    cfg = get_config(category)
    return read_input_tables(cfg.input['table'])


#>>> Get column mappings from crosswalk <<<#
def get_column_mappings(category: str = None):
    cfg = get_config(category)
    return load_column_mappings(cfg.column_maps, category or os.getenv('CATEGORY', 'dpst'))


#>>> Get vintages for a table <<<#
def get_table_vintages(info_str: str, date_var: str, date_type: str,
                       date_format: str, partition_type: str,
                       where_clause: str = "1=1"):
    return get_vintages_from_data(
        info_str=info_str,
        date_var=date_var,
        date_type=date_type,
        date_format=date_format,
        partition_type=partition_type,
        where_clause=where_clause
    )


#>>> Query PCDS Oracle database <<<#
def query_pcds(sql: str, service_name: str) -> pd.DataFrame:
    return proc_pcds(sql, service_name=service_name)


#>>> Query AWS Athena database <<<#
def query_aws(sql: str, database: str) -> pd.DataFrame:
    if inWindows:
        aws_creds_renew(delta=300)
    return proc_aws(sql, data_base=database)


#>>> Build Oracle hash expression for columns <<<#
def build_oracle_hash(columns: list) -> str:
    return utils.build_oracle_hash_expr(columns)


#>>> Build Athena hash expression for columns <<<#
def build_athena_hash(columns: list) -> str:
    return utils.build_athena_hash_expr(columns)


#>>> Get ISO week dates <<<#
def get_iso_week_dates(year, week):
    import datetime as dt
    jan01, dec31 = dt.datetime(year, 1, 1), dt.datetime(year, 12, 31)
    first_day = jan01 - dt.timedelta(days=jan01.weekday())
    start = first_day + dt.timedelta(weeks=week - 1)
    end = start + dt.timedelta(days=6)
    start, end = max(start, jan01), min(end, dec31)
    return start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')


#>>> Parse exclude date clause <<<#
def parse_exclude_date(exclude_clause):
    import datetime as dt
    p1 = r"TO_CHAR\((?P<col>\w+),\s*'YYYY-MM-DD'\)\s+(?P<op>not in|in)\s+\((?P<dates>.*?)\)"
    if m := re.match(p1, exclude_clause, flags=re.I):
        col, op, dates = m.groups()
        new_dates = ', '.join(f"DATE {date.strip()}" for date in dates.split(','))
        return '%s %s (%s)' % (col, op, new_dates)
    p2 = r"DATE_FORMAT\(DATE_PARSE\((?P<col>\w+),\s*'(?P<fmt>%Y%m%d)'\),\s*'%Y-%m-%d'\)\s+(?P<op>not in|in)\s+\((?P<dates>.*?)\)"
    if m := re.match(p2, exclude_clause, flags=re.I):
        col, fmt, op, dates = m.groups()
        new_dates = ', '.join("'%s'" % dt.datetime.strptime(date.strip("'"), '%Y-%m-%d').strftime(fmt) for date in dates.split(','))
        return '%s %s (%s)' % (col, op, new_dates)
    return exclude_clause


#>>> Get PCDS WHERE clause for date filtering <<<#
def get_pcds_where(date_var, date_type, date_partition, date_range, date_format, snapshot=None, exclude_clauses=[]):
    # Handle character-based dates
    if date_type and ('char' in date_type.lower() or 'varchar' in date_type.lower()):
        date_var = f"TO_DATE({date_var}, '{date_format}')"
    if snapshot:
        # For snapshot queries, just use exclude queries
        return ' AND '.join(parse_exclude_date(x) for x in exclude_clauses if x)
    elif date_partition == 'whole':
        # No date filtering for whole dataset
        base_clause = "1=1"
    elif date_partition == 'year':
        start_dt = f"TO_DATE('{date_range}-01-01', 'YYYY-MM-DD')"
        end_dt = f"TO_DATE('{date_range}-12-31', 'YYYY-MM-DD')"
        base_clause = f"{date_var} >= {start_dt} AND {date_var} <= {end_dt}"
    elif date_partition == 'year_month':
        start_dt = f"TO_DATE('{date_range}', 'YYYY-MM')"
        end_dt = f"LAST_DAY(TO_DATE('{date_range}', 'YYYY-MM'))"
        base_clause = f"{date_var} >= {start_dt} AND {date_var} <= {end_dt}"
    elif date_partition in ('year_week', 'week'):
        year, week = date_range.split('-W')
        start_dt, end_dt = get_iso_week_dates(int(year), int(week))
        base_clause = f"{date_var} >= DATE '{start_dt}' AND {date_var} <= DATE '{end_dt}'"
    elif date_partition == 'daily':
        target_dt = f"TO_DATE('{date_range}', 'YYYY-MM-DD')"
        base_clause = f"{date_var} = {target_dt}"
    else:
        raise ValueError(f"Unsupported partition type: {date_partition}")

    # Add exclusions if provided
    if (exclude_clauses := [x for x in exclude_clauses if x]):
        exclude_clause = ' AND '.join(parse_exclude_date(x) for x in exclude_clauses if x)
        return f"({base_clause}) AND ({exclude_clause})"
    else:
        return base_clause


#>>> Parse format date <<<#
def parse_format_date(str_w_format):
    pattern = r'^(.+?)(?:\s*\(([^)]+)\))?$'
    return re.match(pattern, str_w_format)


#>>> Get AWS WHERE clause for date filtering <<<#
def get_aws_where(date_var, date_type, date_partition, date_range, date_format, snapshot=None, exclude_clauses=[]):
    # Handle variable=value format
    if '=' in date_range:
        _date_var, date_range = date_range.split('=', 1)
        assert date_var.split()[0] == _date_var, f"Date Variable Should Match: {date_var} vs {_date_var}"
    # Extract format from date_var if present (e.g., "dw_bus_dt (%Y%m%d)")
    if (m := parse_format_date(date_var)):
        date_var, date_format = m.groups()
    # Handle string/varchar dates that need parsing
    if date_type and re.match(r'^(string|varchar)', date_type, re.IGNORECASE):
        if date_format:
            date_var = f"DATE_PARSE({date_var}, '{date_format}')"
        else:
            date_var = f"DATE_PARSE({date_var}, '%Y%m%d')"  # Default format
    if snapshot:
        return ' AND '.join('(%s)' % parse_exclude_date(x) for x in exclude_clauses if x)
    elif date_partition == 'whole':
        base_clause = "1=1"
    elif date_partition == 'year':
        base_clause = f"DATE_FORMAT({date_var}, '%Y') = '{date_range}'"
    elif date_partition == 'year_month':
        base_clause = f"DATE_FORMAT({date_var}, '%Y-%m') = '{date_range}'"
    elif date_partition in ('year_week', 'week'):
        if '-W' in date_range:
            year, week = date_range.split('-W')
        else:
            year, week = map(int, date_range.split('-'))
            week = f"W{week:02d}"
        base_clause = f"DATE_FORMAT({date_var}, '%Y-%v') = '{year}-{week}'"
    elif date_partition == 'daily':
        base_clause = f"DATE({date_var}) = DATE('{date_range}')"
    else:
        raise ValueError(f"Unsupported partition type: {date_partition}")
    if (exclude_clauses := [x for x in exclude_clauses if x]):
        exclude_clause = ' AND '.join('(%s)' % parse_exclude_date(x) for x in exclude_clauses if x)
        return f"({base_clause}) AND ({exclude_clause})"
    else:
        return base_clause
