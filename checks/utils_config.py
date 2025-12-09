# Configuration Loader for Data Validation Pipeline
# Contains all configuration and database functions (no end2end.py dependency)

import os
import re
import sys
import warnings
from typing import Literal, Optional, List
from datetime import datetime, timedelta, date
import pandas as pd
import pandas.io.sql as psql
from dataclasses import dataclass, asdict
from dateutil import parser as date_parser
from loguru import logger
import tomllib
import attridict
import functools as ft
import json

# Import database libraries
import oracledb
from pyathena import connect as athena_connect_raw

# Import from s3_utils for AWS credentials
from utils_s3 import aws_creds_renew
from utils_date import parse_date_to_std

# Constants
NO_DATE = pd.NaT
MIN_TIME = pd.Timestamp.min.time()
DATE_FMT = '%Y-%m-%d'
TIME_FMT = '%Y-%m-%d %H:%M:%S'
PLATFORM = Literal['PCDS', 'AWS']
CATEGORY = Literal['dpst', 'loan']
PARTITION = Literal['snapshot', 'all', 'year', 'month', 'week']

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
def pcds_connect(service_name: str):
    from constant import SVC2SERVER
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
        False and logger.debug(f"Executed query on {self.platform}: {len(df)} rows")
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


COMMON_FORMATS = [
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%d-%m-%Y",
    "%d/%m/%Y",
    "%Y%m%d",
    "%Y-%m-%d %H:%M:%S",
]

def detect_date_format(date_vals: List):
    samples = [v for v in date_vals if v and not pd.isna(v)]
    if not samples:
        return "unknown", None
    first = samples[0]
    if isinstance(first, (datetime, pd.Timestamp)):
        return "date", None

    s = str(first)
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return "string", "%Y-%m-%d"
    if re.match(r"^\d{4}/\d{2}/\d{2}$", s):
        return "string", "%Y/%m/%d"
    if re.match(r"^\d{8}$", s):
        return "string", "%Y%m%d"
    if re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", s):
        return "string", "%Y-%m-%d"
    
    try:
        date_parser.parse(s)
        return "string", None
    except Exception:
        return "string", None


@dataclass
class DateParser:
    _var: str
    _type: str = None
    _fmt: Optional[str] = None

    def __post_init__(self):
        if self._type.lower().startswith(("date", "time")):
            self._type = "date"
        else:
            self._type = "string"
    
    def _get_func(self, service_name = None, data_base = None):
        if service_name:
            return ft.partial(proc_pcds, service_name=service_name)
        else:
            return ft.partial(proc_aws, data_base=data_base)

    def get_fmt(self, table_name: str, service_name = None, data_base = None):
        if self._type == "date":
            self._fmt = None
            return None
        func = self._get_func(service_name, data_base)
        if service_name:
            sql_stmt = f'SELECT {self._var} FROM {table_name} WHERE ROWNUM = 1'
        else:
            sql_stmt = f'SELECT {self._var} FROM {table_name} LIMIT 1'
        sample_dates = func(sql_stmt)
        _, fmt = detect_date_format(sample_dates.iloc[0].tolist())
        self._fmt = fmt
        return fmt

    def get_cnt(self, table_name: str, where_clause: str, service_name = None, data_base = None):
        func = self._get_func(service_name, data_base)
        where = "" if is_missing(where_clause) else f"WHERE {where_clause}"
        df = func(f"SELECT {self._var}, COUNT(*) as cnt FROM {table_name} {where} GROUP BY {self._var}")
        df[self._var] = df[self._var].apply(parse_date_to_std)
        return df

    def to_original(self, input_date: str) -> str:
        if self._fmt is None:
            return f"DATE '{input_date}'"
        else:
            dt = datetime.strptime(input_date, DATE_FMT)
            return f"'{dt.strftime(self._fmt)}'"

    def to_standard(self, original_date: str) -> str:
        if self._fmt is None:
            return original_date
        else:
            dt = datetime.strptime(original_date, self._fmt)
            return dt.strftime(DATE_FMT)

    def merge_where(self, start_dt, end_dt, where_clause):
        where_clause = [] if is_missing(where_clause) else [where_clause]
        if not is_missing(end_dt):
            where_clause.insert(0, f'{self._var} <= {self.to_original(end_dt)}')
        if not is_missing(start_dt):
            where_clause.insert(0, f'{self._var} >= {self.to_original(start_dt)}')
        return ' AND '.join(where_clause)

    def to_json(self):
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, json_str: str):
        return cls(**json.loads(json_str))


#>>> Setup logger to output folder <<<#
def add_logger(folder, name='events'):
    os.makedirs(folder, exist_ok=True)
    logger.configure( handlers=[ {"sink": sys.stderr, "level": "INFO"} ])
    if os.path.exists(fpath := os.path.join(folder, f'{name}.log')):
        os.remove(fpath)
    logger.add(fpath, level='INFO', format='{time:YY-MM-DD HH:mm:ss} | {level} | {message}', mode='w')

#>>> Return value of each environment variable name passed <<<#
def get_env(*args):
    for env_name in args:
        yield os.environ.get(env_name)

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
def load_config(config_file):
    with open(config_file, "rb") as f:
        config_dict = tomllib.load(f)
    return attridict(config_dict)


#>>> Parse Excel date input <<<#
def parse_excel_date(date_input, fmt: str = "%Y-%m-%d") -> str:
    try:
        if isinstance(date_input, (int, float)):
            dt = datetime(1899, 12, 30) + timedelta(days=date_input)
        else:
            dt = date_parser.parse(str(date_input))
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
    df["end_dt"]   = df["end_dt"].apply(parse_excel_date)
    df['pcds_var'] = df['pcds_var'].str.upper()
    df['aws_var']  = df['aws_var'].str.lower()

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


#>>> Get ISO week dates <<<#
def get_iso_week_dates(year, week):
    import datetime as dt
    jan01, dec31 = dt.datetime(year, 1, 1), dt.datetime(year, 12, 31)
    first_day = jan01 - dt.timedelta(days=jan01.weekday())
    start = first_day + dt.timedelta(weeks=week - 1)
    end = start + dt.timedelta(days=6)
    start, end = max(start, jan01), min(end, dec31)
    return start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')

