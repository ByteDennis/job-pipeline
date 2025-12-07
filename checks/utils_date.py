"""Date parsing and vintage generation utilities."""
import pandas as pd
from datetime import datetime, timedelta
from dateutil import parser as date_parser
from typing import List, Tuple, Dict, Any

STD_DATE_FMT = '%Y-%m-%d'

#>>> Parse date string to standard format <<<#
def parse_date_to_std(date_val: Any) -> str:
    if pd.isna(date_val):
        return None
    if isinstance(date_val, (datetime, pd.Timestamp)):
        return date_val.strftime(STD_DATE_FMT)
    try:
        parsed = date_parser.parse(str(date_val))
        return parsed.strftime(STD_DATE_FMT)
    except:
        return None

#>>> Detect date format from sample values <<<#
def detect_date_format(date_vals: List[Any]) -> Tuple[str, str]:
    samples = [v for v in date_vals if v and not pd.isna(v)][:10]
    if not samples:
        return 'unknown', None

    first = samples[0]
    if isinstance(first, (datetime, pd.Timestamp)):
        return 'date', None

    str_val = str(first)
    if len(str_val) == 8 and str_val.isdigit():
        return 'string', '%Y%m%d'
    elif '-' in str_val:
        return 'string', '%Y-%m-%d'
    elif '/' in str_val:
        return 'string', '%Y/%m/%d'
    else:
        return 'string', None

#>>> Convert date value to SQL literal <<<#
def date_to_sql_literal(date_str: str, var_type: str, var_format: str, db_type: str) -> str:
    if var_type == 'date':
        return f"DATE '{date_str}'" if db_type == 'oracle' else f"DATE '{date_str}'"
    else:
        if var_format == '%Y%m%d':
            return f"'{date_str.replace('-', '')}'"
        else:
            return f"'{date_str}'"

#>>> Generate vintages from date range <<<#
def generate_vintages(min_date: str, max_date: str, partition_type: str,
                      date_var: str, var_type: str, var_format: str, db_type: str) -> List[Dict[str, str]]:
    start = datetime.strptime(min_date, STD_DATE_FMT)
    end = datetime.strptime(max_date, STD_DATE_FMT)
    vintages = []

    if partition_type == 'day':
        delta = timedelta(days=1)
        curr = start
        while curr <= end:
            date_str = curr.strftime(STD_DATE_FMT)
            sql_val = date_to_sql_literal(date_str, var_type, var_format, db_type)
            vintages.append({
                'vintage': f'D{curr.strftime("%Y%m%d")}',
                'start_date': date_str,
                'end_date': date_str,
                'where_clause': f"{date_var} = {sql_val}"
            })
            curr += delta

    elif partition_type == 'week':
        curr = start - timedelta(days=start.weekday())
        while curr <= end:
            week_end = curr + timedelta(days=6)
            if week_end > end:
                week_end = end
            start_str = curr.strftime(STD_DATE_FMT)
            end_str = week_end.strftime(STD_DATE_FMT)
            sql_start = date_to_sql_literal(start_str, var_type, var_format, db_type)
            sql_end = date_to_sql_literal(end_str, var_type, var_format, db_type)
            vintages.append({
                'vintage': f'W{curr.strftime("%Y")}W{curr.isocalendar()[1]:02d}',
                'start_date': start_str,
                'end_date': end_str,
                'where_clause': f"{date_var} >= {sql_start} AND {date_var} <= {sql_end}"
            })
            curr += timedelta(days=7)

    elif partition_type == 'month':
        curr = start.replace(day=1)
        while curr <= end:
            next_month = (curr.replace(day=28) + timedelta(days=4)).replace(day=1)
            month_end = next_month - timedelta(days=1)
            if month_end > end:
                month_end = end
            start_str = curr.strftime(STD_DATE_FMT)
            end_str = month_end.strftime(STD_DATE_FMT)
            sql_start = date_to_sql_literal(start_str, var_type, var_format, db_type)
            sql_end = date_to_sql_literal(end_str, var_type, var_format, db_type)
            vintages.append({
                'vintage': f'M{curr.strftime("%Y%m")}',
                'start_date': start_str,
                'end_date': end_str,
                'where_clause': f"{date_var} >= {sql_start} AND {date_var} <= {sql_end}"
            })
            curr = next_month

    return vintages
