import re
from datetime import datetime

import dateutil.parser
import numpy
import pandas as pd
from pandas._testing import assert_frame_equal
from snowflake.sqlalchemy.snowdialect import ischema_names as snowflake_to_sql_alchemy_types


def snowflake_type_to_dtype(tname):
    return numpy.dtype(snowflake_type_to_sqltype(tname).python_type)


def snowflake_type_to_sqltype(tname):
    return snowflake_to_sql_alchemy_types.get(tname.strip())()


def process_cells(col_name_sqltype_pairs, cells):
    for col_name_sqltype_pair, cell in zip(col_name_sqltype_pairs, cells):
        cell = cell.strip()
        sql_type = col_name_sqltype_pair[1]
        if cell[0] == '"' and cell[-1] == '"':
            value = cell[1:-1]
        elif cell == "{null}":
            value = None
        else:
            value = cell
        if value is not None:
            if sql_type.python_type is bool:
                value = value.lower() == "true"
            if sql_type.python_type is datetime:
                value = dateutil.parser.parse(value)
            else:
                value = sql_type.python_type(value)
        yield value


def table_to_df(table):
    heading = table.split("\n")[0]
    col_names_with_types = list(
        map(lambda header: header.strip(), heading.split("|")[1:-1]))
    col_name_sqltype_pairs = []
    for col_name_with_type in col_names_with_types:
        if len(col_name_with_type.split(':')) != 2:
            raise ValueError(
                f"You must specify name AND data type for columns like this 'my_field:string' at {col_name_with_type}")
        col_name, col_type = col_name_with_type.split(':')
        col_name_sqltype_pairs.append(
            (col_name.strip(), snowflake_type_to_sqltype(col_type.strip())))
    table_body = table.split("\n")[1:]
    table_body = list(filter(lambda x: "|" in x, table_body))
    rows = [list(process_cells(col_name_sqltype_pairs, row.split("|")[1:-1])) for row in table_body]

    df = pd.DataFrame(
        rows,
        columns=[col_name_sqltype_pair[0]
                 for col_name_sqltype_pair in col_name_sqltype_pairs],
    )

    return df, col_name_sqltype_pairs


def assert_frame_equal_with_sort(results, expected, key_columns, dtype_check=True):
    results_sorted = results.sort_values(
        by=key_columns).reset_index(drop=True).sort_index(axis=1)
    expected_sorted = expected.sort_values(
        by=key_columns).reset_index(drop=True).sort_index(axis=1)
    assert_frame_equal(results_sorted, expected_sorted,
                       check_index_type=False, check_dtype=False)


def stub_sql_functions(sql, current_timestamp, current_time):
    if current_timestamp is not None:
        sql = re.sub(r"(current_timestamp|localtimestamp|getdate|systimestamp|sysdate)\s*\(\s*\)",
                     f"CAST ('{current_timestamp}' AS TIMESTAMP)", sql, flags=re.IGNORECASE)
    if current_time is not None:
        sql = re.sub(r"(current_time|localtime)\s*\(\s*\)",
                     f"CAST ('{current_time}' AS TIME)", sql, flags=re.IGNORECASE)
    return sql
