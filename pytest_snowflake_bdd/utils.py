import numpy
import pandas as pd
from pandas._testing import assert_frame_equal
from snowflake.sqlalchemy.snowdialect import ischema_names as snowflake_to_sql_alchemy_types


def snowflake_type_to_dtype(tname):
    return numpy.dtype(snowflake_type_to_sqltype(tname).python_type)


def snowflake_type_to_sqltype(tname):
    return snowflake_to_sql_alchemy_types.get(tname.strip())()


def process_cells(cells):
    for cell in cells:
        cell = cell.strip()
        if cell[0] == '"' and cell[-1] == '"':
            yield cell[1:-1]
        elif cell == "{null}":
            yield None
        else:
            yield cell


def table_to_df(table):
    heading = table.split("\n")[0]
    col_names_with_types = list(
        map(lambda header: header.strip(), heading.split("|")[1:-1]))
    col_name_dtype_pairs = []
    col_name_sqltype_pairs = []
    for col_name_with_type in col_names_with_types:
        if len(col_name_with_type.split(':')) != 2:
            raise ValueError(
                f"You must specify name AND data type for columns like this 'my_field:string' at {col_name_with_type}")
        col_name, col_type = col_name_with_type.split(':')
        col_name_dtype_pairs.append(
            (col_name.strip(), snowflake_type_to_dtype(col_type.strip())))
        col_name_sqltype_pairs.append(
            (col_name.strip(), snowflake_type_to_sqltype(col_type.strip())))
    table_body = table.split("\n")[1:]
    table_body = list(filter(lambda x: "|" in x, table_body))
    rows = [list(process_cells(row.split("|")[1:-1])) for row in table_body]

    df = pd.DataFrame(
        rows,
        columns=[col_name_type_pair[0]
                 for col_name_type_pair in col_name_dtype_pairs],
    )
    df = df.astype(
        dict(col_name_dtype_pairs)
    )

    return df, dict(col_name_dtype_pairs), col_name_sqltype_pairs


def assert_frame_equal_with_sort(results, expected, key_columns, dtype_check=True):
    results_sorted = results.sort_values(
        by=key_columns).reset_index(drop=True).sort_index(axis=1)
    expected_sorted = expected.sort_values(
        by=key_columns).reset_index(drop=True).sort_index(axis=1)
    assert_frame_equal(results_sorted, expected_sorted,
                       check_index_type=False, check_dtype=dtype_check)
