# -*- coding: utf-8 -*-
"""Pytest plugin entry point. Used for any fixtures needed."""

import pandas as pd
import pytest
from pytest_bdd import then, when, parsers, given
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine, Column, MetaData, Table

from .utils import table_to_df, assert_frame_equal_with_sort


def pytest_addoption(parser):
    parser.addoption('--snowflake_user', required=True,
                     action='store', help='snowflake user for test environment')
    parser.addoption('--snowflake_password', required=True, action='store',
                     help='snowflake password for test environment')
    parser.addoption('--snowflake_account', required=True, action='store',
                     help='snowflake password for test environment')


@pytest.fixture
def snowflake_user(request):
    return request.config.getoption('--snowflake_user')


@pytest.fixture
def snowflake_password(request):
    return request.config.getoption('--snowflake_password')


@pytest.fixture
def snowflake_account(request):
    return request.config.getoption('--snowflake_account')


@pytest.fixture(scope="function")
def snowflake_sqlalchemy_conn(snowflake_user, snowflake_password, snowflake_account):
    engine = create_engine(URL(
        account=snowflake_account,
        user=snowflake_user,
        password=snowflake_password,
    ))
    connection = engine.connect()
    yield engine
    connection.close()
    engine.dispose()


@when(parsers.re(r'a temporary table called "(?P<table_name>.+)" has\s+(?P<table>[\s\S]+)'))
def temp_table_create_fixture(snowflake_sqlalchemy_conn, table_name, table):
    create_table_with_data(snowflake_sqlalchemy_conn, table,
                           table_name, temporary=True)


@when(parsers.re(r'a table called "(?P<table_name>.+)" has\s+(?P<table>[\s\S]+)'))
def table_create_fixture(snowflake_sqlalchemy_conn, table_name, table):
    create_table_with_data(snowflake_sqlalchemy_conn, table,
                           table_name, temporary=False)


def create_table_with_data(snowflake_sqlalchemy_conn, table, table_name, temporary):

    df, col_name_dtype_pairs, col_name_sqltype_pairs = table_to_df(table)

    assert len(table_name.split(".")) == 3, "Table name should be fully qualified ex: db_name.schema_name.table_name"

    db_name, schema_name, tb_name = table_name.split(".")
    cols = [
        Column(col_name, col_type)
        for col_name, col_type in col_name_sqltype_pairs
    ]

    pd.read_sql(f"USE DATABASE \"{db_name}\"", snowflake_sqlalchemy_conn)

    metadata = MetaData(bind=snowflake_sqlalchemy_conn)
    temp_table = Table(tb_name,
                       metadata,
                       *cols,
                       schema=schema_name,
                       prefixes=['TEMPORARY'] if temporary else None,
                       )
    temp_table.create(bind=snowflake_sqlalchemy_conn)

    df.to_sql(con=snowflake_sqlalchemy_conn,
              schema=schema_name,
              name=tb_name,
              if_exists="append",
              method="multi",
              index=False)


@then(parsers.re(r'a sql script "(?P<script_path>.+)" runs and the result is\n(?P<table>[\s\S]+)'))
def assert_table_contains(snowflake_sqlalchemy_conn, script_path, table):
    sql = open(script_path, "r").read()
    print("Executing query")
    print(sql)

    actual_df = pd.read_sql(sql, snowflake_sqlalchemy_conn)
    expected_df, _, _ = table_to_df(table)

    print("\n\n\nEXPECTED schema")
    print(expected_df.dtypes)
    print("\n\n\nACTUAL schema")
    print(actual_df.dtypes)

    assert_frame_equal_with_sort(actual_df, expected_df, key_columns=list(actual_df))


@given('a snowflake connection')
def t():
    pass
