# -*- coding: utf-8 -*-
from unittest import mock
from unittest.mock import Mock, ANY

import numpy
import pandas
import pytest
from numpy import dtype
from sqlalchemy import INTEGER, VARCHAR, BOOLEAN


def test_snowflake_cred_fixtures(testdir):
    testdir.makepyfile("""
        def test_sth(snowflake_user, snowflake_password, snowflake_account, snowflake_role, snowflake_warehouse):
            assert snowflake_user == "user"
            assert snowflake_password == "password"
            assert snowflake_account == "account"
            assert snowflake_role is None
            assert snowflake_warehouse is None
    """)

    result = testdir.runpytest(
        '--snowflake-user=user',
        '--snowflake-password=password',
        '--snowflake-account=account',
    )

    # fnmatch_lines does an assertion internally
    result.stdout.fnmatch_lines([
        '*1 passed*',
    ])

    # make sure that that we get a '0' exit code for the testsuite
    assert result.ret == 0


def test_snowflake_cred_fixtures_optional(testdir):
    testdir.makepyfile("""
        def test_sth(snowflake_user, snowflake_password, snowflake_account, snowflake_role, snowflake_warehouse):
            assert snowflake_user == "user"
            assert snowflake_password == "password"
            assert snowflake_account == "account"
            assert snowflake_role == "role"
            assert snowflake_warehouse == "warehouse"
    """)

    result = testdir.runpytest(
        '--snowflake-user=user',
        '--snowflake-password=password',
        '--snowflake-account=account',
        '--snowflake-role=role',
        '--snowflake-warehouse=warehouse',
    )

    # fnmatch_lines does an assertion internally
    result.stdout.fnmatch_lines([
        '*1 passed*',
    ])

    # make sure that that we get a '0' exit code for the testsuite
    assert result.ret == 0


def test_process_cells():
    from pytest_snowflake_bdd import utils
    assert list(utils.process_cells(["name", "12", "{null}", "   {null}"])) == ["name", "12", None,
                                                                                None]


def test_table_to_df():
    from pytest_snowflake_bdd import utils
    table = """| id: INTEGER | name: STRING   | active:BOOLEAN |
               | 1           | "tilak"        | 1           |
    """
    actual_df, col_name_dtype_pairs, col_name_sqltype_pairs = utils.table_to_df(
        table)

    expected_col_name_to_dtype_pairs = {
        "id": numpy.dtype("int64"),
        "name": numpy.dtype("str"),
        "active": numpy.dtype("bool"),
    }

    expected_col_name_to_sqltype_pairs = str(
        [("id", INTEGER()), ("name", VARCHAR()), ("active", BOOLEAN()), ])

    expected_df = pandas.DataFrame([[1, "tilak", True]], columns=["id", "name", "active"]) \
        .astype(expected_col_name_to_dtype_pairs)

    pandas.testing.assert_frame_equal(actual_df, expected_df)

    assert col_name_dtype_pairs == expected_col_name_to_dtype_pairs

    assert str(col_name_sqltype_pairs) == expected_col_name_to_sqltype_pairs

    table = """| id: INTEGER | name: STRING   | active:BOOLEAN   |
               | 1           | "tilak"        | 1                |
               | 2           | "t"            | {null}           |
               | 3           | ""             | {null}           |
    """
    actual_df, col_name_dtype_pairs, col_name_sqltype_pairs = utils.table_to_df(
        table)
    expected_df = pandas.DataFrame([
        [1, "tilak", True],
        [2, "t", None],
        [3, "", None]
    ],
        columns=["id", "name", "active"]).astype(expected_col_name_to_dtype_pairs)

    pandas.testing.assert_frame_equal(actual_df, expected_df)

    assert col_name_dtype_pairs == expected_col_name_to_dtype_pairs

    assert str(col_name_sqltype_pairs) == expected_col_name_to_sqltype_pairs


def test_create_table_with_data_without_fq_table_name():
    from pytest_snowflake_bdd.plugin import create_table_with_data
    snowflake_sqlalchemy_conn = Mock()

    table = """| id: INTEGER | name: STRING   | active:BOOLEAN   |
                   | 1           | "tilak"        | 1                |
                   | 2           | "t"            | {null}           |
                   | 3           | ""             | {null}           |
        """
    with pytest.raises(Exception) as execinfo:
        create_table_with_data(snowflake_sqlalchemy_conn, table, "my_schema.my_table", temporary=True)

    assert "Table name should be fully qualified" in str(execinfo.value)
    assert execinfo.type is AssertionError


def test_create_table_with_data():
    from pytest_snowflake_bdd.plugin import create_table_with_data
    with mock.patch('pytest_snowflake_bdd.plugin.pd.read_sql', return_value=[]) as read_sql_df:
        with mock.patch('pytest_snowflake_bdd.plugin.pd.DataFrame.to_sql', return_value=mock.MagicMock()) as pandas_to_sql:
            with mock.patch('pytest_snowflake_bdd.plugin.Table', return_value=mock.MagicMock()) as sqlalchemy_table:
                snowflake_sqlalchemy_conn = Mock()
                snowflake_sqlalchemy_conn.execute = Mock(return_value=None)

                table = """| id: INTEGER | name: STRING   | active:BOOLEAN   |
                               | 1           | "tilak"        | 1                |
                               | 2           | "t"            | {null}           |
                               | 3           | ""             | {null}           |
                    """

                create_table_with_data(snowflake_sqlalchemy_conn, table, "my_db.my_schema.my_table", temporary=True)

                read_sql_df.assert_called_with("USE DATABASE \"my_db\"", ANY)
                sqlalchemy_table.assert_called_with('my_table', ANY, ANY, ANY, ANY, schema='my_schema', prefixes=['TEMPORARY'])
                assert sqlalchemy_table.call_args[0][2].name == "id"
                assert sqlalchemy_table.call_args[0][2].type.__visit_name__ == "INTEGER"
                assert sqlalchemy_table.call_args[0][3].name == "name"
                assert sqlalchemy_table.call_args[0][3].type.__visit_name__ == "VARCHAR"
                assert sqlalchemy_table.call_args[0][4].name == "active"
                assert sqlalchemy_table.call_args[0][4].type.__visit_name__ == "BOOLEAN"
                pandas_to_sql.assert_called_with(con=ANY, schema='my_schema', name='my_table', if_exists='append', method='multi',
                                                 index=False)


def test_temp_table_create_fixture():
    from pytest_snowflake_bdd.plugin import temp_table_create_fixture
    with mock.patch('pytest_snowflake_bdd.plugin.pd.read_sql', return_value=[]) as read_sql_df:
        with mock.patch('pytest_snowflake_bdd.plugin.pd.DataFrame.to_sql', return_value=mock.MagicMock()) as pandas_to_sql:
            with mock.patch('pytest_snowflake_bdd.plugin.Table', return_value=mock.MagicMock()) as sqlalchemy_table:
                snowflake_sqlalchemy_conn = Mock()
                snowflake_sqlalchemy_conn.execute = Mock(return_value=None)

                table = """| id: INTEGER | name: STRING   | active:BOOLEAN   |
                               | 1           | "tilak"        | 1                |
                               | 2           | "t"            | {null}           |
                               | 3           | ""             | {null}           |
                    """

                temp_table_create_fixture(snowflake_sqlalchemy_conn, "my_db.my_schema.my_table", table)

                read_sql_df.assert_called_with("USE DATABASE \"my_db\"", ANY)
                sqlalchemy_table.assert_called_with('my_table', ANY, ANY, ANY, ANY, schema='my_schema',
                                                    prefixes=['TEMPORARY'])
                assert sqlalchemy_table.call_args[0][2].name == "id"
                assert sqlalchemy_table.call_args[0][2].type.__visit_name__ == "INTEGER"
                assert sqlalchemy_table.call_args[0][3].name == "name"
                assert sqlalchemy_table.call_args[0][3].type.__visit_name__ == "VARCHAR"
                assert sqlalchemy_table.call_args[0][4].name == "active"
                assert sqlalchemy_table.call_args[0][4].type.__visit_name__ == "BOOLEAN"
                pandas_to_sql.assert_called_with(con=ANY, schema='my_schema', name='my_table', if_exists='append',
                                                 method='multi',
                                                 index=False)


def test_table_create_fixture():
    from pytest_snowflake_bdd.plugin import table_create_fixture
    with mock.patch('pytest_snowflake_bdd.plugin.pd.read_sql', return_value=[]) as read_sql_df:
        with mock.patch('pytest_snowflake_bdd.plugin.pd.DataFrame.to_sql', return_value=mock.MagicMock()) as pandas_to_sql:
            with mock.patch('pytest_snowflake_bdd.plugin.Table', return_value=mock.MagicMock()) as sqlalchemy_table:
                snowflake_sqlalchemy_conn = Mock()
                snowflake_sqlalchemy_conn.execute = Mock(return_value=None)

                table = """| id: INTEGER | name: STRING   | active:BOOLEAN   |
                               | 1           | "tilak"        | 1                |
                               | 2           | "t"            | {null}           |
                               | 3           | ""             | {null}           |
                    """
                table_create_fixture(snowflake_sqlalchemy_conn, "my_db.my_schema.my_table", table)

                read_sql_df.assert_called_with("USE DATABASE \"my_db\"", ANY)
                sqlalchemy_table.assert_called_with('my_table', ANY, ANY, ANY, ANY, schema='my_schema', prefixes=None)
                assert sqlalchemy_table.call_args[0][2].name == "id"
                assert sqlalchemy_table.call_args[0][2].type.__visit_name__ == "INTEGER"
                assert sqlalchemy_table.call_args[0][3].name == "name"
                assert sqlalchemy_table.call_args[0][3].type.__visit_name__ == "VARCHAR"
                assert sqlalchemy_table.call_args[0][4].name == "active"
                assert sqlalchemy_table.call_args[0][4].type.__visit_name__ == "BOOLEAN"
                pandas_to_sql.assert_called_with(con=ANY, schema='my_schema', name='my_table', if_exists='append',
                                                 method='multi',
                                                 index=False)


def test_assert_table_contains(tmpdir):
    from pytest_snowflake_bdd.plugin import assert_table_contains
    stubbed_df = pandas.DataFrame([[1, "tilak", True], [2, "t", None], [3, "", None]],
                                  columns=["id", "name", "active"]) \
        .astype({'id': dtype('int64'), 'name': dtype('str'), 'active': dtype('bool')})
    with mock.patch('pytest_snowflake_bdd.plugin.pd.read_sql', return_value=stubbed_df) as read_sql_df:


        tmp_file = (tmpdir / "test.sql").__str__()
        f = open(tmp_file, "w")
        f.write("select 1")
        f.close()

        snowflake_sqlalchemy_conn = Mock()

        table = """| id: INTEGER | name: STRING   | active:BOOLEAN   |
                       | 1           | "tilak"        | 1                |
                       | 2           | "t"            | {null}           |
                       | 3           | ""             | {null}           |
            """

        assert_table_contains(snowflake_sqlalchemy_conn, tmp_file, table)


def test_snowflake_sqlalchemy_conn():
    from pytest_snowflake_bdd.plugin import _snowflake_sqlalchemy_conn
    with mock.patch('pytest_snowflake_bdd.plugin.create_engine', return_value=mock.MagicMock()) as create_engine_mock:
        next(_snowflake_sqlalchemy_conn("user", "password", "account", None, None))
        create_engine_mock.assert_called_with('snowflake://user:password@account/')


def test_snowflake_sqlalchemy_conn_with_optional_params():
    from pytest_snowflake_bdd.plugin import _snowflake_sqlalchemy_conn
    with mock.patch('pytest_snowflake_bdd.plugin.create_engine', return_value=mock.MagicMock()) as create_engine_mock:
        next(_snowflake_sqlalchemy_conn("user", "password", "account", "role", "warehouse"))
        create_engine_mock.assert_called_with('snowflake://user:password@account/?role=role&warehouse=warehouse')
