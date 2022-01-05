# -*- coding: utf-8 -*-
import datetime
import re
from unittest import mock
from unittest.mock import Mock, ANY

import pandas
import pytest
from snowflake.sqlalchemy import DOUBLE
from sqlalchemy import INTEGER, VARCHAR, BOOLEAN, CHAR, BINARY, FLOAT, BIGINT, SMALLINT, DATE, DATETIME, TIME, TIMESTAMP


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
    assert list(utils.process_cells(
        [("name", VARCHAR()), ("age", VARCHAR()), ("dob", VARCHAR()), ("address", VARCHAR())],
        ["name", "12", "{null}", "   {null}"])
    ) == ["name", "12", None,
          None]


def test_table_to_df():
    from pytest_snowflake_bdd import utils
    table = """| id: INTEGER | name: STRING   | active:BOOLEAN |
               | 1           | "tilak"        | True           |
    """
    actual_df, col_name_sqltype_pairs = utils.table_to_df(
        table)

    expected_col_name_to_sqltype_pairs = str(
        [("id", INTEGER()), ("name", VARCHAR()), ("active", BOOLEAN()), ])

    expected_df = pandas.DataFrame([[1, "tilak", True]], columns=["id", "name", "active"])

    pandas.testing.assert_frame_equal(actual_df, expected_df)

    assert str(col_name_sqltype_pairs) == expected_col_name_to_sqltype_pairs

    table = """| id: INTEGER | name: STRING   | active:BOOLEAN   |
               | 1           | "tilak"        | true             |
               | 2           | "t"            | {null}           |
               | 3           | ""             | {null}           |
    """
    actual_df, col_name_sqltype_pairs = utils.table_to_df(
        table)
    expected_df = pandas.DataFrame([
        [1, "tilak", True],
        [2, "t", None],
        [3, "", None]
    ],
        columns=["id", "name", "active"])

    pandas.testing.assert_frame_equal(actual_df, expected_df)

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
        with mock.patch('pytest_snowflake_bdd.plugin.pd.DataFrame.to_sql',
                        return_value=mock.MagicMock()) as pandas_to_sql:
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


def test_temp_table_create_fixture():
    from pytest_snowflake_bdd.plugin import temp_table_create_fixture
    with mock.patch('pytest_snowflake_bdd.plugin.pd.read_sql', return_value=[]) as read_sql_df:
        with mock.patch('pytest_snowflake_bdd.plugin.pd.DataFrame.to_sql',
                        return_value=mock.MagicMock()) as pandas_to_sql:
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
        with mock.patch('pytest_snowflake_bdd.plugin.pd.DataFrame.to_sql',
                        return_value=mock.MagicMock()) as pandas_to_sql:
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
                                  columns=["id", "name", "active"])

    with mock.patch('pytest_snowflake_bdd.plugin._fetch_results', return_value=stubbed_df) as read_sql_df:
        tmp_file = (tmpdir / "test.sql").__str__()
        f = open(tmp_file, "w")
        f.write("select 1")
        f.close()

        snowflake_sqlalchemy_conn = Mock()

        table = """| id: INTEGER     | name: STRING   | active:BOOLEAN   |
                       | 1           | "tilak"        | true             |
                       | 2           | "t"            | {null}           |
                       | 3           | ""             | {null}           |
            """

        assert_table_contains(snowflake_sqlalchemy_conn, tmp_file, table, None, None)


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


def test_stub_sql_functions():
    def _remove_extra_spaces(s):
        return re.sub(r"\s+", " ", s)

    from pytest_snowflake_bdd import utils
    sql = """
    select   current_timestamp()   a, 
    CURRENT_TIMESTAMP() b, 
    current_timestamp(   ) c, 
    CURRENT_TIMESTAMP(   ) d,
    localtimestamp() f,
    getdate() g,
    systimestamp() h,
    sysdate() i,
    current_time() j,
    localtime() k
    """

    actual_sql = utils.stub_sql_functions(sql, current_timestamp="2022-01-05 04:12:17", current_time="04:12:17")

    assert _remove_extra_spaces(actual_sql) == _remove_extra_spaces("""
        select   CAST ('2022-01-05 04:12:17' AS TIMESTAMP)   a,
    CAST ('2022-01-05 04:12:17' AS TIMESTAMP) b,
    CAST ('2022-01-05 04:12:17' AS TIMESTAMP) c,
    CAST ('2022-01-05 04:12:17' AS TIMESTAMP) d,
    CAST ('2022-01-05 04:12:17' AS TIMESTAMP) f,
    CAST ('2022-01-05 04:12:17' AS TIMESTAMP) g,
    CAST ('2022-01-05 04:12:17' AS TIMESTAMP) h,
    CAST ('2022-01-05 04:12:17' AS TIMESTAMP) i,
    CAST ('04:12:17' AS TIME) j,
    CAST ('04:12:17' AS TIME) k
    """)

    assert utils.stub_sql_functions("select 1", current_timestamp=None, current_time=None) == "select 1"


def test_table_to_df_string_types():
    from pytest_snowflake_bdd import utils
    table = """| a: CHAR | b: CHARACTER | c: STRING | d: TEXT | e: BINARY | f: VARBINARY |
               | t       | t            | t         | t       | t         | t            |
    """
    actual_df, col_name_sqltype_pairs = utils.table_to_df(
        table)

    expected_col_name_to_sqltype_pairs = str(
        [("a", CHAR()), ("b", CHAR()), ("c", VARCHAR()), ("d", VARCHAR()), ("e", BINARY()), ("f", BINARY()) ])

    expected_df = pandas.DataFrame([["t", "t", "t", "t", b"t", b"t"]], columns=["a", "b", "c", "d", "e", "f"])

    pandas.testing.assert_frame_equal(actual_df, expected_df)

    assert str(col_name_sqltype_pairs) == expected_col_name_to_sqltype_pairs


def test_table_to_df_numeric_types():
    from pytest_snowflake_bdd import utils
    table = """| a: FLOAT | b: DOUBLE | c: INT | d: INTEGER | e: BIGINT | f: SMALLINT | g: TINYINT | h: BYTEINT |
               | 1.0      | 1.0       | 1      | 1          | 1         | 1           | 1          | 1          |
    """
    actual_df, col_name_sqltype_pairs = utils.table_to_df(
        table)

    expected_col_name_to_sqltype_pairs = str(
        [("a", FLOAT()), ("b", DOUBLE()), ("c", INTEGER()), ("d", INTEGER()), ("e", BIGINT()), ("f", SMALLINT()),
         ("g", SMALLINT()), ("h", SMALLINT())])

    expected_df = pandas.DataFrame([[1.0, 1.0, 1, 1, 1, 1, 1, 1]], columns=["a", "b", "c", "d", "e", "f", "g", "h"])

    pandas.testing.assert_frame_equal(actual_df, expected_df)

    assert str(col_name_sqltype_pairs) == expected_col_name_to_sqltype_pairs


def test_table_to_df_datetime_types():
    from pytest_snowflake_bdd import utils
    table = """| a: DATE    | b: DATETIME         | c: TIME  | d: TIMESTAMP        |
               | 2021-05-05 | 2021-05-05 01:35:00 | 01:35:00 | 2021-05-05 01:35:00 |
    """
    actual_df, col_name_sqltype_pairs = utils.table_to_df(
        table)

    expected_col_name_to_sqltype_pairs = str(
        [("a", DATE()), ("b", DATETIME()), ("c", TIME()), ("d", TIMESTAMP())])

    expected_df = pandas.DataFrame([[datetime.date(2021, 5, 5), datetime.datetime(2021, 5, 5, 1, 35, 0),
                                     datetime.time(1, 35, 0), datetime.datetime(2021, 5, 5, 1, 35, 0)]],
                                   columns=["a", "b", "c", "d"]).astype(
        {
            "a": "datetime64",
            "b": "datetime64",
            "c": "object",
            "d": "datetime64",
        }
    )

    pandas.testing.assert_frame_equal(actual_df, expected_df)

    assert str(col_name_sqltype_pairs) == expected_col_name_to_sqltype_pairs

