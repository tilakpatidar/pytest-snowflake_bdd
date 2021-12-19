pytest-snowflake_bdd
--------------------
.. image:: https://img.shields.io/pypi/v/pytest-snowflake_bdd.svg
    :target: https://pypi.org/project/pytest-snowflake_bdd
    :alt: PyPI version

.. image:: https://img.shields.io/pypi/pyversions/pytest-snowflake_bdd.svg
    :target: https://pypi.org/project/pytest-snowflake_bdd
    :alt: Python versions

Setup test data and run tests on snowflake in BDD style!

--------------------

Features
--------

Provides `pytest-bdd`_ step definitions for testing snow-sql scripts against a snowflake account.



Installation
------------

You can install "pytest-snowflake_bdd" via `pip`_.

    $ pip install pytest-snowflake-bdd


Usage
-----

This plugin relies on `pytest-bdd`_ to run bdd tests.

You can pass your snowflake account details using the cli arguments to pytest command.

.. code:: shell

    custom options:
      --snowflake_user=SNOWFLAKE_USER
                            snowflake user for test environment
      --snowflake_password=SNOWFLAKE_PASSWORD
                            snowflake password for test environment
      --snowflake_account=SNOWFLAKE_ACCOUNT
                            snowflake password for test environment
      --snowflake_role=SNOWFLAKE_ROLE
                            optional snowflake role for test environment
      --snowflake_warehouse=SNOWFLAKE_WAREHOUSE
                            optional snowflake warehouse for test environment


Below example illustrates the usage of step definitions provided by the plugin.

.. code:: gherkin

   Feature: ExampleFeature for snowflake testing

     Scenario: example_scenario
       Given a snowflake connection
       When a temporary called "SNOWFLAKE_LIQUIBASE.PUBLIC.DEPARTMENT" has
         | dept_id: INTEGER | dept_name: STRING      |
         | 1                | "Computer Science"     |
         | 2                | "Software Engineering" |
       When a temporary called "SNOWFLAKE_LIQUIBASE.PUBLIC.PEOPLE" has
         | people_id: INTEGER | name: STRING | dept_id: INTEGER |
         | 10                 | "tilak"      | 1                |
       Then a sql script "./sql/example.sql" runs and the result is
         | people_id: INTEGER | name: STRING | dept_id: INTEGER | dept_name: STRING  |
         | 10                 | "tilak"      | 1                | "Computer Science" |


- ``dept_id: INTEGER``. ``dept_id`` is the column name and ``INTEGER`` is the snowflake data type.
- The step ``a temporary table called "<fully_qualified_table_name>" has``

  Replaces the existing table with a `temporary` table. And adds data to the temporary table. This shadows the existing
  table in snowflake for the entire session. Any changes done to the temporary table does not reflect on the actual
  database. If the table does not exists creates a new temporary table.
- The step ``Then a sql script "<sql_script_path>" runs and the result is``
  This runs the sql script and compares the output with given dataframe.


Available Step definitions
---------------------------

**Creating a new snowflake session**


.. code:: gherkin

    Given a snowflake connection

**Setting up a temporary snowflake table for test**

* Replaces the existing table with a `temporary` table. And adds data to the temporary table. This shadows the existing
  table in snowflake for the entire session. Any changes done to the temporary table does not reflect on the actual
  database. If the table does not exists creates a new temporary table.

.. code:: gherkin

    When a temporary called "SNOWFLAKE_LIQUIBASE.PUBLIC.DEPARTMENT" has
     | dept_id: INTEGER | dept_name: STRING      |
     | 1                | "Computer Science"     |
     | 2                | "Software Engineering" |


**Setting up a snowflake table for test**

* Creates a normal table. Will fail if table already exists.

.. code:: gherkin

    When a called "SNOWFLAKE_LIQUIBASE.PUBLIC.DEPARTMENT" has
     | dept_id: INTEGER | dept_name: STRING      |
     | 1                | "Computer Science"     |
     | 2                | "Software Engineering" |

**Running a sql script and validating results**

.. code:: gherkin

    Then a sql script "./sql/example.sql" runs and the result is
      | people_id: INTEGER | name: STRING | dept_id: INTEGER | dept_name: STRING  |
      | 10                 | "tilak"      | 1                | "Computer Science" |

**Representing null in table data**

Use ``<null>``

.. code:: gherkin

      | people_id: INTEGER | name: STRING | dept_id: INTEGER | dept_name: STRING  |
      | 10                 | "tilak"      | 1                | <null> |


Understanding data-type mismatch errors
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For assertion of tables we are using pandas. Differences are shown
in-terms of pandas dataframe.

Below snowflake to pandas type table can help in understanding the
errors:

================== ===============
Snowflake datatype Pandas datatype
================== ===============
BIGINT             int64
BINARY             bytes
BOOLEAN            bool
CHAR               str
CHARACTER          str
DATE               object
DATETIME           object
DEC                object
DECIMAL            object
DOUBLE             float64
FIXED              object
FLOAT              float64
INT                int64
INTEGER            int64
NUMBER             object
REAL               float64
BYTEINT            int64
SMALLINT           int64
STRING             str
TEXT               str
TIME               object
TIMESTAMP          object
TINYINT            int64
VARBINARY          bytes
VARCHAR            str
================== ===============


Contributing
------------
Contributions are very welcome. Tests can be run with `tox`_, please ensure
the coverage at least stays the same before you submit a pull request.

License
-------

Distributed under the terms of the `MIT`_ license, "pytest-snowflake_bdd" is free and open source software


Issues
------

If you encounter any problems, please `file an issue`_ along with a detailed description.

.. _`MIT`: http://opensource.org/licenses/MIT
.. _`BSD-3`: http://opensource.org/licenses/BSD-3-Clause
.. _`GNU GPL v3.0`: http://www.gnu.org/licenses/gpl-3.0.txt
.. _`Apache Software License 2.0`: http://www.apache.org/licenses/LICENSE-2.0
.. _`file an issue`: https://github.com/tilakpatidar/pytest-snowflake_bdd/issues
.. _`pytest`: https://github.com/pytest-dev/pytest
.. _`tox`: https://tox.readthedocs.io/en/latest/
.. _`pip`: https://pypi.org/project/pip/
.. _`pytest-bdd`: https://pypi.org/project/pytest-bdd/
