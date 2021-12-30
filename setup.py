#!/usr/bin/env python
# -*- coding: utf-8 -*-

import codecs
import os

from setuptools import setup


def read(fname):
    file_path = os.path.join(os.path.dirname(__file__), fname)
    return codecs.open(file_path, encoding='utf-8').read()


gh_run_number = os.environ.get("BUILD_NUMBER", None)
build_number = None if gh_run_number is None or gh_run_number == "" else gh_run_number

version = '0.1.2'

setup(
    name='pytest-snowflake_bdd',
    version=f"{version}-{build_number}" if build_number else version,
    author='Tilak Patidar',
    author_email='tilakpatidar@gmail.com',
    maintainer='Tilak Patidar',
    maintainer_email='tilakpatidar@gmail.com',
    license='MIT',
    url='https://github.com/tilakpatidar/pytest-snowflake_bdd',
    description='Setup test data and run tests on snowflake in BDD style!',
    long_description=read('README.rst'),
    py_modules=['pytest_snowflake_bdd'],
    python_requires='>=3.6.7',
    install_requires=['pytest>=6.2.0', 'pytest-bdd>=3.2.1', 'snowflake-sqlalchemy>=1.3.2', 'SQLAlchemy>=1.4.27', \
                      'pandas>=0.25.3'],
    tests_require=[
      'tox',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Framework :: Pytest',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Testing',
        'Topic :: Database',
        'Topic :: Software Development :: Testing :: BDD',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Framework :: Pytest',
        'Operating System :: OS Independent',
        'License :: OSI Approved :: MIT License',
    ],
    packages=["pytest_snowflake_bdd"],
    entry_points={
        'pytest11': [
            'pytest-snowflake-bdd = pytest_snowflake_bdd.plugin',
        ],
    },
)
