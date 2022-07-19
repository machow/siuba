import importlib

try:
    # once we drop sqlalchemy 1.2, can use create_mock_engine function
    from sqlalchemy.engine.mock import MockConnection
except ImportError:
    from sqlalchemy.engine.strategies import MockEngineStrategy
    MockConnection = MockEngineStrategy.MockConnection


def get_dialect_translator(name):
    mod = importlib.import_module('siuba.sql.dialects.{}'.format(name))
    return mod.translator

def get_dialect_funcs(name):
    #dialect = engine.dialect.name
    mod = importlib.import_module('siuba.sql.dialects.{}'.format(name))
    return mod.funcs

def get_sql_classes(name):
    mod = importlib.import_module('siuba.sql.dialects.{}'.format(name))
    win_name = name.title() + "Column"
    agg_name = name.title() + "ColumnAgg"
    return {
            'window': getattr(mod, win_name),
            'aggregate': getattr(mod, agg_name)
            }


def mock_sqlalchemy_engine(dialect):
    """
    Create a sqlalchemy.engine.Engine without it connecting to a database.

    Examples
    --------

    ::
        from siuba.sql import LazyTbl
        from siuba import _, mutate, show_query

        engine = mock_sqlalchemy_engine('postgresql')
        tbl = LazyTbl(engine, 'some_table', ['x'])

        query = mutate(tbl, y = _.x + _.x)
        show_query(query)

    """

    from sqlalchemy.engine import Engine
    from sqlalchemy.dialects import registry
    from types import ModuleType

    #  TODO: can be removed once v1.3.18 support dropped
    from sqlalchemy.engine.url import URL


    dialect_cls = registry.load(dialect)

    # there is probably a better way to do this, but for some reason duckdb
    # returns a module, rather than the dialect class itself. By convention,
    # dialect modules expose a variable named dialect, so we grab that.
    if isinstance(dialect_cls, ModuleType):
        dialect_cls = dialect_cls.dialect
    
    conn = MockConnection(dialect_cls(), lambda *args, **kwargs: None)

    # set a url on it, so that LazyTbl can read the backend name.
    if is_sqla_12() or is_sqla_13():
        url = URL(drivername=dialect)
    else:
        url = URL.create(drivername=dialect)

    conn.url = url

    return conn


# Temporary fix for pandas bug (https://github.com/pandas-dev/pandas/issues/35484)
from pandas.io import sql as _pd_sql

class _FixedSqlDatabase(_pd_sql.SQLDatabase):
    def execute(self, *args, **kwargs):
        return self.connectable.execute(*args, **kwargs)


# Detect duckdb for temporary workarounds -------------------------------------

def _is_dialect_duckdb(engine):
    return engine.url.get_backend_name() == "duckdb"

# Backwards compatibility for sqlalchemy 1.3 ----------------------------------

import re
import sqlalchemy

RE_VERSION=r"(?P<major>\d+)\.(?P<minor>\d+).(?P<patch>\d+)"  
SQLA_VERSION=tuple(map(int, re.match(RE_VERSION, sqlalchemy.__version__).groups()))

def is_sqla_12():
    return SQLA_VERSION[:-1] == (1, 2)

def is_sqla_13():
    return SQLA_VERSION[:-1] == (1, 3)


def _sql_select(columns, *args, **kwargs):
    from sqlalchemy import sql
    if is_sqla_12() or is_sqla_13():
        # use old syntax, where columns are passed as a list
        return sql.select(columns, *args, **kwargs)

    return sql.select(*columns, *args, **kwargs)


def _sql_column_collection(data, columns):
    from sqlalchemy.sql.base import ColumnCollection, ImmutableColumnCollection

    if is_sqla_12() or is_sqla_13():
        return ImmutableColumnCollection(data, columns)

    return ColumnCollection(list(data.items())).as_immutable()


def _sql_add_columns(select, columns):
    if is_sqla_12() or is_sqla_13():
        for column in columns:
            select = select.column(column)
        return select

    return select.add_columns(*columns)


def _sql_with_only_columns(select, columns):
    if is_sqla_12() or is_sqla_13():
        return select.with_only_columns(columns)

    return select.with_only_columns(*columns)
