import sqlalchemy as sqla
import uuid
import re

from siuba.sql import LazyTbl
from siuba.sql.utils import _is_dialect_duckdb
from siuba.dply.verbs import ungroup, collect
from siuba.siu import FunctionLookupError
from pandas.testing import assert_frame_equal
import pandas as pd
import os
import numpy as np

def data_frame(*args, _index = None, **kwargs):
    if len(args):
        raise NotImplementedError("all arguments to data_frame must be named")

    all_scalars = all(not np.ndim(v) for v in kwargs.values())

    if all_scalars:
        fixed = {k: [v] for k,v in kwargs.items()}
        return pd.DataFrame(fixed, index = _index)
    
    return pd.DataFrame(kwargs, index = _index)

BACKEND_CONFIG = {
        "postgresql": {
            "dialect": "postgresql",
            "driver": "",
            "dbname": ["SB_TEST_PGDATABASE", "postgres"],
            "port": ["SB_TEST_PGPORT", "5433"],
            "user": ["SB_TEST_PGUSER", "postgres"],
            "password": ["SB_TEST_PGPASSWORD", ""],
            "host": ["SB_TEST_PGHOST", "localhost"],
            "options": ""
            },
        "bigquery": {
            "dialect": "bigquery",
            # bigquery uses dbname for dataset
            "dbname": ["SB_TEST_BQDATABASE", "ci"],
            "port": "",
            "user": "",
            "password": "",
            "host": ["SB_TEST_BQPROJECT", "siuba-tests"],
            "options": ""
            },
        "snowflake": {
            "dialect": "snowflake",
            "driver": "",
            "dbname": ["SB_TEST_SNOWFLAKEDATABASE", "CI/TESTS"],
            "port": "",
            "user": ["SB_TEST_SNOWFLAKEUSER", "CI"],
            "password": ["SB_TEST_SNOWFLAKEPASSWORD", ""],
            "host": ["SB_TEST_SNOWFLAKEHOST", ""],
            "options": ["SB_TEST_SNOWFLAKEOPTIONS", "warehouse=COMPUTE_WH&role=CI_USER"]
            },
        "mysql": {
            "dialect": "mysql+pymysql",
            "dbname": "public",
            "port": ["SB_TEST_MYSQLPORT", 3306],
            "user": "root",
            "password": "",
            "host": "127.0.0.1",
            "options": ""
            },
        "sqlite": {
            "dialect": "sqlite",
            "driver": "",
            "dbname": ["SB_TEST_SQLITEDATABASE", ":memory:"],
            "port": "0",
            "user": "",
            "password": "",
            "host": "",
            "options": ""
            },
        "duckdb": {
            "dialect": "duckdb",
            "driver": "",
            "dbname": ":memory:",
            "port": "",
            "user": "",
            "password": "",
            "host": "",
            },
        }

class Backend:
    # TODO: use abstract base class
    kind = "pandas"

    def __init__(self, name):
        self.name = name

    def dispose(self):
        pass

    def load_df(self, df = None, **kwargs):
        if df is None and kwargs:
            df = pd.DataFrame(kwargs)
        elif df is not None and kwargs:
            raise ValueError("Cannot pass kwargs, and a DataFrame")

        return df

    def load_cached_df(self, df):
        return df

    def matches_test_qualifier(self, s):
        return self.name == s or self.kind == s

    def __repr__(self):
        return "{0}({1})".format(self.__class__.__name__, repr(self.name))

class PandasBackend(Backend):
    kind = "pandas"

class SqlBackend(Backend):
    kind = "sql"
    
    table_name_indx = 0

    # if there is a :, sqlalchemy tries to parse the port number.
    # since backends like bigquery do not specify a port, we'll insert it
    # later on the port value passed in.
    sa_conn_fmt = "{dialect}://{user}:{password}@{host}{port}/{dbname}?{options}"

    sa_conn_memory_fmt = "{dialect}:///{dbname}"

    def __init__(self, name):
        from urllib.parse import quote_plus

        cnfg = BACKEND_CONFIG[name]
        params = {k: os.environ.get(*v) if isinstance(v, (list)) else v for k,v in cnfg.items()}

        if params["port"]:
            params["port"] = ":%s" % params["port"]

        if params["password"]:
            params["password"] = quote_plus(params["password"])

        if params["dbname"] == ":memory:":
            sa_conn_uri = self.sa_conn_memory_fmt.format(**params)
        else:
            sa_conn_uri = self.sa_conn_fmt.format(**params)

        self.name = name
        self.engine = sqla.create_engine(sa_conn_uri)
        self.cache = {}

    def dispose(self):
        self.engine.dispose()

    @classmethod
    def unique_table_name(cls):
        cls.table_name_indx += 1
        return "siuba_{0:03d}".format(cls.table_name_indx)

    def load_df(self, df = None, **kwargs):
        df = super().load_df(df, **kwargs)

        table_name = self.unique_table_name()

        return copy_to_sql(df, table_name, self.engine)

    def load_cached_df(self, df):
        import hashlib
        from pandas import util
        hash_arr = util.hash_pandas_object(df, index=True).values
        hashed = hashlib.sha256(hash_arr).hexdigest()

        if hashed in self.cache:
            return self.cache[hashed]
        
        res = self.cache[hashed] = self.load_df(df)

        return res

class CloudBackend(SqlBackend):
    @classmethod
    def unique_table_name(cls):
        return "siuba_{}".format(uuid.uuid4())

class BigqueryBackend(CloudBackend):
    def load_df(self, df = None, **kwargs):
        df = super().load_df(df, **kwargs)

        # since we are using uuids, set table to expire after 1 day, so we can
        # easily inspect the tables, but also ensure cleanup
        self.engine.execute("""
            ALTER TABLE `{table_name}` 
            SET OPTIONS (
              expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
            )
            """.format(table_name=df.tbl.name))
        
        return df


def robust_multiple_sort(df, by):
    """Sort a DataFrame on multiple columns, slower but more reliable than df.sort_values

    Note: pandas errors when you sort by multiple columns, and one has unhashable objects.
          however, it can "sort" a single column with unhashable objects.

          e.g. df.sort_values(by = ['a', 'b']) may cause an error

    This implementation chains sort_values on single columns. In this case,
    pandas sorts a list based on its first entry ¯\_(ツ)_/¯.
    """

    from functools import reduce

    out = reduce(lambda data, col: data.sort_values(col), by, df)

    return out.reset_index(drop = True)

def assert_frame_sort_equal(a, b, *args, **kwargs):
    """Tests that DataFrames are equal, even if rows are in different order"""
    df_a = ungroup(a)
    df_b = ungroup(b)
    sorted_a = robust_multiple_sort(df_a, list(df_a.columns)).reset_index(drop = True)
    sorted_b = robust_multiple_sort(df_b, list(df_b.columns)).reset_index(drop = True)

    assert_frame_equal(sorted_a, sorted_b, **kwargs)

def assert_equal_query(tbl, lazy_query, target, **kwargs):
    from pandas.core.groupby import DataFrameGroupBy

    out = collect(lazy_query(tbl))

    if isinstance(tbl, LazyTbl) and _is_dialect_duckdb(tbl.source):
        # TODO: find a nice way to remove duckdb specific code from here
        # duckdb does not use pandas.DataFrame.to_sql method, which coerces
        # everything to 64 bit. So we need to coerce any results it returns
        # as 32 bit to 64 bit, to match to_sql.
        int_cols = out.select_dtypes('int32').columns
        flt_cols = out.select_dtypes('float32').columns
        out[int_cols] = out[int_cols].astype('int64')
        out[flt_cols] = out[flt_cols].astype('float64')

    if isinstance(tbl, (pd.DataFrame, DataFrameGroupBy)):
        df_a = ungroup(out).reset_index(drop = True)
        df_b = ungroup(target).reset_index(drop = True)
        assert_frame_equal(df_a, df_b, **kwargs)
    else:
        assert_frame_sort_equal(out, target, **kwargs)


#PREFIX_TO_TYPE = {
#        # for datetime, need to convert to pandas datetime column
#        #"dt": types.DateTime,
#        "int": types.Integer,
#        "float": types.Float,
#        "str": types.String,
#        }

#def auto_types(df):
#    dtype = {}
#    for k in df.columns:
#        pref, *_ = k.split('_')
#        if pref in PREFIX_TO_TYPE:
#            dtype[k] = PREFIX_TO_TYPE[pref]
#    return dtype


def copy_to_sql(df, name, engine):
    if isinstance(engine, str):
        engine = sqla.create_engine(engine)

    bool_cols = [k for k, v in df.items() if v.dtype.kind == "b"]
    columns = [sqla.Column(name, sqla.types.Boolean) for name in bool_cols]


    # Create table in database ------------------------------------------------

    # TODO: clean up dialect specific work (if it grows out of control)
    if engine.dialect.name == "bigquery" and df.isna().any().any():
        # TODO: to_gbq makes all datetimes UTC, so does not round trip well, 
        # but is able to handle None -> NULL....
        project_id = engine.url.host
        qual_name = f"{engine.url.database}.{name}"
        df.to_gbq(qual_name, project_id, if_exists="replace") 
    else:
        df.to_sql(name, engine, index = False, if_exists = "replace", method="multi")


    # LazyTbl creation --------------------------------------------------------

    if engine.dialect.name == "snowflake":
        # snowflake reflection is *incredibly* slow. it could take almost 10 seconds
        # to reflect column names / types. to work around this, we tell it the 
        # column names in advance
        autoload_with = None
        columns = [sqla.Column(name) for name in df]
    else:
        autoload_with = engine

    # manually create table, so we can be explicit about boolean columns.
    # this is necessary because MySQL reflection reports them as TinyInts,
    # which mostly works, but returns ints from the query
    table = sqla.Table(
            name,
            sqla.MetaData(),
            *columns,
            autoload_with = autoload_with
            )
    
    return LazyTbl(engine, table)


from functools import wraps
import pytest
        
def backend_notimpl(*names):
    def outer(f):
        @wraps(f)
        def wrapper(backend, *args, **kwargs):
            if any(map(backend.matches_test_qualifier, names)):
                with pytest.raises((NotImplementedError, FunctionLookupError)):
                    f(backend, *args, **kwargs)
                pytest.xfail("Not implemented!")
            else:
                return f(backend, *args, **kwargs)
        return wrapper
    return outer

def backend_sql(msg):
    # allow decorating without an extra call
    if callable(msg):
        return backend_sql(None)(msg)

    def outer(f):
        @wraps(f)
        def wrapper(backend, *args, **kwargs):
            if not isinstance(backend, SqlBackend):
                pytest.skip(msg)
            else:
                return f(backend, *args, **kwargs)
        return wrapper
    return outer

def backend_pandas(msg):
    # allow decorating without an extra call
    if callable(msg):
        return backend_pandas(None)(msg)

    def outer(f):
        @wraps(f)
        def wrapper(backend, *args, **kwargs):
            if not isinstance(backend, PandasBackend):
                pytest.skip(msg)
            else:
                return f(backend, *args, **kwargs)
        return wrapper
    return outer


# Versions --------------------------------------------------------------------

re_version=r"(?P<major>\d+)\.(?P<minor>\d+).(?P<patch>\d+)"  


sqla_version=tuple(map(int, re.match(re_version, sqla.__version__).groups()))
pd_version=tuple(map(int, re.match(re_version, pd.__version__).groups()))

