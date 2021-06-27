import sqlalchemy as sqla

from siuba.sql import LazyTbl
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
            "port": ["SB_TEST_PGPORT", "5432"],
            "user": ["SB_TEST_PGUSER", "postgres"],
            "password": ["SB_TEST_PGPASSWORD", ""],
            "host": ["SB_TEST_PGHOST", "localhost"],
            },
        "bigquery": {
            "dialect": "bigquery",
            # bigquery uses dbname for dataset
            "dbname": ["SB_TEST_BQDATABASE", "ci"],
            "port": "",
            "user": "",
            "password": "",
            "host": ["SB_TEST_BQPROJECT", "siuba-tests"],
            },
        "mysql": {
            "dialect": "mysql+pymysql",
            "dbname": "public",
            "port": 3306,
            "user": "root",
            "password": "",
            "host": "127.0.0.1",
            },
        "sqlite": {
            "dialect": "sqlite",
            "driver": "",
            "dbname": ":memory:",
            "port": "0",
            "user": "",
            "password": "",
            "host": ""
            }
        }

class Backend:
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

    def __repr__(self):
        return "{0}({1})".format(self.__class__.__name__, repr(self.name))

class PandasBackend(Backend):
    pass

class SqlBackend(Backend):
    table_name_indx = 0

    # if there is a :, sqlalchemy tries to parse the port number.
    # since backends like bigquery do not specify a port, we'll insert it
    # later on the port value passed in.
    sa_conn_fmt = "{dialect}://{user}:{password}@{host}{port}/{dbname}"

    def __init__(self, name):
        cnfg = BACKEND_CONFIG[name]
        params = {k: os.environ.get(*v) if isinstance(v, (list)) else v for k,v in cnfg.items()}

        if params["port"]:
            params["port"] = ":%s" % params["port"]

        self.name = name
        self.engine = sqla.create_engine(self.sa_conn_fmt.format(**params))
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

        return copy_to_sql(df, self.unique_table_name(), self.engine)

    def load_cached_df(self, df):
        import hashlib
        from pandas import util
        hash_arr = util.hash_pandas_object(df, index=True).values
        hashed = hashlib.sha256(hash_arr).hexdigest()

        if hashed in self.cache:
            return self.cache[hashed]
        
        res = self.cache[hashed] = self.load_df(df)

        return res


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

def assert_frame_sort_equal(a, b, **kwargs):
    """Tests that DataFrames are equal, even if rows are in different order"""
    df_a = ungroup(a)
    df_b = ungroup(b)
    sorted_a = robust_multiple_sort(df_a, list(df_a.columns)).reset_index(drop = True)
    sorted_b = robust_multiple_sort(df_b, list(df_b.columns)).reset_index(drop = True)

    assert_frame_equal(sorted_a, sorted_b, **kwargs)

def assert_equal_query(tbl, lazy_query, target, **kwargs):
    from pandas.core.groupby import DataFrameGroupBy

    out = collect(lazy_query(tbl))

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

    bool_cols = [k for k, v in df.iteritems() if v.dtype.kind == "b"]
    columns = [sqla.Column(name, sqla.types.Boolean) for name in bool_cols]

    # TODO: clean up dialect specific work (if it grows out of control)
    if engine.dialect.name == "bigquery" and df.isna().any().any():
        # TODO: to_gbq makes all datetimes UTC, so does not round trip well, 
        # but is able to handle None -> NULL....
        project_id = engine.url.host
        qual_name = f"{engine.url.database}.{name}"
        df.to_gbq(qual_name, project_id, if_exists="replace") 

    else:
        df.to_sql(name, engine, index = False, if_exists = "replace")

    # manually create table, so we can be explicit about boolean columns.
    # this is necessary because MySQL reflection reports them as TinyInts,
    # which mostly works, but returns ints from the query
    table = sqla.Table(
            name,
            sqla.MetaData(bind = engine),
            *columns,
            autoload_with = engine
            )
    
    return LazyTbl(engine, table)


from functools import wraps
import pytest
        
def backend_notimpl(*names):
    def outer(f):
        @wraps(f)
        def wrapper(backend, *args, **kwargs):
            if backend.name in names:
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
