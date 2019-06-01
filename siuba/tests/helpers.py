from sqlalchemy import create_engine, types
from siuba.sql import LazyTbl, collect
from siuba.dply.verbs import ungroup
from pandas.testing import assert_frame_equal
import pandas as pd
import os
import numpy as np

def data_frame(**kwargs):
    fixed = {k: [v] if not np.ndim(v) else v for k,v in kwargs.items()}
    return pd.DataFrame(fixed)

BACKEND_CONFIG = {
        "postgresql": {
            "dialect": "postgresql",
            "dbname": ["SB_TEST_PGDATABASE", "postgres"],
            "port": ["SB_TEST_PGPORT", "5433"],
            "user": ["SB_TEST_PGUSER", "postgres"],
            "password": ["SB_TEST_PGPASSWORD", ""],
            "host": ["SB_TEST_PGHOST", "localhost"],
            },
        "sqlite": {
            "dialect": "sqlite",
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

    def __repr__(self):
        return "{0}({1})".format(self.__class__.__name__, repr(self.name))


class SqlBackend(Backend):
    table_name_indx = 0
    sa_conn_fmt = "{dialect}://{user}:{password}@{host}:{port}/{dbname}"

    def __init__(self, name):
        cnfg = BACKEND_CONFIG[name]
        params = {k: os.environ.get(*v) if isinstance(v, (list)) else v for k,v in cnfg.items()}

        self.name = name
        self.engine = create_engine(self.sa_conn_fmt.format(**params))

    def dispose(self):
        self.engine.dispose()

    @classmethod
    def unique_table_name(cls):
        cls.table_name_indx += 1
        return "siuba_{0:03d}".format(cls.table_name_indx)

    def load_df(self, df = None, **kwargs):
        df = super().load_df(df, **kwargs)
        return copy_to_sql(df, self.unique_table_name(), self.engine)


def assert_frame_sort_equal(a, b):
    """Tests that DataFrames are equal, even if rows are in different order"""
    df_a = ungroup(a)
    df_b = ungroup(b)
    sorted_a = df_a.sort_values(by = df_a.columns.tolist()).reset_index(drop = True)
    sorted_b = df_b.sort_values(by = df_b.columns.tolist()).reset_index(drop = True)

    assert_frame_equal(sorted_a, sorted_b)

def assert_equal_query(tbl, lazy_query, target):
    out = collect(lazy_query(tbl))
    assert_frame_sort_equal(out, target)


PREFIX_TO_TYPE = {
        # for datetime, need to convert to pandas datetime column
        #"dt": types.DateTime,
        "int": types.Integer,
        "float": types.Float,
        "str": types.String
        }

def auto_types(df):
    dtype = {}
    for k in df.columns:
        pref, *_ = k.split('_')
        if pref in PREFIX_TO_TYPE:
            dtype[k] = PREFIX_TO_TYPE[pref]
    return dtype


def copy_to_sql(df, name, engine):
    if isinstance(engine, str):
        engine = create_engine(engine)

    df.to_sql(name, engine, dtype = auto_types(df), index = False, if_exists = "replace")
    return LazyTbl(engine, name)


from functools import wraps
import pytest
        
def backend_notimpl(*names):
    def outer(f):
        @wraps(f)
        def wrapper(backend, *args, **kwargs):
            if backend.name in names:
                with pytest.raises(NotImplementedError):
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
