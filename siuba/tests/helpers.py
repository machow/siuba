from sqlalchemy import create_engine, types
from siuba.sql import LazyTbl, collect
from pandas.testing import assert_frame_equal

class DbConRegistry:
    table_name_indx = 0

    def __init__(self):
        self.connections = {}

    def register(self, name, engine):
        self.connections[name] = engine

    def remove(self, name):
        con = self.connections[name]
        con.close()
        del self.connections[name]

        return con

    @classmethod
    def unique_table_name(cls):
        cls.table_name_indx += 1
        return "siuba_{0:03d}".format(cls.table_name_indx)

    def load_df(self, df):
        out = []
        for k, engine in self.connections.items():
            lazy_tbl = copy_to_sql(df, self.unique_table_name(), engine)
            out.append(lazy_tbl)
        return out

def assert_frame_sort_equal(a, b):
    """Tests that DataFrames are equal, even if rows are in different order"""
    sorted_a = a.sort_values(by = a.columns.tolist()).reset_index(drop = True)
    sorted_b = b.sort_values(by = b.columns.tolist()).reset_index(drop = True)

    assert_frame_equal(sorted_a, sorted_b)

def assert_equal_query(tbls, lazy_query, target):
    for tbl in tbls:
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
    df.to_sql(name, engine, dtype = auto_types(df), index = False, if_exists = "replace")
    return LazyTbl(engine, name)
        
    
