import pandas as pd
import warnings

from siuba import *
from siuba.experimental.pivot import pivot_wider, pivot_longer
from siuba.siu import FunctionLookupBound
from siuba.ops import ALL_OPS

from pandas.core.groupby import SeriesGroupBy
from siuba.sql.utils import get_sql_classes

from tabulate import tabulate


SQL_DIALECTS = ["postgresql", "bigquery", "snowflake", "mysql", "sqlite", "duckdb"]


def is_object_dispatch(dialect_cls, f_op):
    return f_op.dispatch(dialect_cls) is f_op.dispatch(object)

    
def is_supported(dialect_cls, f_op):
    return not isinstance(f_op.dispatch(dialect_cls), FunctionLookupBound)


d_dialects = {name: get_sql_classes(name) for name in SQL_DIALECTS}
d_dialects["pandas"] = {"window": SeriesGroupBy, "aggregate": SeriesGroupBy}

df_ops = pd.DataFrame([{"op_name": k, "func": v} for k, v in ALL_OPS.items()])

def dialects_to_support(dialects, df_ops):
    df_dialects = (
        pd.DataFrame(d_dialects)
        >> _.reset_index()
        >> rename(op_context = "index")
    )


    tbl_support_tidy = (
        df_dialects
        >> pivot_longer(~_.op_context, names_to = "dialect_name", values_to = "dialect_class")
        >> _.merge(df_ops, how="cross")
        >> mutate(
            is_registered = _.apply(lambda ser: is_supported(ser["dialect_class"], ser["func"]), axis=1),     
            is_object = _.apply(lambda ser: is_object_dispatch(ser["dialect_class"], ser["func"]), axis=1),
            is_supported = _.is_registered & ~_.is_object
        )
        >> arrange(_.dialect_name, _.op_name, _.op_context)
    )

    return tbl_support_tidy


def get_op_support(simple=True):
    op_support = dialects_to_support(d_dialects, df_ops)
    if simple:
        return select(
            op_support,
            -_.is_registered,
            -_.is_object,
            -_.func,
            -_.dialect_class
        )

    return op_support
