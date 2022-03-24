# sqlvariant, allow defining 3 namespaces to override defaults
from ..translate import (
        SqlColumn, SqlColumnAgg, extend_base,
        SqlTranslator,
        sql_not_impl,
        annotate,
        wrap_annotate
        )

from .base import base_nowin
#from .postgresql import PostgresqlColumn as SqlColumn, PostgresqlColumnAgg as SqlColumnAgg
from . import _dt_generics as _dt

import sqlalchemy.sql.sqltypes as sa_types
from sqlalchemy import sql
from sqlalchemy.sql import func as fn

# Custom dispatching in call trees ============================================

# Note that aggs do not inherit SqlColumnAgg, since we disable aggregate functions
# for sqlite. Could add them in, as recent versions support a wide range of aggs.
class SqliteColumn(SqlColumn): pass
class SqliteColumnAgg(SqlColumnAgg, SqliteColumn): pass

# Note this is taken from the postgres dialect, but it seems that there are 2 key points
# compared to postgresql, which always returns a float
# * sqlite date parts are returned as floats
# * sqlite time parts are returned as integers
def returns_float(func_names):
    # TODO: MC-NOTE - shift all translations to directly register
    # TODO: MC-NOTE - make an AliasAnnotated class or something, that signals
    #                 it is using another method, but w/ an updated annotation.
    from siuba.ops import ALL_OPS
    
    for name in func_names:
        generic = ALL_OPS[name]
        f_concrete = generic.dispatch(SqlColumn)
        f_annotated = wrap_annotate(f_concrete, result_type="float")
        generic.register(SqliteColumn, f_annotated)

# detect first and last date (similar to the mysql dialect) ---
@annotate(return_type="float")
def sql_extract(name):
    if name == "quarter":
        # division in sqlite automatically rounds down
        # so for jan, 1 + 2 = 3, and 3 / 1 is Q1
        return lambda _, col: (fn.strftime("%m", col) + 2) / 3
    return lambda _, col: fn.extract(name, col)


@_dt.sql_is_last_day_of.register
def _sql_is_last_day_of(codata: SqliteColumn, col, period):
    valid_periods = {"month",  "year"}
    if period not in valid_periods:
        raise ValueError(f"Period must be one of {valid_periods}")

    incr = f"+1 {period}"

    target_date = fn.date(col, f'start of {period}', incr, "-1 day")
    return col == target_date

@_dt.sql_is_first_day_of.register
def _sql_is_first_day_of(codata: SqliteColumn, col, period):
    valid_periods = {"month",  "year"}
    if period not in valid_periods:
        raise ValueError(f"Period must be one of {valid_periods}")

    target_date = fn.date(col, f'start of {period}')
    return fn.date(col) == target_date


def sql_days_in_month(_, col):
    date_last_day = fn.date(col, 'start of month', '+1 month', '-1 day')
    return fn.strftime("%d", date_last_day).cast(sa_types.Integer())

    
@annotate(result_type = "float")
def sql_round(_, col, n):
    return sql.func.round(col, n)
    
def sql_func_truediv(_, x, y):
    return sql.cast(x, sa_types.Float()) / y

extend_base(
        SqliteColumn,
        div = sql_func_truediv,
        divide = sql_func_truediv,
        rdiv = lambda _, x,y: sql_func_truediv(_, y, x),

        __truediv__ = sql_func_truediv,
        truediv = sql_func_truediv,
        __rtruediv__ = lambda _, x, y: sql_func_truediv(_, y, x),

        round = sql_round,
        __round__ = sql_round,
        **{
            "dt.quarter": sql_extract("quarter"),
            "dt.is_quarter_start": sql_not_impl("TODO"),
            "dt.is_quarter_end": sql_not_impl("TODO"),
            "dt.days_in_month": sql_days_in_month,
            "dt.daysinmonth": sql_days_in_month,

        }
)

returns_float([
    "dt.dayofweek", 
    "dt.weekday",
])


extend_base(
        SqliteColumn,
        # TODO: should check sqlite version, since < 3.25 can't use windows
        quantile = sql_not_impl("sqlite does not support ordered set aggregates"),
        )

extend_base(
        SqliteColumnAgg,
        quantile = sql_not_impl("sqlite does not support ordered set aggregates"),
        )


translator = SqlTranslator.from_mappings(
        SqliteColumn, SqliteColumnAgg
        )
