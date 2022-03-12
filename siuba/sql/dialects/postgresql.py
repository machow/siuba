"""
Translations for the postgresql dialect of SQL.
"""

from ..translate import (
        win_agg, win_over, win_cumul, sql_scalar, sql_agg, win_absent,
        RankOver,
        wrap_annotate, annotate,
        extend_base,
        SqlTranslator,
        )

from .base import (
        SqlColumn, SqlColumnAgg
        )

import sqlalchemy.sql.sqltypes as sa_types
from sqlalchemy import sql
from sqlalchemy.sql import func as fn


# Custom dispatching in call trees ============================================

class PostgresqlColumn(SqlColumn): pass

class PostgresqlColumnAgg(SqlColumnAgg, PostgresqlColumn): pass

# Custom translations =========================================================

# datetime

@annotate(return_type="float")
def sql_is_quarter_end(_, col):
    last_day = fn.date_trunc("quarter", col) + sql.text("interval '3 month - 1 day'")
    return fn.date_trunc("day", col) == last_day



# other

def returns_float(func_names):
    # TODO: MC-NOTE - shift all translations to directly register
    # TODO: MC-NOTE - make an AliasAnnotated class or something, that signals
    #                 it is using another method, but w/ an updated annotation.
    from siuba.ops import ALL_OPS
    
    for name in func_names:
        generic = ALL_OPS[name]
        f_concrete = generic.dispatch(SqlColumn)
        f_annotated = wrap_annotate(f_concrete, result_type="float")
        generic.register(PostgresqlColumn, f_annotated)
    

def sql_log(_, col, base = None):
    if base is None:
        return sql.func.ln(col)
    return sql.func.log(col)

@annotate(result_type = "float")
def sql_round(_, col, n):
    return sql.func.round(col, n)

def sql_func_contains(_, col, pat, case = True, flags = 0, na = None, regex = True):
    # TODO: warn there differences in regex for python and sql?
    # TODO: validate pat is string?
    if not isinstance(pat, str):
        raise TypeError("pat argument must be a string")
    if flags != 0 or na is not None:
        raise NotImplementedError("flags and na options not supported")

    if not regex:
        case_col = col if case else col.lower()
        return case_col.contains(pat, autoescape = True)

    full_op = "~" if case else "~*"

    return col.op(full_op)(pat)

def sql_func_truediv(_, x, y):
    return sql.cast(x, sa_types.Float()) / y


extend_base(
        PostgresqlColumn,

        # TODO: remove log, not a pandas method
        #log = sql_log,

        # TODO: bring up to date (not pandas methods)
        #concat = lambda col: sql.func.concat(col),
        #cat = lambda col: sql.func.concat(col),
        #str_c = lambda col: sql.func.concat(col),

        # infix and infix methods ----

        div = sql_func_truediv,
        divide = sql_func_truediv,
        rdiv = lambda _, x,y: sql_func_truediv(_, y, x),

        __truediv__ = sql_func_truediv,
        truediv = sql_func_truediv,
        __rtruediv__ = lambda _, x, y: sql_func_truediv(_, y, x),


        round = sql_round,
        __round__ = sql_round,

        **{
            "str.contains": sql_func_contains,
        },
        **{
            "dt.is_quarter_end": sql_is_quarter_end
        }
        )

returns_float([
             "dt.day", "dt.dayofweek", "dt.dayofyear", "dt.days_in_month",
             "dt.daysinmonth", "dt.hour", "dt.minute", "dt.month",
             "dt.quarter", "dt.second", "dt.week", "dt.weekday",
             "dt.weekofyear", "dt.year"
             ])

extend_base(
        PostgresqlColumn,
        any = annotate(win_agg("bool_or"), input_type = "bool"),
        all = annotate(win_agg("bool_and"), input_type = "bool"),
        #lag = win_agg("lag"),
        std = win_agg("stddev_samp"),
        var = win_agg("var_samp"),

        # overrides ----

        # note that postgres does sum(bigint) -> numeric
        quantile = win_absent("percentile_cont"),
        size = win_agg("count"),     #TODO double check
        )

extend_base(
        PostgresqlColumnAgg,
        all = sql_agg("bool_and"),
        any = sql_agg("bool_or"),
        std = sql_agg("stddev_samp"),
        var = sql_agg("var_samp"),
        )


# translate(config, CallTreeLocal, PostgresqlColumn, _.a + _.b)
translator = SqlTranslator.from_mappings(
        PostgresqlColumn, PostgresqlColumnAgg
        )
