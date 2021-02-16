# sqlvariant, allow defining 3 namespaces to override defaults
from ..translate import (
        SqlColumn, SqlColumnAgg, SqlTranslations, 
        win_agg, sql_scalar, sql_agg,
        annotate,
        create_sql_translators
        )

from .base import base_scalar, base_win, base_agg

import sqlalchemy.sql.sqltypes as sa_types
from sqlalchemy import sql


# Custom dispatching in call trees ============================================

class PostgresqlColumn(SqlColumn): pass

class PostgresqlColumnAgg(SqlColumnAgg, PostgresqlColumn): pass

# Custom translations =========================================================

def returns_float(ns, func_names):
    return {k: annotate(ns[k], return_type = "float") for k in func_names}

def sql_log(col, base = None):
    if base is None:
        return sql.func.ln(col)
    return sql.func.log(col)

def sql_round(col, n):
    return sql.func.round(sql.cast(col, sa_types.Numeric()), n)

def sql_func_contains(col, pat, case = True, flags = 0, na = None, regex = True):
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

scalar = SqlTranslations(
        base_scalar,
        log = sql_log,
        round = sql_round,
        #year = lambda col: sql.func.extract('year', sql.cast(col, sql.sqltypes.Date)),
        concat = lambda col: sql.func.concat(col),
        cat = lambda col: sql.func.concat(col),
        str_c = lambda col: sql.func.concat(col),
        __floordiv__ = lambda x, y: sql.cast(x / y, sa_types.Integer()),
        **{
            "str.contains": sql_func_contains,
        },
        **returns_float(base_scalar, [
             "dt.day", "dt.dayofweek", "dt.dayofyear", "dt.days_in_month",
             "dt.daysinmonth", "dt.hour", "dt.minute", "dt.month",
             "dt.quarter", "dt.second", "dt.week", "dt.weekday",
             "dt.weekofyear", "dt.year"
             ]),
        )

aggregate = SqlTranslations(
        base_agg,
        all = sql_agg("bool_and"),
        any = sql_agg("bool_or"),
        std = sql_agg("stddev_samp"),
        var = sql_agg("var_samp"),
        )

window = SqlTranslations(
        base_win,
        any = win_agg("bool_or"),
        all = win_agg("bool_and"),
        lag = win_agg("lag"),
        std = win_agg("stddev_samp"),
        var = win_agg("var_samp"),
        )

funcs = dict(scalar = scalar, aggregate = aggregate, window = window)

# translate(config, CallTreeLocal, PostgresqlColumn, _.a + _.b)
translator = create_sql_translators(
        scalar, aggregate, window,
        PostgresqlColumn, PostgresqlColumnAgg
        )
