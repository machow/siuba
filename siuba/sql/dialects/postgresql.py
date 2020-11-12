# sqlvariant, allow defining 3 namespaces to override defaults
from ..translate import (
        SqlColumn, SqlColumnAgg,
        base_scalar, base_agg, base_win, SqlTranslator, 
        win_agg, sql_scalar, sql_agg
        )
import sqlalchemy.sql.sqltypes as sa_types
from sqlalchemy import sql


# Custom dispatching in call trees ============================================

class PostgresqlColumn(SqlColumn): pass

class PostgresqlColumnAgg(SqlColumnAgg, PostgresqlColumn): pass

# Custom translations =========================================================


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

scalar = SqlTranslator(
        base_scalar,
        log = sql_log,
        round = sql_round,
        contains = sql_func_contains,
        #year = lambda col: sql.func.extract('year', sql.cast(col, sql.sqltypes.Date)),
        concat = sql.func.concat,
        cat = sql.func.concat,
        str_c = sql.func.concat,
        __floordiv__ = lambda x, y: sql.cast(x / y, sa_types.Integer())
        )

aggregate = SqlTranslator(
        base_agg,
        all = sql_agg("bool_and"),
        any = sql_agg("bool_or"),
        std = sql_agg("stddev_samp"),
        var = sql_agg("var_samp"),
        )

window = SqlTranslator(
        base_win,
        any = win_agg("bool_or"),
        all = win_agg("bool_and"),
        lag = win_agg("lag"),
        var = win_agg("var_samp"),
        )

funcs = dict(scalar = scalar, aggregate = aggregate, window = window)
