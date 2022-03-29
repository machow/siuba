# sqlvariant, allow defining 3 namespaces to override defaults
from ..translate import (
        SqlColumn, SqlColumnAgg, extend_base, win_agg,
        SqlTranslator,
        annotate, sql_scalar, sql_agg, win_cumul, sql_not_impl,
        AggOver, RankOver, Over,
        )

import sqlalchemy.sql.sqltypes as sa_types
from sqlalchemy import sql
from sqlalchemy.sql import func as fn
from . import _dt_generics as _dt

# Custom dispatching in call trees ============================================

class BigqueryColumn(SqlColumn): pass
class BigqueryColumnAgg(SqlColumnAgg, BigqueryColumn): pass

# Custom translations =========================================================

@annotate(result_type="float")
def sql_floordiv(_, x, y):
    return sql.func.floor(x / y)

# datetime ----

@_dt.date_trunc.register
def _date_trunc(_: BigqueryColumn, col, name):
    return fn.datetime_trunc(col, sql.text(name))


@_dt.sql_func_last_day_in_period.register
def sql_func_last_day_in_period(_: BigqueryColumn, col, period):
    return fn.last_day(col, sql.text(period))


@_dt.sql_is_last_day_of.register
def sql_is_last_day_of(codata: BigqueryColumn, col, period):
    last_day = sql_func_last_day_in_period(codata, col, period)
    return _date_trunc(codata, col, "DAY") == last_day


def sql_extract(field):
    return lambda _, col: fn.extract(field, col)


# string ----

def sql_str_replace(_, col, pat, repl, n=-1, case=None, flags=0, regex=True):
    if n != -1 or case is not None or flags != 0:
        raise NotImplementedError("only pat and repl arguments supported in sql")

    if regex:
        return fn.regexp_replace(col, pat, repl)

    return fn.replace(col, pat, repl)

def sql_str_contains(_, col, pat, case=None, flags=0, na=None, regex=True):
    if case is not None or flags != 0:
        raise NotImplementedError("only pat and repl arguments supported in sql")

    if regex:
        return fn.regexp_contains(col, pat)

    return col.contains(pat)

# error messages ----

QUANTILE_ERROR = ("taking the median or quantile with percentile_cont can only be done in a mutate, "
                 "since this function cannot be used as an aggregate")

# any / all ----

def sql_any(window = False):
    f_win = AggOver if window else lambda x: x
    
    @annotate(input_type="bool")
    def f(_, col):
        return f_win(fn.sum(fn.cast(col, sa_types.Integer()))) != 0

    return f

def sql_all(window = False):
    f_win = AggOver if window else lambda x: x
    
    @annotate(input_type="bool")
    def f(_, col):
        # similar to any, but uses (not cond) summed is 0
        return f_win(fn.sum(fn.cast(~col, sa_types.Integer()))) == 0

    return f

sql_median = lambda _, col: fn.percentile_cont(col, .5)

scalar = extend_base(
    BigqueryColumn,
    __floordiv__  = sql_floordiv,
    __rfloordiv__ = annotate(lambda _, x, y: sql_floordiv(y, x), result_type="float"),

    __mod__       = lambda _, x, y: sql.func.mod(x, y),
    mod           = lambda _, x, y: sql.func.mod(x, y),
    __rmod__      = lambda _, x, y: sql.func.mod(y, x),
    rmod          = lambda _, x, y: sql.func.mod(y, x),

    __round__     = annotate(sql_scalar("round"), result_type = "float"),
    round         = annotate(sql_scalar("round"), result_type = "float"),

    # date ----
    **{
      # bigquery has Sunday as 1, pandas wants Monday as 0
      "dt.dayofweek":        lambda _, col: fn.extract("DAYOFWEEK", col) - 2,
      "dt.dayofyear":        sql_extract("DAYOFYEAR"),
      "dt.month_name":       lambda _, col: fn.format_date("%B", col),
      "dt.week":             sql_extract("ISOWEEK"),
      "dt.weekday":          lambda _, col: fn.extract("DAYOFWEEK", col) - 2,
      "dt.weekofyear":       sql_extract("ISOWEEK")
    },
    **{
      "str.contains": sql_str_contains,
      "str.replace": sql_str_replace,
    }
    )

aggregate = extend_base(
    BigqueryColumnAgg,
    # NOTE: bigquery has an all() func, but it's not an aggregate
    any      = sql_any(),
    all      = sql_all(),
    count    = lambda _, col: fn.count(col),
    median   = sql_not_impl(QUANTILE_ERROR),
    nunique  = lambda _, col: fn.count(fn.distinct(col)),
    quantile = sql_not_impl(QUANTILE_ERROR),
    size     = lambda _, col: fn.count("*"),
    std       = sql_agg("stddev"),
    sum      = sql_agg("sum"),
    var      = sql_agg("variance"),
    )

window = extend_base(
    BigqueryColumn,
    any      = sql_any(window = True),
    all      = sql_all(window = True),
    count    = lambda _, col: AggOver(fn.count(col)),
    cumsum   = win_cumul("sum"),
    median   = lambda _, col: RankOver(fn.percentile_cont(col, .5)),
    nunique  = lambda _, col: AggOver(fn.count(fn.distinct(col))),
    quantile = lambda _, col, q: RankOver(fn.percentile_cont(col, q)),
    std       = win_agg("stddev"),
    sum      = win_agg("sum"),
    size     = lambda _, col: AggOver(fn.count("*")),
    var      = win_agg("variance"),
    )


translator = SqlTranslator.from_mappings(
        BigqueryColumn, BigqueryColumnAgg
        )
