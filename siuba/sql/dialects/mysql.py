# sqlvariant, allow defining 3 namespaces to override defaults
from ..translate import (
        SqlColumn, SqlColumnAgg, extend_base, win_agg,
        SqlTranslator, sql_not_impl, win_absent
        )

from .base import base_scalar, base_agg, base_win

import sqlalchemy.sql.sqltypes as sa_types

from sqlalchemy import sql
from sqlalchemy.sql import func as fn

# Custom dispatching in call trees ============================================

class MysqlColumn(SqlColumn): pass
class MysqlColumnAgg(SqlColumnAgg, MysqlColumn): pass

def sql_str_strip(left = True, right = True):
    def f(_, col):
        # see https://stackoverflow.com/a/6858168/1144523
        lstrip = "^[[:space:]]+" if left else ""
        rstrip = "[[:space:]]+$" if right else ""
    
        or_op = "|" if lstrip and rstrip else ""
        regex = "(" + lstrip + or_op + rstrip + ")"
    
        return fn.regexp_replace(col, regex, "")

    return f

def sql_func_extract_dow_monday(_, col):
    # MYSQL: sunday starts, equals 1 (an int)
    # pandas: monday starts, equals 0 (also an int)

    raw_dow = fn.dayofweek(col)

    # monday is 2 in MYSQL, so use monday + 5 % 7
    return (raw_dow + 5) % 7

def sql_is_date_offset(period, is_start = True):

    # will check against one day in the past for is_start, v.v. otherwise
    fn_add = fn.date_sub if is_start else fn.date_add

    def f(_, col):
        get_period = getattr(fn, period)
        src_per = get_period(col)
        incr_per = get_period(fn_add(col, sql.text("INTERVAL 1 DAY")))

        return src_per != incr_per

    return f

def sql_func_truediv(_, x, y):
    return sql.cast(x, sa_types.Numeric()) / y

def sql_func_floordiv(_, x, y):
    return x.op("DIV")(y)

def sql_func_between(_, col, left, right, inclusive=True):
    if not inclusive:
        raise NotImplementedError("between must be inclusive")

    # TODO: should figure out how sqlalchemy prefers to set types, rather
    # than setting manually on this expression
    expr = col.between(left, right)
    expr.type = sa_types.Boolean()
    return expr

scalar = extend_base(
        MysqlColumn,
        base_scalar,

        # copied from postgres. MYSQL does true division over ints by default,
        # but it does not produce double precision.
        div = sql_func_truediv,
        divide = sql_func_truediv,
        rdiv = lambda codata, x,y: sql_func_truediv(codata, y, x),

        __truediv__ = sql_func_truediv,
        truediv = sql_func_truediv,
        __rtruediv__ = lambda codata, x, y: sql_func_truediv(codata, y, x),

        __floordiv__ = sql_func_floordiv,
        __rfloordiv__ = lambda codata, x, y: sql_func_floordiv(codata, y, x),

        between = sql_func_between,

        **{
          "str.lstrip": sql_str_strip(right = False),
          "str.rstrip": sql_str_strip(left = False),
          "str.strip": sql_str_strip(),
          "str.title": sql_not_impl()                       # see https://stackoverflow.com/q/12364086/1144523
        },
        **{
          # TODO: MC-NOTE -- can be reworked using extract
          "dt.dayofweek": sql_func_extract_dow_monday,
          "dt.dayofyear": lambda _, col: fn.dayofyear(col),
          "dt.days_in_month": lambda _, col: fn.dayofmonth(fn.last_day(col)),
          "dt.daysinmonth": lambda _, col: fn.dayofmonth(fn.last_day(col)),
          "dt.is_month_end": lambda _, col: col == fn.last_day(col),
          "dt.is_month_start": lambda _, col: fn.dayofmonth(col) == 1,
          "dt.is_quarter_start": sql_is_date_offset("QUARTER"),
          "dt.is_year_start": sql_is_date_offset("YEAR"),
          "dt.is_year_end": sql_is_date_offset("YEAR", is_start = False),
          # see https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_week
          "dt.week": lambda _, col: fn.week(col, 1),
          "dt.weekday": sql_func_extract_dow_monday,
          "dt.weekofyear": lambda _, col: fn.week(col, 1),
        }
        )

aggregate = extend_base(
        MysqlColumnAgg,
        base_agg,

        quantile = win_absent("percentile_cont"),
        )

window = extend_base(
        MysqlColumn,
        base_win,

        # TODO: analytic percentile_cont is supported in mariadb
        quantile = win_absent("percentile_cont"),
        #sd = win_agg("stddev")
        )

funcs = dict(scalar = scalar, aggregate = aggregate, window = window)

translator = SqlTranslator.from_mappings(
        scalar, window, aggregate,
        MysqlColumn, MysqlColumnAgg
        )

