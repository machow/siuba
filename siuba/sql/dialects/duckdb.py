from sqlalchemy.sql import func as fn
from sqlalchemy import sql

from ..translate import (
    # data types
    SqlColumn, SqlColumnAgg,
    # transformations
    wrap_annotate,
    sql_agg,
    win_agg,
    win_cumul,
    sql_not_impl,
    # wiring up translator
    extend_base,
    SqlTranslator
)

from .postgresql import (
    PostgresqlColumn,
    PostgresqlColumnAgg,
    scalar as pg_scalar,
    aggregate as pg_agg,
    window as pg_win
)

from .base import scalar_dt_methods
from .base import sql_func_rank

class DuckdbColumn(PostgresqlColumn): pass
class DuckdbColumnAgg(PostgresqlColumnAgg, DuckdbColumn): pass


def sql_func_last_day_in_period(col, period):
    return fn.date_trunc(period, col) + sql.text("INTERVAL '1 %s - INTERVAL 1 day'" % period)

def sql_func_days_in_month(col):
    return fn.extract('day', sql_func_last_day_in_period(col, 'month'))

def sql_is_last_day_of(period):
    def f(col):
        last_day = sql_func_last_day_in_period(col, period)
        return fn.date_trunc('day', col) == last_day

    return f

scalar = extend_base(
        pg_scalar,
        **{
            "str.contains": lambda col, re: fn.regexp_matches(col, re),
            "str.title": sql_not_impl(),
        },
        **scalar_dt_methods,
        )


aggregate = extend_base(
        pg_agg,
        sum = sql_agg("sum"),
        )

window = extend_base(
        pg_win,
        cumsum = win_cumul("sum"),
        sum = win_agg("sum"),
        rank = wrap_annotate(sql_func_rank, result_type = "int"),
        )


translator = SqlTranslator.from_mappings(
        scalar, window, aggregate,
        DuckdbColumn, DuckdbColumnAgg
        )
