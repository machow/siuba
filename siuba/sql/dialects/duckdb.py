from sqlalchemy.sql import func as fn
from sqlalchemy import sql

from ..translate import (
    # data types
    SqlColumn, SqlColumnAgg,
    AggOver,
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
)

from .base import sql_func_rank


# Data ========================================================================

class DuckdbColumn(PostgresqlColumn): pass
class DuckdbColumnAgg(PostgresqlColumnAgg, DuckdbColumn): pass


# Annotations =================================================================

def returns_int(func_names):
    # TODO: MC-NOTE - shift all translations to directly register
    # TODO: MC-NOTE - make an AliasAnnotated class or something, that signals
    #                 it is using another method, but w/ an updated annotation.
    from siuba.ops import ALL_OPS
    
    for name in func_names:
        generic = ALL_OPS[name]
        f_concrete = generic.dispatch(SqlColumn)
        f_annotated = wrap_annotate(f_concrete, result_type="int")
        generic.register(DuckdbColumn, f_annotated)
    

# Translations ================================================================


def sql_quantile(is_analytic=False):
    # Ordered and theoretical set aggregates
    sa_func = getattr(sql.func, "percentile_cont")

    def f_quantile(codata, col, q, *args):
        if args:
            raise NotImplementedError("Quantile only supports the q argument.")
        if not isinstance(q, (int, float)):
            raise TypeError("q argument must be int or float, but received: %s" %type(q))

        # as far as I can tell, there's no easy way to tell sqlalchemy to render
        # the exact text a dialect would render for a literal (except maybe using
        # literal_column), so use the classic sql.text.
        q_text = sql.text(str(q))

        if is_analytic:
            return AggOver(sa_func(sql.text(q_text)).within_group(col))

        return sa_func(q_text).within_group(col)

    return f_quantile


# scalar ----

extend_base(
    DuckdbColumn,
    **{
        "str.contains": lambda _, col, re: fn.regexp_matches(col, re),
        "str.title": sql_not_impl(),
    }
)

returns_int([
    "dt.day", "dt.dayofyear", "dt.days_in_month",
    "dt.daysinmonth", "dt.hour", "dt.minute", "dt.month",
    "dt.quarter", "dt.second", "dt.week",
    "dt.weekofyear", "dt.year"
])

# window ----

extend_base(
    DuckdbColumn,
    __floordiv__ = lambda _, x, y: x.op("//")(y),
    __rfloordiv__ = lambda _, y, x: x.op("//")(y),
    rank = sql_func_rank,
    #quantile = sql_quantile(is_analytic=True),
)


# aggregate ----

extend_base(
    DuckdbColumnAgg,
    quantile = sql_quantile(),
)


translator = SqlTranslator.from_mappings(DuckdbColumn, DuckdbColumnAgg)
