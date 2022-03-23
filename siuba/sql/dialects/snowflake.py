from sqlalchemy.sql import func as fn
from sqlalchemy import sql

from ..translate import (
    SqlTranslator,
    extend_base,
    sql_scalar,
    sql_agg,
    win_agg,
    win_cumul,
    annotate
)

#from .postgresql import PostgresqlColumn, PostgresqlColumnAgg
from .base import SqlColumn, SqlColumnAgg
from . import _dt_generics as _dt

# Data ----

class SnowflakeColumn(SqlColumn): pass
class SnowflakeColumnAgg(SqlColumnAgg, SnowflakeColumn): pass


# Translations ================================================================

@_dt.sql_func_last_day_in_period.register
def sql_func_last_day_in_period(codata: SnowflakeColumn, col, period):
    return _dt.date_trunc(codata, col, period) \
            + sql.text("interval '1 %s'" % period) \
            - sql.text("interval '1 day'")

# Scalar ----
extend_base(
    SnowflakeColumn,
    __floordiv__ = lambda _, x, y: fn.floor(x / y),
    __rfloordiv__ = lambda _, x, y: fn.floor(y / x),

    # connector has a bug with %
    # see: https://github.com/snowflakedb/snowflake-sqlalchemy/issues/246
    __mod__ = lambda _, x, y: fn.mod(x, y),
    __rmod__ = lambda _, x, y: fn.mod(y, x),
    mod = lambda _, x, y: fn.mod(x,y),
    rmod = lambda _, x, y: fn.mod(y,x),

    # TODO: str.contains
)

# Window ----

extend_base(
    SnowflakeColumn,
    all = win_agg("booland_agg"),
    any = win_agg("boolor_agg"),
    count = win_agg("count"),
    cumsum = annotate(win_cumul("sum"), result_type="variable"),
    # note that the number of decimal places Snowflake returns, and whether
    # the result is numeric depends on the input. mark as variable, so tests
    # do not check dtype
    # see https://community.snowflake.com/s/question/0D50Z000079hpxvSAA/numeric-calculations-truncated-to-3-decimal-places
    mean = annotate(win_agg("avg"), result_type="variable"),
    std = win_agg("stddev_samp"),
    sum = annotate(win_agg("sum"), result_type="variable"),

    var = win_agg("var_samp"),

    # str.contains
    # dt methods are more like base
)

# Agg ----

extend_base(
    SnowflakeColumnAgg,
    all = sql_agg("booland_agg"),
    any = sql_agg("boolor_agg"),
    count = sql_agg("count"),

    std = sql_agg("stddev_samp"),
    var = sql_agg("var_samp"),
)


translator = SqlTranslator.from_mappings(
        SnowflakeColumn, SnowflakeColumnAgg
        )
