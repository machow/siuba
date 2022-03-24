from siuba.siu import symbolic_dispatch
from siuba.sql.translate import SqlColumn, SqlColumnAgg
from sqlalchemy.sql import func as fn
from sqlalchemy import types as sa_types
from sqlalchemy import sql

@symbolic_dispatch(cls = SqlColumn)
def date_trunc(_, col, period):
    return fn.date_trunc(period, col)


# TODO: MC-NOTE: make a date_trunc generic?
# for mysql, could implement as https://stackoverflow.com/a/32955740

@symbolic_dispatch(cls = SqlColumn)
def sql_func_last_day_in_period(codata, col, period):
    return date_trunc(codata, col, period) + sql.text("interval '1 %s - 1 day'" % period)


# TODO: RENAME TO GEN
# MYSQL: sql_is_date_offset(period, is_start=True)
@symbolic_dispatch(cls = SqlColumn)
def sql_is_first_day_of(codata, col, period):
    return date_trunc(codata, col, "day") == date_trunc(codata, col, period)


# TODO: how to handle this function generator?
# RENAME TO GEN
@symbolic_dispatch(cls = SqlColumn)
def sql_is_last_day_of(codata, col, period):
    last_day = sql_func_last_day_in_period(codata, col, period)
    return date_trunc(codata, col, 'day') == last_day


# MYSQL FUNCS ===

# TODO: easy to register
# def sql_func_extract_dow_monday(_, col):
#     # MYSQL: sunday starts, equals 1 (an int)
#     # pandas: monday starts, equals 0 (also an int)
# 
#     raw_dow = fn.dayofweek(col)
# 
#     # monday is 2 in MYSQL, so use monday + 5 % 7
#     return (raw_dow + 5) % 7

def sql_is_date_offset(period, is_start = True):

    # will check against one day in the past for is_start, v.v. otherwise
    fn_add = fn.date_sub if is_start else fn.date_add

    def f(_, col):
        get_period = lambda col: fn.extract(period, col)
        src_per = get_period(col)
        incr_per = get_period(fn_add(col, sql.text("INTERVAL 1 DAY")))

        return src_per != incr_per

    return f
