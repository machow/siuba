"""
This module holds default translations from pandas syntax to sql for 3 kinds of operations...

1. scalar - elementwise operations (e.g. array1 + array2)
2. aggregation - operations that result in a single number (e.g. array1.mean())
3. window - operations that do calculations across a window
            (e.g. array1.lag() or array1.expanding().mean())


"""

from sqlalchemy import sql
from sqlalchemy.sql import sqltypes as types, func as fn
from functools import singledispatch
from .verbs import case_when, if_else

# warning for when sql defaults differ from pandas ============================
import warnings


class SiubaSqlRuntimeWarning(UserWarning): pass

def warn_arg_default(func_name, arg_name, arg, correct):
    warnings.warn(
            "\n{func_name} sql translation defaults "
            "{arg_name} to {arg}. To return identical result as pandas, use "
            "{arg_name} = {correct}.\n\n"
            "This warning only displays once per function".format(
                func_name = func_name, arg_name = arg_name, arg = repr(arg), correct = repr(correct)
                ),
            SiubaSqlRuntimeWarning
            )

# Custom dispatching in call trees ============================================
from sqlalchemy.sql.elements import ColumnClause
from sqlalchemy.sql.base import ImmutableColumnCollection

class SqlColumn(ColumnClause): pass

class SqlColumnAgg(SqlColumn): pass

# Custom over clause handling  ================================================

# TODO: must make these take both tbl, col as args, since hard to find window funcs
def sa_is_window(clause):
    return isinstance(clause, sql.elements.Over) \
            or isinstance(clause, sql.elements.WithinGroup)

def sa_get_over_clauses(clause):
    windows = []
    append_win = lambda col: windows.append(col)

    sql.util.visitors.traverse(clause, {}, {"over": append_win})

    return windows

def sa_modify_window(clause, group_by = None, order_by = None):
    if group_by:
        group_cols = [columns[name] for name in group_by]
        partition_by = sql.elements.ClauseList(*group_cols)
        clone = clause._clone()
        clone.partition_by = partition_by

        return clone

    return clause

from sqlalchemy.sql.elements import Over

class CustomOverClause: pass

class AggOver(Over, CustomOverClause):
    def set_over(self, group_by, order_by = None):
        self.partition_by = group_by
        return self


class RankOver(Over, CustomOverClause): 
    def set_over(self, group_by, order_by = None):
        crnt_partition = getattr(self.partition_by, 'clauses', tuple())
        self.partition_by = sql.elements.ClauseList(*crnt_partition, *group_by.clauses)
        return self


class CumlOver(Over, CustomOverClause):
    def set_over(self, group_by, order_by):
        self.partition_by = group_by
        self.order_by = order_by

        if not len(order_by):
            warnings.warn(
                    "No order by columns explicitly set in window function. SQL engine"
                    "does not guarantee a row ordering. Recommend using an arrange beforehand.",
                    RuntimeWarning
                    )
        return self


# Translator creation funcs ===================================================

# Windows ----

def win_absent(name):
    from typing import Any
    def not_implemented(*args, **kwargs) -> Any:
        raise NotImplementedError("SQL dialect does not support {}.".format(name))
    
    return not_implemented

def win_over(name: str):
    sa_func = getattr(sql.func, name)
    def f(col) -> RankOver:
        return RankOver(sa_func(), order_by = col)

    return f

def win_cumul(name):
    sa_func = getattr(sql.func, name)
    def f(col, *args, **kwargs) -> CumlOver:
        return CumlOver(sa_func(col, *args, **kwargs), rows = (None,0))

    return f

def win_agg(name):
    sa_func = getattr(sql.func, name)
    def f(col, *args, **kwargs) -> AggOver:
        return AggOver(sa_func(col, *args, **kwargs))

    return f

def sql_func_diff(col, periods = 1):
    if periods > 0:
        return CumlOver(col - sql.func.lag(col, periods))
    elif periods < 0:
        return CumlOver(col - sql.func.lead(col, abs(periods)))

    raise ValueError("periods argument to sql diff cannot be 0")


# Ordered and theoretical set aggregates ----

def set_agg(name):
    sa_func = getattr(sql.func, name)
    return lambda col, *args: sa_func(*args).within_group(col)

# Datetime ----

def sql_extract(name):
    return lambda col: sql.func.extract(name, col)

def sql_func_extract_dow_monday(col):
    # make monday = 0 rather than sunday
    monday0 = sql.cast(sql.func.extract('dow', col) + 6, types.Integer) % 7
    # cast to numeric, since that's what extract('dow') returns
    return sql.cast(monday0, types.Numeric)

def sql_is_first_of(name, reference):
    return lambda col: fn.date_trunc(name, col) == fn.date_trunc(reference, col)

def sql_func_last_day_in_period(col, period):
    return fn.date_trunc(period, col) + sql.text("interval '1 %s - 1 day'" % period)

def sql_func_days_in_month(col):
    return fn.extract('day', sql_func_last_day_in_period(col, 'month'))

def sql_is_last_day_of(period):
    def f(col):
        last_day = sql_func_last_day_in_period(col, period)
        return fn.date_trunc('day', col) == last_day

    return f

def sql_func_floor_date(col, unit):
    # see https://www.postgresql.org/docs/9.1/functions-datetime.html#FUNCTIONS-DATETIME-TRUNC
    # valid values: 
    #   microseconds, milliseconds, second, minute, hour,
    #   day, week, month, quarter, year, decade, century, millennium
    # TODO: implement in siuba.dply.lubridate
    return fn.date_trunc(unit, col)


# Strings ----

def sql_str_strip(name):
    
    strip_func = getattr(fn, name)
    def f(col, to_strip = " \t\n\v\f\r"):
        return strip_func(col, to_strip)

    return f

def sql_func_capitalize(col):
    first_char = fn.upper(fn.left(col, 1)) 
    rest = fn.right(col, fn.length(col) - 1)
    return first_char.op('||')(rest)


# Others ----

def sql_agg(name):
    sa_func = getattr(sql.func, name)
    return lambda col: sa_func(col)

def sql_scalar(name):
    sa_func = getattr(sql.func, name)
    return lambda col, *args: sa_func(col, *args)

def sql_colmeth(meth, *outerargs):
    def f(col, *args) -> SqlColumn:
        return getattr(col, meth)(*outerargs, *args)
    return f

def sql_not_impl():
    return NotImplementedError


# Custom implementations ----

def sql_func_astype(col, _type):
    mappings = {
            str: types.Text,
            'str': types.Text,
            int: types.Integer,
            'int': types.Integer,
            float: types.Numeric,
            'float': types.Numeric,
            bool: types.Boolean,
            'bool': types.Boolean
            }
    try:
        sa_type = mappings[_type]
    except KeyError:
        raise ValueError("sql astype currently only supports type objects: str, int, float, bool")
    return sql.cast(col, sa_type)


# Base translations ===========================================================

base_scalar = dict(
        # infix operators -----
        # NOTE: all sqlalchemy.Column operators are used
        # TODO: need way to implement ** operator

        # sqlalchemy.ColumnElement methods ----
        cast = sql_colmeth("cast"),
        between = sql_colmeth("between"),
        isin = sql_colmeth("in_"),

        # bitwise operations -----
        # TODO

        # these are methods on sql.funcs ---- 
        abs = sql_scalar("abs"),
        acos = sql_scalar("acos"),
        asin = sql_scalar("asin"),
        atan = sql_scalar("atan"),
        atan2 = sql_scalar("atan2"),
        cos = sql_scalar("cos"),
        cot = sql_scalar("cot"),
        astype = sql_func_astype,
        # I was lazy here and wrote lambdas directly ---
        # TODO: I think these are postgres specific?
        isna = sql_colmeth("is_", None),
        isnull = sql_colmeth("is_", None),
        notna = lambda col: ~col.is_(None),
        fillna = sql.functions.coalesce,
        # dply.vector funcs ----

        # TODO: move to postgres specific
        # TODO: this is to support a DictCall (e.g. used in case_when)
        dict = dict,
        # TODO: don't use singledispatch to add sql support to case_when
        case_when = case_when,
        if_else = if_else,

        # POSTGRES compatility ------------------------------------------------
        # _special_methods
        __round__ = sql_scalar("round"),

        #
        copy = sql_not_impl(),

        # binary
        add = sql_colmeth('__add__'),
        sub = sql_colmeth('__sub__'),
        #truediv
        #floordiv
        mul = sql_colmeth('__mul__'),
        mod = sql_colmeth('__mod__'),
        #pow = sql_colmeth('__pow__'),
        lt = sql_colmeth('__lt__'),
        gt = sql_colmeth('__gt__'),
        le = sql_colmeth('__le__'),
        ge = sql_colmeth('__ge__'),
        ne = sql_colmeth('__ne__'),
        eq = sql_colmeth('__eq__'),
        #round = sql_scalar("round"),
        radd = sql_colmeth('__radd__'),
        rsub = sql_colmeth('__rsub__'),
        #rtruediv
        #rfloordiv
        rmul = sql_colmeth('__rmul__'),
        rmod = sql_colmeth('__rmod__'),

        # computations ---
        clip = lambda col, lower, upper: fn.least(fn.greatest(col, lower), upper),

        # datetime_properties ---
        date = sql_not_impl(),
        time = sql_not_impl(),
        timetz = sql_not_impl(),
        year = sql_extract('year'),# , sql.cast(col, sql.sqltypes.Date)),
        month = sql_extract('month'),
        day = sql_extract('day'),
        hour = sql_extract('hour'),
        minute = sql_extract('minute'),
        second = sql_extract('second'),
        #microsecond = sql_extract('microsecond'), # TODO: postgres includes seconds
        nanosecond = sql_not_impl(),
        week = sql_extract('week'),
        weekofyear = sql_extract('week'),
        dayofweek = sql_func_extract_dow_monday,
        weekday = sql_func_extract_dow_monday,
        dayofyear = sql_extract('doy'),
        quarter = sql_extract('quarter'),
        is_month_start = sql_is_first_of('day', 'month'),
        is_month_end = sql_is_last_day_of('month'),
        is_quarter_start = sql_is_first_of('day', 'quarter'),
        #is_quarter_end = sql_is_last_day_of('quarter'),
        is_year_start = sql_is_first_of('day', 'year'),
        is_year_end = sql_is_last_day_of('year'),
        is_leap_year = sql_not_impl(),
        daysinmonth = sql_func_days_in_month,
        days_in_month = sql_func_days_in_month,
        tz = sql_not_impl(),
        freq = sql_not_impl(),

        # datetime methods ---
        #to_period = ,
        ## dt.to_pydatetime
        #tz_localize = 
        ## dt.tz_convert
        #normalize = 
        #strftime = 
        #round = 
        #floor = 
        #ceil = 
        #month_name = 
        #day_name =
        # TODO: slotting in a floor_date method, since I can't do my job
        #       or make common SQL queries without it....
        floor_date = sql_func_floor_date,


        # string methdos ---
        
        capitalize = sql_func_capitalize,
        #casefold = ,
        #cat  = ,
        center = sql_not_impl(),
        contains = sql_not_impl(),
#        count = ,
#        # str.decode
#        encode = ,
        endswith = sql_colmeth("endswith"),
#        #extract = 
#        # str.extractall
#        find = ,
#        findall = ,
#        #get
#        #index
#        #join
        len = sql.func.length,
#        ljust = ,
        lower = sql.func.lower,
        # TODO: whitespace options based loosely on builtin string.isspace
        lstrip = sql_str_strip('ltrim'),
#        match = ,
#        # str.normalize
#        pad = ,
#        # str.partition
#        # str.repeat
#        replace = ,
#        rfind = ,
#        # str.rindex
#        rjust = ,
#        # str.rpartition
        rstrip = sql_str_strip('rtrim'),
#        slice = ,
#        slice_replace = ,
#        split = ,
#        rsplit = ,
        startswith = sql_colmeth("startswith"),
        strip = sql_str_strip('trim'),
#        swapcase = ,
        title = sql.func.initcap,
#        # str.translate
        upper = sql.func.upper,
#        wrap = ,
#        # str.zfill
#        isalnum = ,
#        isalpha = ,
#        isdigit = ,
#        isspace = ,
#        islower = ,
#        isupper = ,
#        istitle = ,
#        isnumeric = ,
#        isdecimal = ,
        )

base_agg = dict(
        mean = sql_agg("avg"),
        sum = sql_agg("sum"),
        min = sql_agg("min"),
        max = sql_agg("max"),
        count = sql_agg("count"),
        # TODO: generalize case where doesn't use col
        # need better handeling of vector funcs
        nunique = lambda col: sql.func.count(sql.func.distinct(col)),

        # POSTGRES compatibility ----------------------------------------------
        quantile = set_agg("percentile_cont"),
        )

base_win = dict(
        rank = win_over("rank"),
        #first = win_over2("first"),
        #last = win_over2("last"),
        #nth = win_over3
        #lead = win_over4
        #lag

        # aggregate functions ---
        mean = win_agg("avg"),
        var = win_agg("variance"),
        sum = win_agg("sum"),
        min = win_agg("min"),
        max = win_agg("max"),

        # ordered set funcs ---
        #quantile
        #median

        # counts ----
        count = win_agg("count"),
        #n
        #n_distinct

        # cumulative funcs ---
        #avg("id") OVER (PARTITION BY "email" ORDER BY "id" ROWS UNBOUNDED PRECEDING)
        #cummean = win_agg("
        cumsum = win_cumul("sum"),
        #cummin
        #cummax
        diff = sql_func_diff,

        # POSTGRES compatibility ----------------------------------------------
        # computations
        #prod = lambda col: AggOver(fn.exp(fn.sum(fn.log(col)))),
        std = win_agg("stddev_samp"),
        )

# based on https://github.com/tidyverse/dbplyr/blob/master/R/backend-.R
base_nowin = dict(
        #row_number   = win_absent("ROW_NUMBER"),
        #min_rank     = win_absent("RANK"),
        rank         = win_absent("RANK"),
        dense_rank   = win_absent("DENSE_RANK"),
        percent_rank = win_absent("PERCENT_RANK"),
        cume_dist    = win_absent("CUME_DIST"),
        ntile        = win_absent("NTILE"),
        mean         = win_absent("AVG"),
        sd           = win_absent("SD"),
        var          = win_absent("VAR"),
        cov          = win_absent("COV"),
        cor          = win_absent("COR"),
        sum          = win_absent("SUM"),
        min          = win_absent("MIN"),
        max          = win_absent("MAX"),
        median       = win_absent("PERCENTILE_CONT"),
        quantile    = win_absent("PERCENTILE_CONT"),
        n            = win_absent("N"),
        n_distinct   = win_absent("N_DISTINCT"),
        cummean      = win_absent("MEAN"),
        cumsum       = win_absent("SUM"),
        cummin       = win_absent("MIN"),
        cummax       = win_absent("MAX"),
        nth          = win_absent("NTH_VALUE"),
        first        = win_absent("FIRST_VALUE"),
        last         = win_absent("LAST_VALUE"),
        lead         = win_absent("LEAD"),
        lag          = win_absent("LAG"),
        order_by     = win_absent("ORDER_BY"),
        str_flatten  = win_absent("STR_FLATTEN"),
        count        = win_absent("COUNT")
        )

funcs = dict(scalar = base_scalar, aggregate = base_agg, window = base_win)

# MISC ===========================================================================
# scalar, aggregate, window #no_win, agg_no_win

# local to table
# alias to LazyTbl.no_win.dense_rank...
#          LazyTbl.agg.dense_rank...

from collections.abc import MutableMapping
import itertools

class SqlTranslator(MutableMapping):
    def __init__(self, d, **kwargs):
        self.d = d
        self.kwargs = kwargs

    def __len__(self):
        return len(set(self.d) + set(self.kwargs))

    def __iter__(self):
        old_keys = iter(self.d)
        new_keys = (k for k in self.kwargs if k not in self.d)
        return itertools.chain(old_keys, new_keys)

    def __getitem__(self, x):
        try:
            return self.kwargs[x]
        except KeyError:
            return self.d[x]

    def __setitem__(self, k, v):
        self.d[k] = v

    def __delitem__(self, k):
        del self.d[k]
    
