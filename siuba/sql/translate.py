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

class SqlBase(sql.elements.ColumnClause): pass

class SqlColumn(SqlBase): pass

class SqlColumnAgg(SqlBase): pass

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

# TODO: move parts using siu to separate file (includes CallTreeLocal stuff below)
from siuba.siu import FunctionLookupBound

def win_absent(name):
    # Return an error, that is picked up by the translator.
    # this allows us to report errors at translation, rather than call time.
    return FunctionLookupBound("SQL dialect does not support {}.".format(name))

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


# MISC ===========================================================================
# scalar, aggregate, window #no_win, agg_no_win

# local to table
# alias to LazyTbl.no_win.dense_rank...
#          LazyTbl.agg.dense_rank...

from collections.abc import MutableMapping
import itertools

class SqlTranslations(MutableMapping):
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
    

# 

from siuba.ops.translate import create_pandas_translator

# TODO: should inherit from a ITranslate class (w/ abstract translate method)
class SqlTranslator:
    def __init__(self, window, aggregate):
        self.window = window
        self.aggregate = aggregate

    def translate(self, expr, window = True):
        if window:
            return self.window.translate(expr)

        return self.aggregate.translate(expr)


def create_sql_translators(base, agg, window, WinCls, AggCls):
    trans_win = {**base, **window}
    trans_agg = {**base, **agg}
    return SqlTranslator(
            window = create_pandas_translator(trans_win, WinCls, sql.elements.ClauseElement),
            aggregate = create_pandas_translator(trans_agg, AggCls, sql.elements.ClauseElement)
            )
