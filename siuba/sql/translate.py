from sqlalchemy import sql
from sqlalchemy.sql import sqltypes as types
from functools import singledispatch
from .verbs import case_when, if_else

# TODO: must make these take both tbl, col as args, since hard to find window funcs
def sa_is_window(clause):
    return isinstance(clause, sql.elements.Over) \
            or isinstance(clause, sql.elements.WithinGroup)


def sa_modify_window(clause, columns, group_by = None, order_by = None):
    cls = clause.__class__ if sa_is_window(clause) else getattr(clause, "over")
    if group_by:
        partition_by = [columns[name] for name in group_by]
        return cls(**{**clause.__dict__, 'partition_by': partition_by})

    return clause


def win_over(name):
    sa_func = getattr(sql.func, name)
    return lambda col: sa_func().over(order_by = col)


def win_agg(name):
    sa_func = getattr(sql.func, name)
    return lambda col: sa_func(col).over()

def sql_agg(name):
    sa_func = getattr(sql.func, name)
    return lambda col: sa_func(col)

def sql_scalar(name):
    sa_func = getattr(sql.func, name)
    return lambda col: sa_func(col)

def sql_colmeth(meth):
    def f(col, *args):
        return getattr(col, meth)(*args)
    return f

def sql_astype(col, _type):
    mappings = {
            str: types.Text,
            int: types.Integer,
            float: types.Numeric,
            bool: types.Boolean
            }
    sa_type = mappings[_type]
    return sql.cast(col, sa_type)

base_scalar = dict(
        # TODO: these methods are sqlalchemy ColumnElement methods, simplify?
        cast = sql_colmeth("cast"),
        startswith = sql_colmeth("startswith"),
        endswith = sql_colmeth("endswith"),
        between = sql_colmeth("between"),
        isin = sql_colmeth("in_"),
        # these are methods on sql.funcs ---- 
        abs = sql_scalar("abs"),
        acos = sql_scalar("acos"),
        asin = sql_scalar("asin"),
        atan = sql_scalar("atan"),
        atan2 = sql_scalar("atan2"),
        cos = sql_scalar("cos"),
        cot = sql_scalar("cot"),
        astype = sql_astype,
        # I was lazy here and wrote lambdas directly ---
        # TODO: I think these are postgres specific?
        hour = lambda col: sql.func.date_trunc('hour', col),
        week = lambda col: sql.func.date_trunc('week', col),
        isna = lambda col: col.is_(None),
        isnull = lambda col: col.is_(None),
        # dply.vector funcs ----
        
        # TODO: string methods
        #str.len,
        #str.upper,
        #str.lower,
        #str.replace_all or something similar,
        #str_detect or similar,
        #str_trim func to cut text off sides
        # TODO: move to postgres specific
        n = lambda col: sql.func.count(),
        sum = sql_scalar("sum"),
        # TODO: this is to support a DictCall (e.g. used in case_when)
        dict = dict,
        # TODO: don't use singledispatch to add sql support to case_when
        case_when = case_when,
        if_else = if_else
        )

base_agg = dict(
        mean = sql_agg("avg"),
        # TODO: generalize case where doesn't use col
        # need better handeling of vector funcs
        len = lambda col: sql.func.count()
        )

base_win = dict(
        row_number = win_over("row_number"),
        min_rank = win_over("rank"),
        rank = win_over("rank"),
        dense_rank = win_over("dense_rank"),
        percent_rank = win_over("percent_rank"),
        cume_dist = win_over("cume_dist"),
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
        len = lambda col: sql.func.count().over(),
        #n
        #n_distinct

        # cumulative funcs ---
        #avg("id") OVER (PARTITION BY "email" ORDER BY "id" ROWS UNBOUNDED PRECEDING)
        #cummean = win_agg("
        #cumsum
        #cummin
        #cummax

        )

funcs = dict(scalar = base_scalar, aggregate = base_agg, window = base_win)

# MISC ===========================================================================
# scalar, aggregate, window #no_win, agg_no_win

# local to table
# alias to LazyTbl.no_win.dense_rank...
#          LazyTbl.agg.dense_rank...

from collections.abc import Mapping
import itertools

class SqlTranslator(Mapping):
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
