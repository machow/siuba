from sqlalchemy import sql
from functools import singledispatch

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


base_scalar = dict(
        startswith = lambda col, x: col.startswith(x)
        )

base_agg = dict(
        mean = sql_agg("avg")
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
        mean = win_agg("mean"),
        var = win_agg("variance"),
        sum = win_agg("sum"),
        min = win_agg("min"),
        max = win_agg("max"),

        # ordered set funcs ---
        #quantile
        #median

        # counts ----
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
from functools import partial

class SqlFuncs(Mapping):
    def __init__(self, tbl, funcs):
        self._tbl = tbl
        self._funcs = funcs

    def __len__(self):
        return len(self._funcs)

    def __iter__(self):
        return iter(map(self.__getitem__, self._funcs.values()))

    def __getitem__(self, x):
        return partial(self._funcs[x], self._tbl)

    def __getattr__(self, x):
        return self.__getitem__(x)

