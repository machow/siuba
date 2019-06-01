import pandas as pd
import numpy as np
from functools import singledispatch
from ..siu import Symbolic, create_sym_call,Call


def register_symbolic(f):
    # TODO: don't use singledispatch if it has already been done
    f = singledispatch(f)
    @f.register(Symbolic)
    def _dispatch_symbol(__data, *args, **kwargs):
        return create_sym_call(f, __data.source, *args, **kwargs)

    return f

def _expand_bool(x, f):
    return x.expanding().apply(f).astype(bool)

@register_symbolic
def cumall(x):
    return _expand_bool(x, np.all)


@register_symbolic
def cumany(x):
    return _expand_bool(x, np.any)


@register_symbolic
def cummean(x):
    return x.expanding().mean()

@register_symbolic
def desc(x):
    return x.sort_values()


@register_symbolic
def dense_rank(x):
    return x.rank(method = "dense")


@register_symbolic
def percent_rank(x):
    NotImplementedError("PRs welcome")


@register_symbolic
def min_rank(x):
    return x.rank(method = "min")


@register_symbolic
def cume_dist(x):
    return x.rank(method = "max") / x.count()


@register_symbolic
def row_number(x):
    if isinstance(x, pd.DataFrame):
        n = x.shape[0]
    else:
        n = len(x)
    return np.arange(1, n + 1)


@register_symbolic
def ntile(x, n):
    NotImplementedError("ntile not implemented")


@register_symbolic
def between(x, left, right):
    # note: NA -> False, in tidyverse NA -> NA
    return x.between(left, right)
    

@register_symbolic
def coalesce(*args):
    NotImplementedError("coalesce not implemented")


@register_symbolic
def lead(x, n = 1, default = None):
    res = x.shift(-1*n)

    if default is not None:
        res.iloc[-n:] = default

    return res


@register_symbolic
def lag(x, n = 1, default = None):
    res = x.shift(n)

    if default is not None:
        res.iloc[:n] = default

    return res


@register_symbolic
def n(x):
    if isinstance(x, pd.DataFrame):
        return x.shape[0]

    return len(x)


@register_symbolic
def n_distinct(x):
    return len(x.unique())


@register_symbolic
def na_if(x, y):
    y = [y] if not np.ndim(y) else y

    tmp_x = x.copy(deep = True)
    tmp_x[x.isin(y)] = np.nan

    return tmp_x


@register_symbolic
def near():
    NotImplementedError("near not implemented") 


@register_symbolic
def nth():
    NotImplementedError("nth not implemented") 


@register_symbolic
def first():
    NotImplementedError("first not implemented")


@register_symbolic
def last():
    NotImplementedError("last not implemented")
