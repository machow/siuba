import pandas as pd
import numpy as np
from functools import singledispatch
from siuba.siu import symbolic_dispatch
from pandas.core.groupby import SeriesGroupBy, GroupBy
from pandas.core.frame import NDFrame
from pandas import Series

from siuba.experimental.pd_groups.groupby import GroupByAgg, _regroup
from siuba.experimental.pd_groups.translate import method_agg_op

# Utils =======================================================================

def _expand_bool(x, f):
    return x.expanding().apply(f, raw = True).astype(bool)

def group_value_splits(g, to_series = False):
    indices = g.grouper.indices
    for g_key, inds in indices.items():
        array = g.obj.values[inds]
        if to_series:
            indx = pd.RangeIndex._simple_new(range(len(array)))
            yield pd.Series(array, index = indx, dtype = g.obj.dtype, fastpath = True)

        else:
            yield array


def alias_series_agg(name):
    method = method_agg_op(name, is_property = False, accessor = False)

    def decorator(dispatcher):
        dispatcher.register(SeriesGroupBy, method)
        return dispatcher

    return decorator


# Single dispatch functions ===================================================

@symbolic_dispatch(cls = Series)
def cumall(x):
    """Return a same-length array. For each entry, indicates whether that entry and all previous are True-like.

    Example:
        >>> cumall(pd.Series([True, False, False]))
        0     True
        1    False
        2    False
        dtype: bool

    """
    return _expand_bool(x, np.all)


@symbolic_dispatch(cls = Series)
def cumany(x):
    """Return a same-length array. For each entry, indicates whether that entry or any previous are True-like.

    Example:
        >>> cumany(pd.Series([False, True, False]))
        0    False
        1     True
        2     True
        dtype: bool

    """
    return _expand_bool(x, np.any)


@symbolic_dispatch(cls = Series)
def cummean(x):
    """Return a same-length array, containing the cumulative mean."""
    return x.expanding().mean()


@cummean.register(SeriesGroupBy)
def _cummean_grouped(x):
    grouper = x.grouper
    n_entries = x.obj.notna().groupby(grouper).cumsum()

    res = x.cumsum() / n_entries

    return res.groupby(grouper)


@symbolic_dispatch(cls = Series)
def desc(x):
    """Return array sorted in descending order."""
    return x.sort_values(ascending = False).reset_index(drop = True)


@symbolic_dispatch(cls = Series)
def dense_rank(x):
    """Return the dense rank.
    
    This method of ranking returns values ranging from 1 to the number of unique entries.
    Ties are all given the same ranking.

    Example:

        >>> dense_rank(pd.Series([1,3,3,5]))
        0    1.0
        1    2.0
        2    2.0
        3    3.0
        dtype: float64


    """
    return x.rank(method = "dense")


@symbolic_dispatch
def percent_rank(x):
    """TODO: Not Implemented"""
    NotImplementedError("PRs welcome")


@symbolic_dispatch
def min_rank(x):
    """Return the min rank. See pd.Series.rank for details.

    """
    return x.rank(method = "min")


@symbolic_dispatch
def cume_dist(x):
    """Return the cumulative distribution corresponding to each value in x.

    This reflects the proportion of values that are less than or equal to each value.

    """
    return x.rank(method = "max") / x.count()


# row_number ------------------------------------------------------------------

@symbolic_dispatch
def row_number(x):
    """Return the row number (position) for each value in x, beginning with 1.

    Example:
        >>> row_number(pd.Series([7,8,9]))
        0    1
        1    2
        2    3
        dtype: int64

    """
    if isinstance(x, pd.DataFrame):
        n = x.shape[0]
    else:
        n = len(x)
    
    arr = np.arange(1, n + 1)

    # could use single dispatch, but for now ensure output data type matches input
    if isinstance(x, pd.Series):
        return x._constructor(arr, pd.RangeIndex(n), fastpath = True)

    return pd.Series(arr)


@row_number.register(GroupBy)
def _row_number_grouped(g: GroupBy) -> GroupBy:
    out = np.ones(len(g.obj), dtype = int)

    indices = g.grouper.indices
    for g_key, inds in indices.items():
        out[inds] = np.arange(1, len(inds) + 1, dtype = int)
    
    return _regroup(out, g)


# ntile -----------------------------------------------------------------------

@symbolic_dispatch
def ntile(x, n):
    """TODO: Not Implemented"""
    NotImplementedError("ntile not implemented")


# between ---------------------------------------------------------------------

@symbolic_dispatch
def between(x, left, right):
    """Return whether a value is between left and right (including either side).

    Example:
        >>> between(pd.Series([1,2,3]), 0, 2)
        0     True
        1     True
        2    False
        dtype: bool

    Note:
        This is a thin wrapper around pd.Series.between(left, right)
    
    """
    # note: NA -> False, in tidyverse NA -> NA
    return x.between(left, right)
    

# coalesce --------------------------------------------------------------------

@symbolic_dispatch
def coalesce(*args):
    """TODO: Not Implemented"""
    NotImplementedError("coalesce not implemented")


# lead ------------------------------------------------------------------------

@symbolic_dispatch
def lead(x, n = 1, default = None):
    """Return an array with each value replaced by the next (or further forward) value in the array.

    Arguments:
        x: a pandas Series object
        n: number of next values forward to replace each value with
        default: what to replace the n final values of the array with

    Example:
        >>> lead(pd.Series([1,2,3]), n=1)
        0    2.0
        1    3.0
        2    NaN
        dtype: float64

        >>> lead(pd.Series([1,2,3]), n=1, default = 99)
        0     2
        1     3
        2    99
        dtype: int64

    """
    res = x.shift(-1*n, fill_value = default)

    return res


@lead.register(SeriesGroupBy)
def _lead_grouped(x, n = 1, default = None):
    res = x.shift(-1*n, fill_value = default)

    return _regroup(res, x)


# lag -------------------------------------------------------------------------

@symbolic_dispatch
def lag(x, n = 1, default = None):
    """Return an array with each value replaced by the previous (or further backward) value in the array.

    Arguments:
        x: a pandas Series object
        n: number of next values backward to replace each value with
        default: what to replace the n final values of the array with

    Example:
        >>> lag(pd.Series([1,2,3]), n=1)
        0    NaN
        1    1.0
        2    2.0
        dtype: float64

        >>> lag(pd.Series([1,2,3]), n=1, default = 99)
        0    99.0
        1     1.0
        2     2.0
        dtype: float64


    """
    res = x.shift(n)

    if default is not None:
        res.iloc[:n] = default

    return res


@lag.register(SeriesGroupBy)
def _lag_grouped(x, n = 1, default = None):
    res = x.shift(n, fill_value = default)

    return _regroup(res, x)

# n ---------------------------------------------------------------------------

@symbolic_dispatch(cls = NDFrame)
def n(x):
    """Return the total number of elements in the array (or rows in a DataFrame).

    Example:
        >>> ser = pd.Series([1,2,3])
        >>> n(ser)
        3

        >>> df = pd.DataFrame({'x': ser})
        >>> n(df)
        3

    """
    if isinstance(x, pd.DataFrame):
        return x.shape[0]

    return len(x)


@n.register(GroupBy)
def _n_grouped(x: GroupBy) -> GroupByAgg:
    return GroupByAgg.from_result(x.size(), x)


# n_distinct ------------------------------------------------------------------

@alias_series_agg('nunique')
@symbolic_dispatch
def n_distinct(x):
    """Return the total number of distinct (i.e. unique) elements in an array.
    
    Example:
        >>> n_distinct(pd.Series([1,1,2,2]))
        2

    """
    return x.nunique()


# na_if -----------------------------------------------------------------------

@symbolic_dispatch
def na_if(x, y):
    """Return a array like x, but with values in y replaced by NAs.
    
    Examples:
        >>> na_if(pd.Series([1,2,3]), [1,3])
        0    NaN
        1    2.0
        2    NaN
        dtype: float64
        
    """
    y = [y] if not np.ndim(y) else y

    tmp_x = x.copy(deep = True)
    tmp_x[x.isin(y)] = np.nan

    return tmp_x


@symbolic_dispatch
def near(x):
    """TODO: Not Implemented"""
    NotImplementedError("near not implemented") 


@symbolic_dispatch
def nth(x):
    """TODO: Not Implemented"""
    NotImplementedError("nth not implemented") 


@symbolic_dispatch
def first(x):
    """TODO: Not Implemented"""
    NotImplementedError("first not implemented")


@symbolic_dispatch
def last(x):
    """TODO: Not Implemented"""
    NotImplementedError("last not implemented")
