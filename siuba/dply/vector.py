import pandas as pd
import numpy as np
from functools import singledispatch
from siuba.siu import symbolic_dispatch


def _expand_bool(x, f):
    return x.expanding().apply(f, raw = True).astype(bool)

@symbolic_dispatch
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


@symbolic_dispatch
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


@symbolic_dispatch
def cummean(x):
    """Return a same-length array, containing the cumulative mean."""
    return x.expanding().mean()

@symbolic_dispatch
def desc(x):
    """Return array sorted in descending order."""
    return x.sort_values(ascending = False).reset_index(drop = True)


@symbolic_dispatch
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

    return arr


@symbolic_dispatch
def ntile(x, n):
    """TODO: Not Implemented"""
    NotImplementedError("ntile not implemented")


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
    

@symbolic_dispatch
def coalesce(*args):
    """TODO: Not Implemented"""
    NotImplementedError("coalesce not implemented")


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
        0     2.0
        1     3.0
        2    99.0
        dtype: float64

    """
    res = x.shift(-1*n)

    if default is not None:
        res.iloc[-n:] = default

    return res


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


@symbolic_dispatch
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


@symbolic_dispatch
def n_distinct(x):
    """Return the total number of distinct (i.e. unique) elements in an array.
    
    Example:
        >>> n_distinct(pd.Series([1,1,2,2]))
        2

    """
    return len(x.unique())


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
