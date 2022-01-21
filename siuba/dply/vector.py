import pandas as pd
import numpy as np
from functools import singledispatch
from siuba.siu import symbolic_dispatch
from pandas.core.groupby import SeriesGroupBy, GroupBy
from pandas.core.frame import NDFrame
from pandas import Series

from siuba.experimental.pd_groups.groupby import GroupByAgg, regroup
from siuba.experimental.pd_groups.translate import method_agg_op

__ALL__ = [
        "cumall", "cumany", "cummean", 
        "desc",
        "dense_rank", "percent_rank", "min_rank", "cume_dist",
        "row_number",
        "ntile",
        "between",
        "coalesce",
        "lead", "lag",
        "n", "n_distinct",
        "na_if",
        "near",
        "nth", "first", "last"
        ]

# Utils =======================================================================

def _expand_bool(x, f):
    return x.expanding().apply(f, raw = True).astype(bool)

def alias_series_agg(name):
    method = method_agg_op(name, is_property = False, accessor = False)

    def decorator(dispatcher):
        dispatcher.register(SeriesGroupBy, method)
        return dispatcher

    return decorator


# Single dispatch functions ===================================================

# cumall ----------------------------------------------------------------------

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


# cumany ----------------------------------------------------------------------

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


# cummean ---------------------------------------------------------------------

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


# desc ------------------------------------------------------------------------

@symbolic_dispatch(cls = Series)
def desc(x):
    """Return array sorted in descending order."""
    return x.sort_values(ascending = False).reset_index(drop = True)


# dense_rank ------------------------------------------------------------------

@symbolic_dispatch(cls = Series)
def dense_rank(x, na_option = "keep"):
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
    return x.rank(method = "dense", na_option = na_option)


# percent_rank ----------------------------------------------------------------

@symbolic_dispatch(cls = Series)
def percent_rank(x, na_option = "keep"):
    """Return the percent rank.

    Note:
        Uses minimum rank, and reports the proportion of unique ranks each entry is greater than.

    Examples:
        >>> percent_rank(pd.Series([1, 2, 3]))
        0    0.0
        1    0.5
        2    1.0
        dtype: float64

        >>> percent_rank(pd.Series([1, 2, 2]))
        0    0.0
        1    0.5
        2    0.5
        dtype: float64

        >>> percent_rank(pd.Series([1]))
        0   NaN
        dtype: float64


    """
    return (min_rank(x) - 1) / (x.count() - 1)


# min_rank --------------------------------------------------------------------

@symbolic_dispatch(cls = Series)
def min_rank(x, na_option = "keep"):
    """Return the min rank. See pd.Series.rank with method="min" for details.

    """
    return x.rank(method = "min", na_option = na_option)


# cume_dist -------------------------------------------------------------------

@symbolic_dispatch(cls = Series)
def cume_dist(x, na_option = "keep"):
    """Return the cumulative distribution corresponding to each value in x.

    This reflects the proportion of values that are less than or equal to each value.

    """
    return x.rank(method = "max", na_option = na_option) / x.count()


# row_number ------------------------------------------------------------------

@symbolic_dispatch(cls = NDFrame)
def row_number(x):
    """Return the row number (position) for each value in x, beginning with 1.

    Example:
        >>> ser = pd.Series([7,8])
        >>> row_number(ser)
        0    1
        1    2
        dtype: int64

        >>> row_number(pd.DataFrame({'a': ser}))
        0    1
        1    2
        dtype: int64

        >>> row_number(pd.Series([7,8], index = [3, 4]))
        3    1
        4    2
        dtype: int64


    """
    if isinstance(x, pd.DataFrame):
        n = x.shape[0]
    else:
        n = len(x)
    
    arr = np.arange(1, n + 1)

    # could use single dispatch, but for now ensure output data type matches input
    if isinstance(x, pd.Series):
        return x._constructor(arr, x.index, fastpath = True)

    return pd.Series(arr, x.index, fastpath = True)


@row_number.register(GroupBy)
def _row_number_grouped(g: GroupBy) -> GroupBy:
    out = np.ones(len(g.obj), dtype = int)

    indices = g.grouper.indices
    for g_key, inds in indices.items():
        out[inds] = np.arange(1, len(inds) + 1, dtype = int)
    
    return regroup(g, out)


# ntile -----------------------------------------------------------------------

@symbolic_dispatch(cls = Series)
def ntile(x, n):
    """TODO: Not Implemented"""
    raise NotImplementedError("ntile not implemented")


# between ---------------------------------------------------------------------

@symbolic_dispatch(cls = Series)
def between(x, left, right, default = False):
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
    if default is not False:
        raise TypeError("between function must use default = False for pandas Series")

    return x.between(left, right)
    

# coalesce --------------------------------------------------------------------

@symbolic_dispatch(cls = Series)
def coalesce(x, *args):
    """Returns a copy of x, with NaN values filled in from \*args. Ignores indexes.

    Arguments:
        x: a pandas Series object
        *args: other Series that are the same length as x, or a scalar

    Examples:
        >>> x = pd.Series([1.1, None, None])
        >>> abc = pd.Series(['a', 'b', None])
        >>> xyz = pd.Series(['x', 'y', 'z'])
        >>> coalesce(x, abc)
        0     1.1
        1       b
        2    None
        dtype: object

        >>> coalesce(x, abc, xyz)
        0    1.1
        1      b
        2      z
        dtype: object
        
    """

    crnt = x.reset_index(drop = True)

    for other in args:
        if isinstance(other, pd.Series):
            other = other.reset_index(drop = True)

        crnt = crnt.where(crnt.notna(), other)

    crnt.index = x.index
    return crnt


# lead ------------------------------------------------------------------------

@symbolic_dispatch(cls = Series)
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

    return regroup(x, res)


# lag -------------------------------------------------------------------------

@symbolic_dispatch(cls = Series)
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

    return regroup(x, res)

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
@symbolic_dispatch(cls = Series)
def n_distinct(x):
    """Return the total number of distinct (i.e. unique) elements in an array.
    
    Example:
        >>> n_distinct(pd.Series([1,1,2,2]))
        2

    """
    return x.nunique()


# na_if -----------------------------------------------------------------------

@symbolic_dispatch(cls = Series)
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


# near ------------------------------------------------------------------------

@symbolic_dispatch(cls = Series)
def near(x):
    """TODO: Not Implemented"""
    raise NotImplementedError("near not implemented") 


# nth -------------------------------------------------------------------------

@symbolic_dispatch(cls = Series)
def nth(x, n, order_by = None, default = None):
    """Return the nth entry of x. Similar to x[n].

    Note:
        first(x) and last(x) are nth(x, 0) and nth(x, -1).

    Arguments:
        x: series to get entry from.
        n: position of entry to get from x (0 indicates first entry).
        order_by: optional Series used to reorder x.
        default: (not implemented) value to return if no entry at n.

    Examples:
        >>> ser = pd.Series(['a', 'b', 'c'])
        >>> nth(ser, 1)
        'b'

        >>> sorter = pd.Series([1, 2, 0])
        >>> nth(ser, 1, order_by = sorter)
        'a'

        >>> nth(ser, 0), nth(ser, -1)
        ('a', 'c')

        >>> first(ser), last(ser)
        ('a', 'c')

    """

    if default is not None:
        raise NotImplementedError("default argument not implemented") 

    # check indexing is in range, handles positive and negative cases.
    # TODO: is returning None the correct behavior for an empty Series?
    if n >= len(x) or abs(n) > len(x):
        return default

    if order_by is None:
        return x.iloc[n]

    # case where order_by is specified and n in range ----
    # TODO: ensure order_by is arraylike
    if not isinstance(order_by, pd.Series):
        raise NotImplementedError(
                "order_by argument is type %s, but currently only"
                "implemented for Series" % type(order_by)
                )

    if len(x) != len(order_by):
        raise ValueError("x and order_by arguments must be same length")

    order_indx = order_by.reset_index(drop = True).sort_values().index
    return x.iloc[order_indx[n]]


# first and last ----

from functools import wraps

_copy_nth_docs = wraps(nth, assigned = ('__doc__',))

@_copy_nth_docs
def first(x, order_by = None, default = None):
    return nth(x, 0, order_by, default)


@_copy_nth_docs
def last(x, order_by = None, default = None):
    return nth(x, -1, order_by, default)
