"""GroupByAgg class and generic methods for fast pandas grouped operations.
"""

import inspect

from functools import singledispatch

from pandas import Series
from pandas.api.types import is_scalar
from pandas.core.groupby import SeriesGroupBy, DataFrameGroupBy

try:
    from pandas.core.algorithms import take_1d
except ImportError:
    from pandas.core.array_algos.take import take_1d


# Custom SeriesGroupBy class ==================================================

class GroupByAgg(SeriesGroupBy):
    """Class for representing the result of a grouped Series aggregation.

    Imagine that you are trying to add two grouped Series, where one might be
    an aggregate:

    >>> from siuba.data import mtcars
    >>> g_cyl = mtcars.groupby("cyl")
             
    >>> avg_hp_raw = g_cyl.hp.mean()
    >>> # how can we do: g_cyl.hp - avg_hp_raw ?

    This class is designed to allows operations like minute (``-``) to work under 3 cases:

    * original - aggregate: broadcast to original length; return SeriesGroupBy.
    * aggregate - aggregate: no need to broadcast; return GroupByAgg.
    * unary method over aggregate: no need to broadcast; return GroupByAgg.

    Due to complexities in how pandas creates grouped objects, the easiest way
    to create this class is to use its ``from_result`` class method:

    >>> avg_hp = GroupByAgg.from_result(avg_hp_raw, g_cyl.hp)
    
    Below are examples of the first two cases:

    >>> # avg_hp plus hp ----
    >>> x, y, grp = broadcast_group_elements(avg_hp, g_cyl.hp)
    >>> res1 = regroup(grp, x + y)           # SeriesGroupBy
    
    >>> # avg_hp plus avg_hp ----
    >>> x, y, grp = broadcast_group_elements(avg_hp, avg_hp)
    >>> res2 = regroup(grp, x + y)    # GroupByAgg

    You can use `is_compatible` to check whether broadcasting will work:
    
    >>> is_compatible(g_cyl.hp, avg_hp)
    True
    
    Lastly, this is a subclass of SeriesGroupBy, where each row is its
    own group, so unary methods can be performed without issue.:

    >>> res3 = regroup(grp, avg_hp.fillna(1))  # GroupByAgg
    >>> ser = broadcast_agg(res3)           # ungroup, same len as original data

    """


    def __init__(self, *args, orig_grouper, orig_obj, **kwargs):
        self._orig_grouper = orig_grouper
        self._orig_obj = orig_obj
        super().__init__(*args, **kwargs)
    
    @classmethod
    def from_result(cls, result: Series, src_groupby: SeriesGroupBy):
        """GroupByAgg class constructor.
        """

        if not isinstance(result, Series):
            raise TypeError("requires pandas Series")

        # Series.groupby is hard-coded to produce a SeriesGroupBy,
        # but its signature is very large, so use inspect to bind on it.
        sig = inspect.signature(result.groupby)
        bound = sig.bind(by = result.index)
        
        orig_grouper = getattr(src_groupby, "_orig_grouper", src_groupby.grouper)
        orig_obj     = getattr(src_groupby, "_orig_obj", src_groupby.obj)
        
        return cls(
            result,
            *bound.args, **bound.kwargs,
            orig_grouper = orig_grouper,
            orig_obj = orig_obj,
            )


# SeriesGroupBy generic functions =============================================

# broadcast_agg ---- 

@singledispatch
def broadcast_agg(groupby, result, obj):
    """Return a Series that is the length of original data, with original index."""

    raise NotImplementedError()


@broadcast_agg.register(GroupByAgg)
def _broadcast_agg_gba(groupby):
    """
    >>> import pandas as pd
    >>> gdf = pd.DataFrame({"g": ['a','a','b'], "x": [4,5,6]}).groupby("g")
    >>> agg = GroupByAgg.from_result(gdf.x.mean(), gdf.x)

    >>> len(broadcast_agg(agg))
    3

    """

    src = groupby._orig_obj
    ids, _, ngroup = groupby._orig_grouper.group_info
    out = take_1d(groupby.obj._values, ids)
    
    # Note: reductions like siuba.dply.vector.n(_) map DataFrameGroupBy -> GroupByAgg,
    # so the underlying object is a DataFrame, and does not have a .name attribute.
    return Series(out, index=src.index, name=getattr(src, "name", None))

@broadcast_agg.register(SeriesGroupBy)
def _broadcast_agg_sgb(groupby):
    return groupby.obj


# regroup ----

@singledispatch
def regroup(groupby, res):
    """Return an instance of type(groupby) from res."""

    raise TypeError("Not implemented for group by class: %s"% type(groupby))

@regroup.register(GroupByAgg)
def _(groupby, res):
    return groupby.from_result(res, groupby)

@regroup.register(SeriesGroupBy)
def _(groupby, res):
    # TODO: this will always return SeriesGroupBy, even if groupby is a subclass
    return res.groupby(groupby.grouper)


# is_compatible ----

@singledispatch
def is_compatible(grp1: SeriesGroupBy, grp2: SeriesGroupBy):
    """Return whether objects have identical original groupers."""

    grouper1 = getattr(grp1, '_orig_grouper', grp1.grouper)
    grouper2 = getattr(grp2, '_orig_grouper', grp2.grouper)

    return grouper1 is grouper2


# Utils =======================================================================

def all_isinstance(cls, *args):
    return all(isinstance(x, cls) for x in args)

# Broadcasting Groupby elements -----------------------------------------------

def grouper_match(grp1: SeriesGroupBy, grp2):
    # No need to broadcast against a scalar (pandas will handle) ----
    if is_scalar(grp2):
        return grp1.obj, grp2
    
    # Broadcasting requires: non-agg groupby with same original grouper ----
    if not isinstance(grp2, SeriesGroupBy):
        raise TypeError("grp2 must be a scalar or SeriesGroupBy")

    if not is_compatible(grp1, grp2):
        raise ValueError("groups must have matching groupers")

    return broadcast_agg(grp1), broadcast_agg(grp2)
    

def broadcast_group_elements(x, y):
    """Returns 3-tuple of same-length x, y Series, plus a reference group by object.

    Note:
        * Raises error if x and y are not compatible group by objects.
        * Will broadcast a GroupByAgg, to ensure same length as other data.
    """

    # Both are aggregations, so don't need to broadcast ----
    if all_isinstance(GroupByAgg, x, y) and is_compatible(x, y):
        return x.obj, y.obj, x
    
    # Only one is an aggregation, may broadcast along other ----
    elif isinstance(x, SeriesGroupBy):
        res_x, res_y = grouper_match(x, y)
        return res_x, res_y, x
    
    elif isinstance(y, SeriesGroupBy):
        # same as above, but with args / results flipped
        res_y, res_x, grouper_match(y, x)
        return res_x, res_y, y
    
    # Both are non-agg groupby, just need underlying objects ----
    #elif all_isinstance(SeriesGroupBy, x, y) and is_compatible(x, y):
    #    return x.obj, y.obj, x
    

    raise ValueError("need scalar, or groupby objects with matching groupers")

