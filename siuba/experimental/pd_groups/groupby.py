from pandas.core.groupby import DataFrameGroupBy, SeriesGroupBy
import inspect
from pandas.core import algorithms
import pandas as pd


# Custom SeriesGroupBy class ==================================================

class GroupByAgg(SeriesGroupBy):
    def __init__(self, *args, orig_grouper, orig_obj, should_cast, **kwargs):
        self._orig_grouper = orig_grouper
        self._orig_obj = orig_obj
        self._should_cast = should_cast
        super().__init__(*args, **kwargs)
    
    def _broadcast_agg_result(self):
        return broadcast_agg_result(
            self._orig_grouper, 
            self.obj,
            self._orig_obj,
            cast = self._should_cast
        )
    
    @classmethod
    def from_result(cls, result, groupby):
        if not isinstance(result, pd.Series):
            raise TypeError("requires pandas Series")

        # Series.groupby is hard-coded to produce a SeriesGroupBy,
        # but it's signature is very large, so use inspect to bind on it.
        sig = inspect.signature(result.groupby)
        bound = sig.bind(by = result.index)
        
        orig_grouper = groupby._orig_grouper if isinstance(groupby, cls) else groupby.grouper
        orig_obj = groupby._orig_obj if isinstance(groupby, cls) else groupby.obj
        should_cast = False
        
        return cls(
            result,
            *bound.args, **bound.kwargs,
            orig_grouper = orig_grouper,
            orig_obj = orig_obj,
            should_cast = should_cast
            )

def broadcast_agg_result(grouper, result, obj, cast = False):
    """
    fast version of transform, only applicable to
    builtin/cythonizable functions
    """
    ids, _, ngroup = grouper.group_info
    out = algorithms.take_1d(result._values, ids)
    
    # TODO: consequence of skipping this step? A cast is already done
    # once when aggregating....
    if cast:
        out = try_cast(out, obj)
    return pd.Series(out, index=obj.index, name=obj.name)


# Utils =======================================================================

def all_isinstance(cls, *args):
    return all(isinstance(x, cls) for x in args)

def _regroup(res, groupby):
    if isinstance(groupby, GroupByAgg):
        # need to manually a constructor, since Series classes are hardcoded
        # all over the pandas library :/ :/ :/
        return groupby.from_result(res, groupby)
    elif isinstance(groupby, SeriesGroupBy):
        return res.groupby(groupby.grouper)
    
    raise ValueError("Unknown group by class: %s"% type(groupby))


# Broadcasting Groupby elements -----------------------------------------------

def grouper_match(grp1, grp2):
    if not isinstance(grp2, SeriesGroupBy):
        raise TypeError("grp2 must be a SeriesGroupBy")

    if grp1._orig_grouper is not grp2.grouper:
        raise ValueError("groups must match")

    return grp1._broadcast_agg_result(), grp2.obj, grp2
    

def broadcast_group_elements(x, y):
    """Returns 3-tuple of same-length x and y data, plus a reference group by object.

    Note:
        * Raises error if x and y are not compatible group by objects.
        * Will broadcast a GroupByAgg, to ensure same length as other data.
    """
    if all_isinstance(GroupByAgg, x, y) and x._orig_grouper is y._orig_grouper:
        return x.obj, y.obj, x
    
    elif isinstance(x, GroupByAgg):
        return grouper_match(x, y)
    
    elif isinstance(y, GroupByAgg):
        res_y, res_x, grp = grouper_match(y, x)
        return res_x, res_y, grp
    
    elif all_isinstance(SeriesGroupBy, x, y) and x.grouper is y.grouper:
        return x.obj, y.obj, x

    raise ValueError("need groupby objects with matching groupers")

