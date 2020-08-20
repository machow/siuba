"""
NOTE: This module provides an alternative, OOP approach to fast grouped operations.
      The downside of this approach is that it requires pretending to subclass SeriesGroupBy,
      using the FastGroupedSeries class, and wrapping results to ensure that they
      return that class instead.

      A more principled approach is possible in recent versions of pandas, using proper
      _constructor arguments.
"""


from siuba.spec.series import spec
from siuba.experimental.pd_groups.dialect import create_grouped_methods

from siuba.experimental.pd_groups.translate import SeriesGroupBy, GroupByAgg, GROUP_METHODS

from functools import wraps

from abc import ABCMeta

# TODO: accessors, refactor groupby to use an ABC SerieGroupBy class?

# factory for creating methods dictionary =====================================

local, call_props = create_grouped_methods(
        spec,
        GROUP_METHODS,
        keep_only_impl = False,
        wrap_properties = True
        )

def _series_group_swap_wrapper(f):
    if isinstance(f, property):
        return property(
                _series_group_swap_wrapper(f.__get__),
                f.__set__,
                f.__delattr__
                )

    @wraps(f)
    def wrapper(self, *args, **kwargs):
        res = f(self._data, *args, **kwargs)

        if not isinstance(res, GroupByAgg):
            return FastGroupedSeries(res)
        return res

    return wrapper


# classes =====================================================================

class FastGroupedDataFrame:
    def __init__(self, df):
        self.__data = df

    def __getitem__(self, x):
        if isinstance(x, str):
            return FastGroupedSeries(self.__data[x])

        raise TypeError("%s can only accept a single column name as as string" %self.__class__.__name__)

    def __getattr__(self, x):
        if x in self.__data.obj.columns:
            return FastGroupedSeries(self.__data[x])

        raise AttributeError("no column named %s" %x)

class FastGroupedSeries(metaclass=ABCMeta):
    """
    Imagine that Series only have 3 attributes:
        * __getitem__  (for filtering)
        * index
        * array
        * grouper (for SeriesGroupBy)
        * obj     (for SeriesGroupBy)
    """

    def __init__(self, data):
        self._data = data
    
    @property
    def grouper(self):
        return self._data.grouper

    @property
    def index(self):
        return self._data.index

    @property
    def obj(self):
        return self._data.obj

    @property
    def __class__(self):
        # This allows this class to pass as SeriesGroupBy, but is also dangerous,
        # since class construction using self.__class__... will now use SeriesGroupBy
        return SeriesGroupBy



def extract(grouped_ser):
    f_broadcast = getattr(grouped_ser, '_broadcast_agg_result', None)
    if f_broadcast is not None:
        return f_broadcast

    return grouped_ser.obj


for k, meth in local.items():
    setattr(FastGroupedSeries, k, _series_group_swap_wrapper(meth))

del k, meth

