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

def _forward_method(f):
    # Wrap a method for a DataDecorator, so it replaces any DataDecorator instances
    # with the underlying data. This is important because it allows DataDecorator
    # to essentially do the same job as a visitor over expressions.
    if isinstance(f, property):
        return property(
                _forward_method(f.__get__),
                f.__set__,
                f.__delattr__
                )

    @wraps(f)
    def wrapper(self, *args, **kwargs):
        new_args = map(strip_data, args)
        new_kwargs = {k: strip_data(v) for k,v in kwargs.items()}
        res = f(strip_data(self), *new_args, **new_kwargs)

        # since whole system relies on methods returning grouped series
        # should be trivially true, but putting in as sanity check
        from pandas.core.groupby import SeriesGroupBy
        assert isinstance(res, SeriesGroupBy)

        return self.__class__(res)


    return wrapper

def strip_data(x):
    if isinstance(x, (DataDecorator)):
        return x._data
    
    return x


# classes =====================================================================

class DataDecorator:
    """
    used for class decorator pattern.
    see https://en.wikipedia.org/wiki/Decorator_pattern.
    """

    def __init__(self, data):
        self._data = data

    

class FastGroupedDataFrame(DataDecorator):
    """Decorates a DataFrameGroupBy to produce the FastGroupedSeries class below."""

    def __getitem__(self, x):
        if isinstance(x, str):
            return FastGroupedSeries(self._data[x])

        raise TypeError("%s can only accept a single column name as as string" %self.__class__.__name__)

    def __getattr__(self, x):
        if x in self._data.obj.columns:
            return FastGroupedSeries(self._data[x])

        raise AttributeError("no column named %s" %x)

class FastGroupedSeries(DataDecorator):
    """Forwards fast versions of methods to underlying SeriesGroupBy.

    """
    # Note that loop at bottom of class puts methods onto it

    # These properties are forwarded for convenience, in case people want to 
    # inspect them without having to get _data.
    @property
    def grouper(self):
        return self._data.grouper

    @property
    def index(self):
        return self._data.index

    @property
    def obj(self):
        return self._data.obj



# put methods onto FastGroupedSeries ----
for k, meth in local.items():
    setattr(FastGroupedSeries, k, _forward_method(meth))

del k, meth

# generic function to extract underlying data from grouped objects -----

from functools import singledispatch

@singledispatch
def extract(data):
    """Return a Series from a grouped data class."""
    return data.obj

@extract.register(GroupByAgg)
def _extract_gser(data):
    return data._broadcast_agg_result()

@extract.register(FastGroupedSeries)
def _extract_fgs(data):
    wrapped_cls = data._data.__class__
    f = extract.dispatch(wrapped_cls)
    return f(data._data)

