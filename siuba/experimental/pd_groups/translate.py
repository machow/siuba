"""Functions for creating fast versions of methods like .mean().

Examples:
    .. code-block:: python    

       from siuba.data import mtcars
       g_cyl = mtcars.groupby("cyl")

       avg_hp_raw = g_cyl.hp.mean()

       # imagine that avg_hp was not SeriesGroupBy, but a GroupByAgg object
       avg_hp = GroupByAgg.from_result(avg_hp_raw, g_cyl.hp)

       f_mean = method_agg_op("mean", is_property = False, accessor = None)
       f_mean(g_cyl.hp)        # returns GroupByAgg

       f_add = method_el_op2("__add__", is_property = False, accessor = None)
       f_add(g_cyl.hp, g_cyl.hp)
       f_add(f_mean(g_cyl.hp), g_cyl.hp)

       # property methods ----
       f_is_unique = method_el_op("is_unique", is_property = True, None)
       f_is_unique(g_cyl.hp)

       # accessor methods ----
       import pandas as pd
       gdf = pd.DataFrame({'g': ['a', 'b'], 'x': ['AA', 'BB']}).groupby('g')

       f_str_lower = method_el_op("lower", False, "str")
       f_str_lower(gdf.x)

"""

from siuba.siu import FunctionLookupBound
from .groupby import GroupByAgg, SeriesGroupBy, broadcast_group_elements, regroup

import pandas as pd


# utilities -------------------------------------------------------------------

def _validate_data_args(x, **kwargs):
    # Sanity checks that only SeriesGroupBy passed for ops
    # could be removed once this sees more use
    if not isinstance(x, SeriesGroupBy):
        raise TypeError("First data argument must be a grouped Series object")

    for name, other_data in kwargs.items():
        if isinstance(other_data, pd.Series):
            raise TypeError("{} may not be a Series.".format(name))


def _apply_grouped_method(ser, name, is_property, accessor, args, kwargs):
    if accessor:
        method = getattr(getattr(ser, accessor), name)
    else:
        method = getattr(ser, name)

    res = method(*args, **kwargs) if not is_property else method

    return res


# Translations ----------------------------------------------------------------

def not_implemented(name, *args, **kwargs):
    return FunctionLookupBound(name)


def method_agg_op(name, is_property, accessor):
    def f(__ser: SeriesGroupBy, *args, **kwargs) -> GroupByAgg:
        _validate_data_args(__ser)
        res = _apply_grouped_method(__ser, name, is_property, accessor, args, kwargs)

        return GroupByAgg.from_result(res, __ser)

    f.__name__ = f.__qualname__ = name
    return f


def method_el_op(name, is_property, accessor):
    def f(__ser: SeriesGroupBy, *args, **kwargs) -> SeriesGroupBy:
        _validate_data_args(__ser)
        res = _apply_grouped_method(__ser.obj, name, is_property, accessor, args, kwargs)

        return regroup(__ser, res)

    f.__name__ = f.__qualname__ = name
    return f


def method_el_op2(name, is_property, accessor):
    def f(x: SeriesGroupBy, y: SeriesGroupBy, *args, **kwargs) -> SeriesGroupBy:
        _validate_data_args(x, y = y)
        left, right, ref_groupby = broadcast_group_elements(x, y)
        
        op_function = getattr(left, name)

        res = op_function(right, *args, **kwargs)
        return regroup(ref_groupby, res)

    f.__name__ = f.__qualname__ = name
    return f


def method_win_op(name, is_property, accessor):
    def f(__ser: SeriesGroupBy, *args, **kwargs) -> SeriesGroupBy:
        _validate_data_args(__ser)
        res = _apply_grouped_method(__ser, name, is_property, accessor, args, kwargs)

        return regroup(__ser, res)

    f.__name__ = f.__qualname__ = name
    return f

def method_win_op_agg_result(name, is_property, accessor):
    def f(__ser, *args, **kwargs):
        _validate_data_args(__ser)
        res = _apply_grouped_method(__ser, name, is_property, accessor, args, kwargs)

        return GroupByAgg.from_result(res, __ser)

    f.__name__ = f.__qualname__ = name
    return f


def method_agg_singleton(name, is_property, accessor):
    def f(__ser: SeriesGroupBy, *args, **kwargs) -> SeriesGroupBy:
        _validate_data_args(__ser)
        if accessor is not None:
            op_function = getattr(getattr(__ser.obj, accessor, __ser.obj), name)
        else:
            op_function = getattr(__ser.obj, name)

        # cast singleton result to be GroupByAgg, as if we did an aggregation
        # could create a class to for grouped singletons, but seems like overkill
        # for now
        singleton = op_function if is_property else op_function()
        dtype = 'object' if singleton is None else None

        # note that when the value is None, need to explicitly make dtype object
        res = pd.Series(singleton, index = __ser.grouper.levels, dtype = dtype) 

        return GroupByAgg.from_result(res, __ser)
    return f


def forward_method(dispatcher, constructor = None, cls = SeriesGroupBy):
    op = dispatcher.operation
    kind = op.kind.title() if op.kind is not None else None
    key = (kind, op.arity)

    constructor = GROUP_METHODS[key] if constructor is None else constructor

    f_concrete = constructor(
            name = op.name,
            is_property = op.is_property,
            accessor = op.accessor
            )
    
    return dispatcher.register(cls, f_concrete)



# TODO: which methods can be pulled off GroupedDFs?
#       * all elwise in certain categories can be added (dt, str, etc..)
#       * some aggs / windows can be punted to SeriesGroupBy
#       * others require custom implementation

GROUP_METHODS = { 
        ("Elwise", 1): method_el_op,
        ("Elwise", 2): method_el_op2,
        ("Agg", 1): method_agg_op,
        ("Agg", 2): not_implemented,
        ("Window", 1): method_win_op,
        ("Window", 2): method_win_op,
        ("Singleton", 1): method_agg_singleton,
        ("Todo", 1): not_implemented,
        ("Maydo", 1): not_implemented,
        ("Wontdo", 1): not_implemented,
        ("Todo", 2): not_implemented,
        ("Maydo", 2): not_implemented,
        ("Wontdo", 2): not_implemented,
        (None, 1): not_implemented,
        (None, 2): not_implemented,
        }



