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

def not_implemented(name, is_property, accessor):
    return NotImplementedError


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
    def f(x: SeriesGroupBy, y: SeriesGroupBy) -> SeriesGroupBy:
        _validate_data_args(x, y = y)
        left, right, ref_groupby = broadcast_group_elements(x, y)
        
        op_function = getattr(left, name)

        res = op_function(right)
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




GROUP_METHODS = { 
        ("Elwise", 1): method_el_op,
        ("Elwise", 2): method_el_op2,
        ("Agg", 1): method_agg_op,
        ("Window", 1): method_win_op,
        ("Window", 2): method_win_op,
        ("Singleton", 1): method_agg_singleton,
        ("Todo", 1): not_implemented,
        ("Maydo", 1): not_implemented,
        ("Wontdo", 1): not_implemented,
        ("Todo", 2): not_implemented,
        ("Maydo", 2): not_implemented,
        ("Wontdo", 2): not_implemented,
        }



