from .groupby import DataFrameGroupBy, GroupByAgg, SeriesGroupBy, broadcast_group_elements, _regroup
import pandas as pd


# utilities -------------------------------------------------------------------

def _validate_data_args(x, **kwargs):
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


def _maybe_broadcast(x, y):
    """Same as broadcast_group_elements, but y doesn't have to be SeriesGroupBy

    This is important when y is a literal (e.g. 1), since we don't want to raise
    an error, or broadcast 1 to the length of x. Rather, we want to keep the literal,
    and let the pandas series handle it in the operation.

    """
    if isinstance(y, SeriesGroupBy):
        left, right, groupby = broadcast_group_elements(x, y)
    else:
        left, right, groupby = x.obj, y, x

    return left, right, groupby


# Translations ----------------------------------------------------------------

def not_implemented(name, is_property, accessor):
    return NotImplementedError


def method_agg_op(name, is_property, accessor):
    def f(__ser, *args, **kwargs):
        _validate_data_args(__ser)
        res = _apply_grouped_method(__ser, name, is_property, accessor, args, kwargs)

        return GroupByAgg.from_result(res, __ser)

    f.__name__ = f.__qualname__ = name
    return f


def method_el_op(name, is_property, accessor):
    def f(__ser, *args, **kwargs):
        _validate_data_args(__ser)
        res = _apply_grouped_method(__ser.obj, name, is_property, accessor, args, kwargs)

        return _regroup(res, __ser)

    f.__name__ = f.__qualname__ = name
    return f


def method_el_op2(name, is_property, accessor):
    def f(x, y):
        _validate_data_args(x, y = y)
        left, right, groupby = _maybe_broadcast(x, y)
        
        op_function = getattr(left, name)

        res = op_function(right)
        return _regroup(res, groupby)

    f.__name__ = f.__qualname__ = name
    return f


def method_win_op(name, is_property, accessor):
    def f(__ser, *args, **kwargs):
        _validate_data_args(__ser)
        res = _apply_grouped_method(__ser, name, is_property, accessor, args, kwargs)

        return _regroup(res, __ser)

    f.__name__ = f.__qualname__ = name
    return f


GROUP_METHODS = { 
        ("Elwise", 1): method_el_op,
        ("Elwise", 2): method_el_op2,
        ("Agg", 1): method_agg_op,
        ("Window", 1): method_win_op,
        ("Window", 2): method_win_op,
        ("Singleton", 1): not_implemented,
        ("Todo", 1): not_implemented,
        ("Maydo", 1): not_implemented,
        ("Wontdo", 1): not_implemented,
        ("Todo", 2): not_implemented,
        ("Maydo", 2): not_implemented,
        ("Wontdo", 2): not_implemented,
        }



