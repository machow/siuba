from .groupby import DataFrameGroupBy, GroupByAgg, SeriesGroupBy, broadcast_group_elements, _regroup
import pandas as pd


def is_literal(el):
    # TODO: pandas has this function--should use that
    return isinstance(el, (int, float, str))

def not_implemented(name, is_property, accessor):
    return NotImplementedError

def method_agg_op(name, is_property, accessor):
    def f(__ser, *args, **kwargs):
        if not isinstance(__ser, SeriesGroupBy):
            raise TypeError("All methods must operate on grouped Series objects")
        
        method = getattr(__ser, name)

        res = method(*args, **kwargs)
        return GroupByAgg.from_result(res, __ser)

    f.__name__ = name
    f.__qualname__ = name
    return f

def method_el_op(name, is_property, accessor):
    def f(__ser, *args, **kwargs):
        if not isinstance(__ser, SeriesGroupBy):
            raise TypeError("All methods must operate on a grouped Series objects")
        
        if accessor:
            method = getattr(getattr(__ser.obj, accessor), name)
        else:
            method = getattr(__ser.obj, name)

        res = method(*args, **kwargs) if not is_property else method
        return _regroup(res, __ser)

    f.__name__ = name
    f.__qualname__ = name
    return f

def method_el_op2(name, **kwargs):
    def f(x, y):
        if isinstance(x, pd.Series) or isinstance(y, pd.Series):
            raise TypeError("No Series allowed")

        elif isinstance(x, SeriesGroupBy) and isinstance(y, SeriesGroupBy):
            left, right, groupby = broadcast_group_elements(x, y)
        elif is_literal(x):
            right, left, groupby = x, y.obj, y
        elif is_literal(y):
            left, right, groupby = x.obj, y, x
        else:
            raise TypeError("All methods must operate on a grouped Series objects")
        
        op_function = getattr(left, name)

        res = op_function(right)
        return _regroup(res, groupby)

    f.__name__ = name
    f.__qualname__ = name
    return f

GROUP_METHODS = { 
        ("Elwise", 1): method_el_op,
        ("Elwise", 2): method_el_op2,
        ("Agg", 1): method_agg_op,
        ("Window", 1): not_implemented,
        ("Window", 2): not_implemented,
        ("Singleton", 1): not_implemented
        }



