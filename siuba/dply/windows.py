import pandas as pd

from siuba.siu import symbolic_dispatch
from siuba.siu import strip_symbolic, Symbolic, Call

def _process_call_list(expr):
    if expr is None:
        return
    if isinstance(expr, Call):
        return [expr]
    elif isinstance(expr, (tuple, list)):
        return list(map(strip_symbolic, expr))
    else:
        raise NotImplementedError("Expected siu Call, tuple, or list. Received %s" % type(expr))


@symbolic_dispatch(cls = pd.DataFrame)
def win_over(__data, expr, partition = None, order = None, frame = None):

    order_args = _process_call_list(order)
    group_args = _process_call_list(partition)

    if order_args:
        df_sorted = arrange(__data, *order_args)
    else:
        df_sorted = __data

    if group_args:
        df_grouped = group_by(df_sorted, *group_args)
    else:
        df_grouped = df_sorted

    return expr(df_grouped) 


@symbolic_dispatch(cls = pd.Series)
def order_by(order_by, call):
    raise NotImplementedError()
