from siuba.dply.verbs import arrange, _call_strip_ascending
from siuba.dply.across import _set_data_context

from ..utils import lift_inner_cols
from ..backend import LazyTbl
from ..translate import ColumnCollection

# Helpers ---------------------------------------------------------------------

@arrange.register(LazyTbl)
def _arrange(__data, *args):
    # Note that SQL databases often do not subquery order by clauses. Arrange
    # sets order_by on the backend, so it can set order by in over elements,
    # and handle when new columns are named the same as order by vars.
    # see: https://dba.stackexchange.com/q/82930

    last_sel = __data.last_select
    cols = lift_inner_cols(last_sel)

    # TODO: implement across in arrange
    sort_cols = _eval_arrange_args(__data, args, cols)

    final_sel = last_sel.order_by(None).order_by(*sort_cols)
    return __data.append_op(final_sel, order_by = tuple(args))


def _eval_arrange_args(__data, args, cols):
    sort_cols = []
    for ii, expr in enumerate(args):
        shaped = __data.shape_call(
                expr, window = False, str_accessors = True,
                verb_name = "Arrange", arg_name = ii,
                )
        
        new_call, ascending = _call_strip_ascending(shaped)

        with _set_data_context(__data, window=True):
            res = new_call(cols)

        if isinstance(res, ColumnCollection):
            raise NotImplementedError(
                f"`arrange()` expression {ii} of {len(args)} returned multiple columns, "
                "which is currently unsupported."
            )

        if not ascending:
            res = res.desc()

        sort_cols.append(res)

    return sort_cols

