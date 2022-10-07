from siuba.dply.verbs import arrange
from ..utils import lift_inner_cols
from ..backend import LazyTbl, _create_order_by_clause

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
    #exprs, _ = _mutate_cols(__data, args, kwargs, "Arrange", arrange_clause=True)

    new_calls = []
    for ii, expr in enumerate(args):
        if callable(expr):

            res = __data.shape_call(
                    expr, window = False,
                    verb_name = "Arrange", arg_name = ii
                    )

        else:
            res = expr

        new_calls.append(res)

    sort_cols = _create_order_by_clause(cols, *new_calls)

    order_by = __data.order_by + tuple(new_calls)
    return __data.append_op(last_sel.order_by(*sort_cols), order_by = order_by)
