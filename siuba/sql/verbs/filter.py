from siuba.dply.verbs import filter

from ..backend import LazyTbl
from ..translate import ColumnCollection
from ..utils import _sql_select

from sqlalchemy import sql
from siuba.siu import Call

from siuba.dply.across import _set_data_context


@filter.register(LazyTbl)
def _filter(__data, *args):
    # Note: currently always produces 2 additional select statements,
    #       1 for window/aggs, and 1 for the where clause

    sel = __data.last_op.alias()                   # original select
    win_sel = sel.select()

    conds = []
    windows = []
    with _set_data_context(__data, window=True):
        for ii, arg in enumerate(args):

            if isinstance(arg, Call):
                new_call = __data.shape_call(arg, verb_name = "Filter", arg_name = ii)
                #var_cols = new_call.op_vars(attr_calls = False)

                # note that a new win_sel is returned, w/ window columns appended
                col_expr, win_cols, win_sel = __data.track_call_windows(
                        new_call,
                        sel.columns,
                        window_cte = win_sel
                        )

                if isinstance(col_expr, ColumnCollection):
                    conds.extend(col_expr)
                else:
                    conds.append(col_expr)

                windows.extend(win_cols)
                
            else:
                conds.append(arg)

    bool_clause = sql.and_(*conds)

    # first cte, windows ----
    if len(windows):
        
        win_alias = win_sel.alias()

        # move non-window functions to refer to win_sel clause (not the innermost) ---
        bool_clause = sql.util.ClauseAdapter(win_alias) \
                .traverse(bool_clause)

        orig_cols = [win_alias.columns[k] for k in sel.columns.keys()]
    else:
        orig_cols = [sel]
    
    # create second cte ----
    filt_sel = _sql_select(orig_cols).where(bool_clause)
    return __data.append_op(filt_sel)
