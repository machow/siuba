from siuba.dply.verbs import distinct, mutate, _var_select_simple

from ..backend import LazyTbl
from ..utils import _sql_select, lift_inner_cols


@distinct.register(LazyTbl)
def _distinct(__data, *args, _keep_all = False, **kwargs):
    if (args or kwargs) and _keep_all:
        raise NotImplementedError("Distinct with variables specified in sql requires _keep_all = False")
    
    inner_sel = mutate(__data, **kwargs).last_select if kwargs else __data.last_select

    # TODO: this is copied from the df distinct version
    # cols dict below is used as ordered set
    cols = _var_select_simple(args)
    cols.update(kwargs)

    # use all columns by default
    if not cols:
        cols = {k: True for k in lift_inner_cols(inner_sel).keys()}

    final_names = {**{k: True for k in __data.group_by}, **cols}

    if not len(inner_sel._order_by_clause):
        # select distinct has to include any columns in the order by clause,
        # so can only safely modify existing statement when there's no order by
        sel_cols = lift_inner_cols(inner_sel)
        distinct_cols = [sel_cols[k] for k in final_names]
        sel = inner_sel.with_only_columns(distinct_cols).distinct()
    else:
        # fallback to cte
        cte = inner_sel.alias()
        distinct_cols = [cte.columns[k] for k in final_names]
        sel = _sql_select(distinct_cols).select_from(cte).distinct()

    return __data.append_op(sel)
