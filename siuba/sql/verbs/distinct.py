from siuba.dply.verbs import distinct, mutate

from ..backend import LazyTbl, ordered_union
from ..utils import _sql_select, _sql_with_only_columns, lift_inner_cols

from .mutate import _mutate_cols


@distinct.register(LazyTbl)
def _distinct(__data, *args, _keep_all = False, **kwargs):
    if (args or kwargs) and _keep_all:
        raise NotImplementedError("Distinct with variables specified in sql requires _keep_all = False")
    
    result_names, inner_sel = _mutate_cols(__data, args, kwargs, "Distinct")

    # create list of final column names ----
    missing = [name for name in __data.group_by if name not in result_names]
    if not result_names:
        # use all columns if none passed to distinct
        all_names = list(lift_inner_cols(inner_sel).keys())
        final_names = ordered_union(missing, all_names)
    else:
        final_names = ordered_union(missing, result_names)


    if not (len(inner_sel._order_by_clause) or len(inner_sel._group_by_clause)):
        # select distinct has to include any columns in the order by clause,
        # so can only safely modify existing statement when there's no order by
        sel_cols = lift_inner_cols(inner_sel)
        distinct_cols = [sel_cols[k] for k in final_names]
        sel = _sql_with_only_columns(inner_sel, distinct_cols).distinct()
    else:
        # fallback to cte
        cte = inner_sel.alias()
        distinct_cols = [cte.columns[k] for k in final_names]
        sel = _sql_select(distinct_cols).select_from(cte).distinct()

    return __data.append_op(sel)
