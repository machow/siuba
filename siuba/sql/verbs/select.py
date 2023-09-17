from siuba.dply.verbs import select, rename, _select_group_renames 
from siuba.dply.tidyselect import VarList, var_select
from siuba.dply.verbs import simple_varname

from pandas import Series

from ..backend import LazyTbl, _warn_missing
from ..utils import lift_inner_cols, _sql_with_only_columns


@select.register(LazyTbl)
def _select(__data, *args, **kwargs):
    # see https://stackoverflow.com/questions/25914329/rearrange-columns-in-sqlalchemy-select-object
    if kwargs:
        raise NotImplementedError(
                "Using kwargs in select not currently supported. "
                "Use _.newname == _.oldname instead"
                )
    last_sel = __data.last_select
    columns = {c.key: c for c in last_sel.inner_columns}

    # same as for DataFrame
    colnames = Series(list(columns))
    vl = VarList()
    evaluated = (arg(vl) if callable(arg) else arg for arg in args)
    od = var_select(colnames, *evaluated)

    missing_groups, group_keys = _select_group_renames(od, __data.group_by)

    if missing_groups:
        _warn_missing(missing_groups)

    final_od = {**{k: None for k in missing_groups}, **od}

    col_list = []
    for k,v in final_od.items():
        col = columns[k]
        col_list.append(col if v is None else col.label(v))

    return __data.append_op(
        _sql_with_only_columns(last_sel, col_list),
        group_by = group_keys
    )


@rename.register(LazyTbl)
def _rename(__data, **kwargs):
    sel = __data.last_select
    columns = lift_inner_cols(sel)

    # old_keys uses dict as ordered set
    old_to_new = {simple_varname(v):k for k,v in kwargs.items()}
    
    if None in old_to_new:
        raise KeyError("positional arguments must be simple column, "
                        "e.g. _.colname or _['colname']"
                        )

    labs = [c.label(old_to_new[k]) if k in old_to_new else c for k,c in columns.items()]

    new_sel = _sql_with_only_columns(sel, labs)

    missing_groups, group_keys = _select_group_renames(old_to_new, __data.group_by)

    return __data.append_op(new_sel, group_by=group_keys)


