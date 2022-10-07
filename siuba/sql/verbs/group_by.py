from siuba.dply.verbs import group_by, ungroup

from ..backend import LazyTbl, ordered_union
from ..utils import lift_inner_cols

from .mutate import _mutate_cols


@group_by.register(LazyTbl)
def _group_by(__data, *args, add = False, **kwargs):
    if not (args or kwargs):
        return __data.copy()

    group_names, sel = _mutate_cols(__data, args, kwargs, "Group by")

    if None in group_names:
        raise NotImplementedError("Complex, unnamed expressions not supported in sql group_by")

    # check whether we can just use underlying table ----
    new_cols = lift_inner_cols(sel)
    if set(new_cols).issubset(set(__data.last_op.columns)):
        sel = __data.last_op

    if add:
        group_names = ordered_union(__data.group_by, group_names)

    return __data.append_op(sel, group_by = tuple(group_names))


@ungroup.register(LazyTbl)
def _ungroup(__data):
    return __data.copy(group_by = tuple())
