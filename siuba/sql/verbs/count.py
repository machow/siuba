"""
Implements LazyTbl to represent tables of SQL data, and registers it on verbs.

This module is responsible for the handling of the "table" side of things, while
translate.py handles translating column operations.


"""

from sqlalchemy import sql

from siuba.dply.verbs import count, add_count, inner_join

from ..utils import _sql_select, lift_inner_cols
from ..backend import LazyTbl, ordered_union

from .mutate import _mutate_cols



@count.register(LazyTbl)
def _count(__data, *args, sort = False, wt = None, **kwargs):
    # TODO: if already col named n, use name nn, etc.. get logic from tidy.py
    if wt is not None:
        raise NotImplementedError("TODO")

    res_name = "n"
    # similar to filter verb, we need two select statements,
    # an inner one for derived cols, and outer to group by them

    # inner select ----
    # holds any mutation style columns
    #arg_names = []
    #for arg in args:
    #    name = simple_varname(arg)
    #    if name is None:
    #        raise NotImplementedError(
    #                "Count positional arguments must be single column name. "
    #                "Use a named argument to count using complex expressions."
    #                )
    #    arg_names.append(name)

    result_names, sel_inner = _mutate_cols(__data, args, kwargs, "Count")

    # remove unnecessary select, if we're operating on a table ----
    if set(lift_inner_cols(sel_inner)) == set(lift_inner_cols(__data.last_select)):
        sel_inner = __data.last_op

    # create outer select ----
    # holds selected columns and tally (n)
    sel_inner_cte = sel_inner.alias()
    inner_cols = sel_inner_cte.columns

    # apply any group vars from a group_by verb call first
    missing = [k for k in __data.group_by if k not in result_names]

    all_group_names = ordered_union(__data.group_by, result_names)
    outer_group_cols = [inner_cols[k] for k in all_group_names]

    # holds the actual count (e.g. n)
    count_col = sql.functions.count().label(res_name)

    sel_outer = _sql_select([*outer_group_cols, count_col]) \
            .select_from(sel_inner_cte) \
            .group_by(*outer_group_cols)

    # count is like summarize, so removes order_by
    return __data.append_op(
            sel_outer.order_by(count_col.desc()),
            order_by = tuple()
            )


@add_count.register(LazyTbl)
def _add_count(__data, *args, wt = None, sort = False, **kwargs):
    counts = count(__data, *args, wt = wt, sort = sort, **kwargs)
    by = list(c.name for c in counts.last_select.inner_columns)[:-1]

    return inner_join(__data, counts, by = by)
