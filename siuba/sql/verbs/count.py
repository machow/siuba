"""
Implements LazyTbl to represent tables of SQL data, and registers it on verbs.

This module is responsible for the handling of the "table" side of things, while
translate.py handles translating column operations.


"""

from sqlalchemy import sql

from siuba.dply.verbs import count, add_count, inner_join, _check_name

from ..utils import _sql_select, _sql_add_columns, lift_inner_cols
from ..backend import LazyTbl, ordered_union
from ..translate import AggOver

from .mutate import _mutate_cols


@count.register(LazyTbl)
def _count(__data, *args, sort = False, wt = None, name=None, **kwargs):
    if wt is not None:
        raise NotImplementedError("wt argument is currently not implemented")

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
    label_n = _check_name(name, set(inner_cols.keys()))
    count_col = sql.functions.count().label(label_n)

    sel_outer = _sql_select([*outer_group_cols, count_col]) \
            .select_from(sel_inner_cte) \
            .group_by(*outer_group_cols)

    # count is like summarize, so removes order_by
    return __data.append_op(
            sel_outer.order_by(count_col.desc()),
            order_by = tuple()
            )


@add_count.register(LazyTbl)
def _add_count(__data, *args, wt = None, sort = False, name=None, **kwargs):
    if wt is not None:
        raise NotImplementedError("wt argument is currently not implemented")

    result_names, sel_inner = _mutate_cols(__data, args, kwargs, "Count")

    # TODO: if clause copied from count
    # remove unnecessary select, if we're operating on a table ----
    if set(lift_inner_cols(sel_inner)) == set(lift_inner_cols(__data.last_select)):
        sel_inner = __data.last_select

    inner_cols = lift_inner_cols(sel_inner)


    # TODO: this code to append groups to columns copied a lot inside verbs
    # apply any group vars from a group_by verb call first
    missing = [k for k in __data.group_by if k not in result_names]

    all_group_names = ordered_union(__data.group_by, result_names)
    outer_group_cols = [inner_cols[k] for k in all_group_names]


    count_col = AggOver(sql.functions.count(), partition_by=outer_group_cols)
    label_n = _check_name(name, set(inner_cols.keys()))

    sel_appended = _sql_add_columns(sel_inner, [count_col.label(label_n)])

    return __data.append_op(sel_appended)
