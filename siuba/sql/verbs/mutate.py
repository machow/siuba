from siuba.dply.verbs import (
        simple_varname,
        mutate,
        transmute,
        )

from ..backend import LazyTbl, SqlLabelReplacer
from ..utils import (
    _sql_with_only_columns,
    lift_inner_cols
)

from sqlalchemy import sql
# TODO: currently needed for select, but can we remove pandas?

from siuba.dply.across import _require_across, _eval_with_context


@mutate.register(LazyTbl)
def _mutate(__data, *args, **kwargs):
    # TODO: verify it can follow a renaming select

    # track labeled columns in set
    if not (len(args) or len(kwargs)):
        return __data.append_op(__data.last_op)

    names, sel_out = _mutate_cols(__data, args, kwargs, "Mutate")
    return __data.append_op(sel_out)


def _sql_upsert_columns(sel, new_columns: "list[base.Label | base.Column]"):
    orig_cols = lift_inner_cols(sel)
    replaced = {**orig_cols}

    for new_col in new_columns:
        replaced[new_col.name] = new_col
    return _sql_with_only_columns(sel, list(replaced.values()))


def _select_mutate_result(src_sel, expr_result):
    dst_alias = src_sel.alias()
    src_columns = set(lift_inner_cols(src_sel))
    replacer = SqlLabelReplacer(set(src_columns), dst_alias.columns)

    if isinstance(expr_result, sql.base.ImmutableColumnCollection):
        replaced_cols = list(map(replacer, expr_result))
        orig_cols = expr_result
    #elif isinstance(expr_result, None):
    #    pass
    else:
        replaced_cols = [replacer(expr_result)]
        orig_cols = [expr_result]

    if replacer.applied:
        return _sql_upsert_columns(dst_alias.select(), replaced_cols)

    return _sql_upsert_columns(src_sel, orig_cols)


def _eval_expr_arg(__data, sel, func, verb_name, window=True):
    inner_cols = lift_inner_cols(sel)

    # case 1: simple names ----
    simple_name = simple_varname(func)
    if simple_name is not None:
        return inner_cols[simple_name]

    # case 2: across ----
    _require_across(func, verb_name)

    cols_result = _eval_with_context(__data, window, inner_cols, func)

    # TODO: remove or raise a more informative error
    assert isinstance(cols_result, sql.base.ImmutableColumnCollection), type(cols_result)

    return cols_result


def _eval_expr_kwarg(__data, sel, func, new_name, verb_name, window=True):
    inner_cols = lift_inner_cols(sel)

    expr_shaped = __data.shape_call(func, window, verb_name = verb_name, arg_name = new_name)
    new_col, windows, _ = __data.track_call_windows(expr_shaped, inner_cols)

    if isinstance(new_col, sql.base.ImmutableColumnCollection):
        raise TypeError(
            f"{verb_name} named arguments must return a single column, but `{new_name}` "
            "returned multiple columns."
        )

    return new_col.label(new_name)


def _mutate_cols(__data, args, kwargs, verb_name):
    result_names = {}     # used as ordered set
    sel = __data.last_select

    for ii, func in enumerate(args):
        cols_result = _eval_expr_arg(__data, sel, func, verb_name)

        # replace any labels that require a subquery ----
        sel = _select_mutate_result(sel, cols_result)

        if isinstance(cols_result, sql.base.ImmutableColumnCollection):
            result_names.update({k: True for k in cols_result.keys()})
        else:
            result_names[cols_result.name] = True

    
    for new_name, func in kwargs.items():
        labeled = _eval_expr_kwarg(__data, sel, func, new_name, verb_name)

        sel = _select_mutate_result(sel, labeled)
        result_names[new_name] = True


    return list(result_names), sel


@transmute.register(LazyTbl)
def _transmute(__data, *args, **kwargs):
    # will use mutate, then select some cols
    result_names, sel = _mutate_cols(__data, args, kwargs, "Transmute")

    # transmute keeps grouping cols, and any defined in kwargs
    missing = [x for x in __data.group_by if x not in result_names]
    cols_to_keep = [*missing, *result_names]

    columns = lift_inner_cols(sel)
    sel_stripped = sel.with_only_columns([columns[k] for k in cols_to_keep])

    return __data.append_op(sel_stripped)
