import warnings

from collections.abc import Mapping
from sqlalchemy import sql
from siuba.dply.verbs import join, left_join, right_join, inner_join, semi_join, anti_join

from ..backend import LazyTbl
from ..utils import _sql_select


def _joined_cols(left_cols, right_cols, on_keys, how, suffix = ("_x", "_y")):
    """Return labeled columns, according to selection rules for joins.

    Rules:
        1. For join keys, keep left table's column
        2. When keys have the same labels, add suffix
    """

    # TODO: remove sets, so uses stable ordering
    # when left and right cols have same name, suffix with _x / _y
    keep_right = set(right_cols.keys()) - set(on_keys.values())
    shared_labs = set(left_cols.keys()).intersection(keep_right)

    right_cols_no_keys = {k: right_cols[k] for k in keep_right}

    # for an outer join, have key columns coalesce values

    left_cols = {**left_cols}
    if how == "full":
        for lk, rk in on_keys.items():
            col = sql.functions.coalesce(left_cols[lk], right_cols[rk])
            left_cols[lk] = col.label(lk)
    elif how == "right":
        for lk, rk in on_keys.items():
            # Make left key columns actually be right ones (which contain left + extra)
            left_cols[lk] = right_cols[rk].label(lk)


    # create labels ----
    l_labs = _relabeled_cols(left_cols, shared_labs, suffix[0])
    r_labs = _relabeled_cols(right_cols_no_keys, shared_labs, suffix[1])

    return l_labs + r_labs
    


def _relabeled_cols(columns, keys, suffix):
    # add a suffix to all columns with names in keys
    cols = []
    for k, v in columns.items():
        new_col = v.label(k + str(suffix)) if k in keys else v
        cols.append(new_col)
    return cols


@join.register(LazyTbl)
def _join(left, right, on = None, *args, by = None, how = "inner", sql_on = None):
    _raise_if_args(args)

    if on is None and by is not None:
        on = by

    # Needs to be on the table, not the select
    left_sel = left.last_op.alias()
    right_sel = right.last_op.alias()

    # handle arguments ----
    on  = _validate_join_arg_on(on, sql_on)
    how = _validate_join_arg_how(how)
    
    # for equality join used to combine keys into single column
    consolidate_keys = on if sql_on is None else {}
    
    if how == "right":
        # switch joins, since sqlalchemy doesn't have right join arg
        # see https://stackoverflow.com/q/11400307/1144523
        left_sel, right_sel = right_sel, left_sel
        on = {v:k for k,v in on.items()}

    # create join conditions ----
    bool_clause = _create_join_conds(left_sel, right_sel, on)

    # create join ----
    join = left_sel.join(
            right_sel,
            onclause = bool_clause,
            isouter = how != "inner",
            full = how == "full"
            )

    # if right join, set selects back
    if how == "right":
        left_sel, right_sel = right_sel, left_sel
        on = {v:k for k,v in on.items()}

    # note, shared_keys assumes on is a mapping...
    # TODO: shared_keys appears to be for when on is not specified, but was unused
    #shared_keys = [k for k,v in on.items() if k == v]
    labeled_cols = _joined_cols(
            left_sel.columns,
            right_sel.columns,
            on_keys = consolidate_keys,
            how = how
            )

    sel = _sql_select(labeled_cols).select_from(join)
    return left.append_op(sel, order_by = tuple())


@semi_join.register(LazyTbl)
def _semi_join(left, right = None, on = None, *args, by = None, sql_on = None):
    if on is None and by is not None:
        on = by

    _raise_if_args(args)

    left_sel = left.last_op.alias()
    right_sel = right.last_op.alias()

    # handle arguments ----
    on  = _validate_join_arg_on(on, sql_on, left_sel, right_sel)
    
    # create join conditions ----
    bool_clause = _create_join_conds(left_sel, right_sel, on)

    # create inner join ----
    exists_clause = _sql_select([sql.literal(1)]) \
            .select_from(right_sel) \
            .where(bool_clause)

    # only keep left hand select's columns ----
    sel = _sql_select(left_sel.columns) \
            .select_from(left_sel) \
            .where(sql.exists(exists_clause))

    return left.append_op(sel, order_by = tuple())


@anti_join.register(LazyTbl)
def _anti_join(left, right = None, on = None, *args, by = None, sql_on = None):
    if on is None and by is not None:
        on = by

    _raise_if_args(args)

    left_sel = left.last_op.alias()
    right_sel = right.last_op.alias()

    # handle arguments ----
    on  = _validate_join_arg_on(on, sql_on, left, right)
    
    # create join conditions ----
    bool_clause = _create_join_conds(left_sel, right_sel, on)

    # create inner join ----
    #not_exists = ~sql.exists([1], from_obj = right_sel).where(bool_clause)
    exists_clause = _sql_select([sql.literal(1)]) \
            .select_from(right_sel) \
            .where(bool_clause)

    sel = left_sel.select().where(~sql.exists(exists_clause))
    
    return left.append_op(sel, order_by = tuple())
       
def _raise_if_args(args):
    if len(args):
        raise NotImplemented("*args is reserved for future arguments (e.g. suffix)")

def _validate_join_arg_on(on, sql_on = None, lhs = None, rhs = None):
    # handle sql on case
    if sql_on is not None:
        if on is not None:
            raise ValueError("Cannot specify both on and sql_on")

        return sql_on

    # handle general cases
    if on is None:
        # TODO:  currently, we check for lhs and rhs tables to indicate whether
        #        a verb supports inferring columns. Otherwise, raise an error.
        if lhs is not None and rhs is not None:
            # TODO: consolidate with duplicate logic in pandas verb code
            warnings.warn(
                "No on column passed to join. "
                "Inferring join columns instead using shared column names."
            )

            on_cols = list(set(lhs.columns.keys()).intersection(set(rhs.columns.keys())))

            if not on_cols:
                raise ValueError(
                    "No join column specified, or shared column names in join."
                )

            # trivial dict mapping shared names to themselves
            warnings.warn("Detected shared columns: %s" % on_cols)
            on = dict(zip(on_cols, on_cols))

        else:
            raise NotImplementedError("on arg currently cannot be None (default) for SQL")
    elif isinstance(on, str):
        on = {on: on}
    elif isinstance(on, (list, tuple)):
        on = dict(zip(on, on))


    if not isinstance(on, Mapping):
        raise TypeError("on must be a Mapping (e.g. dict)")

    return on

def _validate_join_arg_how(how):
    how_options = ("inner", "left", "right", "full")
    if how not in how_options:
        raise ValueError("how argument needs to be one of %s" %how_options)
    
    return how

def _create_join_conds(left_sel, right_sel, on):
    left_cols  = left_sel.columns  #lift_inner_cols(left_sel)
    right_cols = right_sel.columns #lift_inner_cols(right_sel)

    if callable(on):
        # callable, like with sql_on arg
        conds = [on(left_cols, right_cols)]
    else:
        # dict-like of form {left: right}
        conds = []
        for l, r in on.items():
            col_expr = left_cols[l] == right_cols[r]
            conds.append(col_expr)
            
    return sql.and_(*conds)
