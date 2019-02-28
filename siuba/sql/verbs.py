from siuba.tidy import (
        select, VarList, var_select,
        mutate,
        filter,
        arrange, _call_strip_ascending,
        summarize,
        count,
        group_by, ungroup,
        case_when,
        Pipeable,
        join, left_join, right_join, inner_join,
        head,
        rename
        )
from .translate import sa_modify_window, sa_is_window
from sqlalchemy import sql
from siuba.siu import strip_symbolic, Call, CallTreeLocal
from functools import singledispatch
# TODO: currently needed for select, but can we remove pandas?
from pandas import Series
import pandas as pd

from sqlalchemy.sql import schema


# TODO:
#   - summarize
#   - distinct
#   - head
#   - annotate functions using sel.prefix_with("\n/*<Mutate Statement>*/\n") ?


# Helpers ---------------------------------------------------------------------

class CallListener:
    def enter(self, node):
        args, kwargs = node.map_subcalls(self.enter)

        return self.exit(node.__class__(node.func, *args, **kwargs))

    def exit(self, node):
        return node


class WindowReplacer(CallListener):
    """Call tree listener.

    Produces 2 important behaviors via the enter method:
      - returns evaluated sql call expression, with labels on all window expressions.
      - stores all labeled window expressions via the windows property.
    """

    def __init__(self, columns, group_by, window_cte = None):
        self.columns = columns
        self.group_by = group_by
        self.window_cte = window_cte
        self.windows = []

    def exit(self, node):
        # evaluate
        col_expr = node(self.columns)
        if sa_is_window(col_expr):
            label = sa_modify_window(col_expr, self.columns, self.group_by).label(None)

            self.windows.append(label)

            if self.window_cte is not None:
                self.window_cte.append_column(label)
                win_col = self.window_cte.c.values()[-1]
                return win_col
                
            return label

        return col_expr


def track_call_windows(call, columns, group_by, window_cte = None):
    listener = WindowReplacer(columns, group_by, window_cte)
    col = listener.enter(call)
    return col, listener.windows


def lift_inner_cols(tbl):
    cols = list(tbl.inner_columns)
    data = {col.key: col for col in cols}

    return sql.base.ImmutableColumnCollection(data, cols)

def get_labeled_cols(cols):
    return set(k for k,v in cols.items() if isinstance(v, sql.elements.Label))


def is_grouped_sel(select):
    return False

def has_windows(clause):
    windows = []
    append_win = lambda col: windows.append(col)

    sql.util.visitors.traverse(clause, {}, {"over": append_win})
    if len(windows):
        return True

    return False

def compile_el(tbl, el):
    compiled = el.compile(
         dialect = tbl.source.dialect,
         compile_kwargs = {"literal_binds": True}
    )
    return compiled




# Table -----------------------------------------------------------------------

class LazyTbl:
    def __init__(
            self, source, tbl, ops = None,
            group_by = tuple(), order_by = tuple(), funcs = None,
            CallShaper = CallTreeLocal
            ):
        self.source = source
        self.tbl = tbl
        self.ops = [sql.Select([tbl])] if ops is None else ops
        self.group_by = group_by
        self.order_by = order_by
        self.funcs = {} if funcs is None else funcs
        self.CallShaper = CallShaper

    def append_op(self, op):
        return self.__class__(
                self.source,
                self.tbl,
                self.ops + [op],
                self.group_by,
                self.order_by,
                self.funcs,
                self.CallShaper
                )

    def copy(self, **kwargs):
        return self.__class__(**{**self.__dict__, **kwargs})

    def shape_call(self, call, window = True):
        f_dict1 = self.funcs['scalar']
        f_dict2 = self.funcs['window' if window else 'aggregate']

        funcs = {**f_dict1, **f_dict2}
        cs = self.CallShaper(funcs)
        return cs.visit(call)

    def track_call_windows(self, call, columns = None, window_cte = None):
        """Returns tuple of (new column expression, list of window exprs)"""

        columns = self.last_op.columns if columns is None else columns
        return track_call_windows(call, columns, self.group_by, window_cte)

    @property
    def last_op(self):
        return self.ops[-1] if len(self.ops) else None


# Main Funcs 
# =============================================================================

@Pipeable.add_to_dispatcher
@singledispatch
def show_query(tbl):
    query = tbl.last_op
    compiled = query.compile(
        dialect = tbl.source.dialect,
        compile_kwargs = {"literal_binds": True}
    )
    print(compiled)

    return tbl

@Pipeable.add_to_dispatcher
@singledispatch
def collect(tbl, as_df = True):
    # TODO: maybe remove as_df options, always return dataframe
    # normally can just pass the sql objects to execute, but for some reason
    # psycopg2 completes about incomplete template.
    # see https://stackoverflow.com/a/47193568/1144523
    query = tbl.last_op
    compiled = query.compile(
        dialect = tbl.source.dialect,
        compile_kwargs = {"literal_binds": True}
    )
    if as_df:
        return pd.read_sql(compiled, tbl.source)

    return tbl.source.execute(compiled).fetchall()


@select.register(LazyTbl)
def _(__data, *args, **kwargs):
    # see https://stackoverflow.com/questions/25914329/rearrange-columns-in-sqlalchemy-select-object
    last_op = __data.last_op
    columns = {c.key: c for c in last_op.inner_columns}

    # same as for DataFrame
    colnames = Series(list(columns))
    vl = VarList()
    evaluated = (strip_symbolic(arg)(vl) if callable(arg) else arg for arg in args)
    od = var_select(colnames, *evaluated)

    col_list = []
    for k,v in od.items():
        col = columns[k]
        col_list.append(col if v is None else col.label(v))

    return __data.append_op(last_op.with_only_columns(col_list))



@filter.register(LazyTbl)
def _(__data, *args, **kwargs):
    # TODO: aggregate funcs
    # Note: currently always produces 2 additional select statements,
    #       1 for window/aggs, and 1 for the where clause
    sel = __data.last_op.alias()
    win_sel = sql.select([sel], from_obj = sel)
    #fil_sel = sql.select([win_sel], from_obj = win_sel)

    conds = []
    windows = []
    for arg in map(strip_symbolic, args):
        if isinstance(arg, Call):
            new_call = __data.shape_call(arg)
            var_cols = new_call.op_vars(attr_calls = False)

            col_expr, win_cols = __data.track_call_windows(
                    new_call,
                    sel.columns,
                    window_cte = win_sel
                    )

            #if sa_is_window(col_expr):
            #    col_expr = sa_modify_window(col_expr, columns, __data.group_by)
            conds.append(col_expr)
        else:
            conds.append(arg)

    bool_clause = sql.and_(*conds)

    # move non-window functions to refer to win_sel clause (not the innermost)
    win_alias = win_sel.alias()
    bool_clause = sql.util.ClauseAdapter(win_alias).traverse(bool_clause)


    sel = sql.select([win_alias], from_obj = win_alias, whereclause = bool_clause)
    return __data.append_op(sel)


@mutate.register(LazyTbl)
def _(__data, **kwargs):
    # Cases
    #  - work with group by
    #  - window functions
    # TODO: verify it can follow a renaming select

    # track labeled columns in set
    sel = __data.last_op
    labs = set(k for k,v in sel.columns.items() if isinstance(v, sql.elements.Label))

    # evaluate each call
    for colname, func in kwargs.items():
        strip_f = strip_symbolic(func)
        new_call = __data.shape_call(strip_f)

        sel = _mutate_select(sel, colname, new_call, labs, __data)
        labs.add(colname)

    return __data.append_op(sel)

    raise NotImplementedError("Must be select statement")


def _mutate_select(sel, colname, func, labs, __data):
    """Return select statement containing a new column, func expr as colname.
    
    Note: since a func can refer to previous columns generated by mutate, this
    function handles whether to add a column to the existing select statement,
    or to use it as a subquery.
    """
    #colname, func
    replace_col = colname in sel.columns
    strip_f = strip_symbolic(func)
    # Call objects let us check whether column expr used a derived column
    # e.g. SELECT a as b, b + 1 as c raises an error in SQL, so need subquery
    call_vars = strip_f.op_vars(attr_calls = False)
    if isinstance(strip_f, Call) and labs.isdisjoint(call_vars):
        # New column may be able to modify existing select
        columns = lift_inner_cols(sel)
        # replacing an existing column, so strip it from select statement
        if replace_col:
            sel = sel.with_only_columns([v for k,v in columns.items() if k != colname])

    else:
        # anything else requires a subquery
        cte = sel.alias(None)
        columns = cte.columns
        sel = sql.select([cte], from_obj = cte)

    # evaluate call expr on columns, making sure to use group vars
    new_col, windows = __data.track_call_windows(strip_f, columns)

    #if sa_is_window(new_col) or hasattr(new_col, "over"):
    #    new_col = sa_modify_window(new_col, columns, group_by)

    return sel.column(new_col.label(colname))


@arrange.register(LazyTbl)
def _(__data, *args):
    last_op = __data.last_op
    cols = lift_inner_cols(last_op)

    sort_cols = []
    for arg in args:
        # simple named column
        if isinstance(arg, str):
            sort_cols.append(cols[arg])
        # an expression
        elif callable(arg):
            f, asc = _call_strip_ascending(arg)
            col_op = f(cols) if asc else f(cols).desc()
            sort_cols.append(col_op)
        else:
            raise NotImplementedError("Must be string or callable")

    return __data.append_op(last_op.order_by(*sort_cols))


@count.register(LazyTbl)
def _(__data, *args, sort = False):
    # TODO: if already col named n, use name nn, etc.. get logic from tidy.py
    # similar to filter verb, we need two select statements,
    # an inner one for derived cols, and outer to group by them
    sel = __data.last_op.alias()
    sel_inner = sql.select([sel], from_obj = sel)

    # since we can't append columns to an alias, we need to add them to 
    # sel_inner below, then alias it after the for loop
    group_cols = []
    for arg in args:
        strip_f = strip_symbolic(arg)
        col_expr = strip_f(sel.columns) if callable(strip_f) else strip_f
        if not isinstance(col_expr, schema.Column):
            # compile, so we can use the expr as its name (e.g. "id + 1")
            name = str(compile_el(__data, col_expr))
            label = col_expr.label(name)
            sel_inner.append_column(label)
        else:
            name = str(col_expr)

        group_cols.append(name)

    # now that inner has all needed columns, build outer
    sel_inner_cte = sel_inner.alias()
    inner_cols = sel_inner_cte.columns
    sel_outer = sql.select(from_obj = sel_inner_cte)

    # apply any group vars from a group_by verb call first
    prev_group_cols = [inner_cols[k] for k in __data.group_by]
    sel_outer.append_group_by(*prev_group_cols)
    sel_outer.append_column(*prev_group_cols)

    # now any defined in the count verb call
    for k in group_cols:
        sel_outer.append_group_by(inner_cols[name])
        sel_outer.append_column(inner_cols[name])

    sel_outer.append_column(sql.functions.count().label("n"))

    return __data.append_op(sel_outer)
    


@summarize.register(LazyTbl)
def _(__data, **kwargs):
    # https://stackoverflow.com/questions/14754994/why-is-sqlalchemy-count-much-slower-than-the-raw-query
    # what if windowed mutate or filter has been done? 
    #   - filter is fine, since it uses a CTE
    #   - need to detect any window functions...
    sel = __data.last_op._clone()
    labs = set(k for k,v in sel.columns.items() if isinstance(v, sql.elements.Label))

    if len(sel._group_by_clause):
        # current select stmt has window functions, so need to make it subquery
        cte = sel.alias()
        columns = cte.columns
        sel = sql.select(from_obj = cte)
    else:
        # otherwise, can alter the existing select statement
        columns = lift_inner_cols(sel)
        sel = sel.with_only_columns([])

    # add group by columns to statement
    group_cols = [columns[k] for k in __data.group_by]
    sel.append_group_by(*group_cols)
    for col in group_cols:
        sel.append_column(col)

    # add each aggregate column
    # TODO: can't do summarize(b = mean(a), c = b + mean(a))
    #       since difficult for c to refer to agg and unagg cols in SQL
    for k, expr in kwargs.items():
        strip_f = strip_symbolic(expr)
        new_call = __data.shape_call(strip_f, window = False)
        col = new_call(columns)

        sel.append_column(col)

    # TODO: is a simple method on __data for doing this...
    new_data = __data.append_op(sel)
    new_data.group_by = None
    return new_data


@group_by.register(LazyTbl)
def _(__data, *args):
    return __data.copy(group_by = args)

@ungroup.register(LazyTbl)
def _(__data):
    return __data.copy(group_by = None)


@case_when.register(sql.base.ImmutableColumnCollection)
def _(__data, cases):
    # TODO: will need listener to enter case statements, to handle when they use windows
    if isinstance(cases, Call):
        cases = cases(__data)

    whens = []
    case_items = list(cases.items())
    n_items = len(case_items)

    else_val = None
    for ii, (expr, val) in enumerate(case_items):
        # handle where val is a column expr
        if callable(val):
            val = val(__data)

        # handle when expressions
        if ii+1 == n_items and expr is True:
            else_val = val
        elif callable(expr):
            whens.append((expr(__data), val))
        else:
            whens.append((expr, val))

    return sql.case(whens, else_ = else_val)
        

# Join ------------------------------------------------------------------------

from collections.abc import Mapping

def _joined_cols(left_cols, right_cols):
    # when left and right cols have same name, suffix with _x / _y
    shared_labs = set(left_cols.keys()).intersection(right_cols.keys())

    l_labs = _relabeled_cols(left_cols, shared_labs, "_x")
    r_labs = _relabeled_cols(right_cols, shared_labs, "_y")

    return l_labs + r_labs
    


def _relabeled_cols(columns, keys, suffix):
    # add a suffix to all columns with names in keys
    cols = []
    for k, v in columns.items():
        new_col = v.label(k + str(suffix)) if k in keys else v
        cols.append(new_col)
    return cols


@join.register(LazyTbl)
def _(left, right, on = None, how = None):
    # Needs to be on the table, not the select
    left_sel = left.last_op.alias()
    right_sel = right.last_op.alias()
    
    if on is None:
        raise NotImplementedError("on arg must currently be dict")

    if isinstance(on, Mapping):
        left_cols  = left_sel.columns  #lift_inner_cols(left_sel)
        right_cols = right_sel.columns #lift_inner_cols(right_sel)

        conds = []
        for l, r in on.items():
            col_expr = left_cols[l] == right_cols[r]
            conds.append(col_expr)

    bool_clause = sql.and_(*conds)
    join = left_sel.join(right_sel, onclause = bool_clause)
    
    labeled_cols = _joined_cols(left_sel.columns, right_sel.columns)
    sel = sql.select(labeled_cols, from_obj = join)
    return left.append_op(sel)


# Head ------------------------------------------------------------------------

@head.register(LazyTbl)
def _(__data, n):
    sel = __data.last_op
    
    return __data.append_op(sel.limit(n))


# Rename-- --------------------------------------------------------------------

@rename.register(LazyTbl)
def _(__data, **kwargs):
    sel = __data.last_op
    columns = lift_inner_cols(sel)

    old_keys = set(kwargs.values())
    old_labs = [columns[k] for k in old_keys]
    new_labs = [columns[old].label(new) for new, old in kwargs.items()]

    new_sel = sel.with_only_columns(old_labs)
    new_sel.append_column(*new_labs)

    return __data.append_op(new_sel)


