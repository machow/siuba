from siuba.dply.verbs import (
        singledispatch2,
        show_query, collect,
        simple_varname,
        select, VarList, var_select,
        mutate,
        transmute,
        filter,
        arrange, _call_strip_ascending,
        summarize,
        count,
        group_by, ungroup,
        case_when,
        join, left_join, right_join, inner_join, semi_join, anti_join,
        head,
        rename,
        distinct,
        if_else
        )
from .translate import sa_modify_window, sa_is_window, CustomOverClause
from .utils import get_dialect_funcs

from sqlalchemy import sql
import sqlalchemy
from siuba.siu import Call, CallTreeLocal, str_to_getitem_call, Lazy, FunctionLookupError
# TODO: currently needed for select, but can we remove pandas?
from pandas import Series
import pandas as pd

from sqlalchemy.sql import schema


# TODO:
#   - distinct
#   - annotate functions using sel.prefix_with("\n/*<Mutate Statement>*/\n") ?


# Helpers ---------------------------------------------------------------------

class SqlFunctionLookupError(FunctionLookupError): pass


class CallListener:
    """Generic listener. Each exit is called on a node's copy."""
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

    TODO: could replace with a sqlalchemy transformer
    """

    def __init__(self, columns, group_by, order_by, window_cte = None):
        self.columns = columns
        self.group_by = group_by
        self.order_by = order_by
        self.window_cte = window_cte
        self.windows = []

    def exit(self, node):
        # evaluate
        col_expr = node(self.columns)
        if isinstance(col_expr, CustomOverClause):
            group_by = sql.elements.ClauseList(
                    *[self.columns[name] for name in self.group_by]
                    )
            order_by = sql.elements.ClauseList(
                    *_create_order_by_clause(self.columns, *self.order_by)
                    )

            label = col_expr.set_over(group_by, order_by).label(None)
            #label = sa_modify_window(col_expr, self.columns, self.group_by).label(None)

            self.windows.append(label)

            if self.window_cte is not None:
                self.window_cte.append_column(label)
                win_col = self.window_cte.c.values()[-1]
                return win_col
                
            return label

        return col_expr


def track_call_windows(call, columns, group_by, order_by, window_cte = None):
    listener = WindowReplacer(columns, group_by, order_by, window_cte)
    col = listener.enter(call)
    return col, listener.windows


def lift_inner_cols(tbl):
    cols = list(tbl.inner_columns)
    data = {col.key: col for col in cols}

    return sql.base.ImmutableColumnCollection(data, cols)

def col_expr_requires_cte(call, sel, is_mutate = False):
    """Return whether a variable assignment needs a CTE"""

    call_vars = set(call.op_vars(attr_calls = False))

    columns = lift_inner_cols(sel)
    sel_labs = set(k for k,v in columns.items() if isinstance(v, sql.elements.Label))

    # I use the acronym fwg sol (frog soul) to remember sql clause eval order
    # from, where, group by, select, order by, limit
    # group clause evaluated before select clause, so not issue for mutate
    group_needs_cte = not is_mutate and len(sel._group_by_clause)
    
    return (   group_needs_cte
            or len(sel._order_by_clause)
            or not sel_labs.isdisjoint(call_vars)
            )

def get_missing_columns(call, columns):
    missing_cols = set(call.op_vars(attr_calls = False)) - set(columns.keys())
    return missing_cols

def compile_el(tbl, el):
    compiled = el.compile(
         dialect = tbl.source.dialect,
         compile_kwargs = {"literal_binds": True}
    )
    return compiled

# Misc utilities --------------------------------------------------------------

def ordered_union(x, y):
    dx = {el: True for el in x}
    dy = {el: True for el in y}

    return tuple({**dx, **dy})




# Table -----------------------------------------------------------------------

class LazyTbl:
    def __init__(
            self, source, tbl, columns = None,
            ops = None, group_by = tuple(), order_by = tuple(), funcs = None,
            rm_attr = ('str', 'dt'), call_sub_attr = ('dt',)
            ):
        """Create a representation of a SQL table.

        Args:
            source: a sqlalchemy.Engine or sqlalchemy.Connection instance.
            tbl: table of form 'schema_name.table_name', 'table_name', or sqlalchemy.Table.
            columns: if specified, a listlike of column names.

        Examples
        --------

        ::
            from sqlalchemy import create_engine
            from siuba.data import mtcars

            # create database and table
            engine = create_engine("sqlite:///:memory:")
            mtcars.to_sql('mtcars', engine)

            tbl_mtcars = LazyTbl(engine, 'mtcars')
            
        """
        
        # connection and dialect specific functions
        self.source = sqlalchemy.create_engine(source) if isinstance(source, str) else source
        self.funcs = get_dialect_funcs(self.source.dialect.name) if funcs is None else funcs

        self.tbl = self._create_table(tbl, columns, self.source)

        # important states the query can be in (e.g. grouped)
        self.ops = [sql.Select([self.tbl])] if ops is None else ops

        self.group_by = group_by
        self.order_by = order_by

        # customizations to allow interop with pandas (e.g. handle dt methods)
        self.rm_attr = rm_attr
        self.call_sub_attr = call_sub_attr


    def append_op(self, op, **kwargs):
        cpy = self.copy(**kwargs)
        cpy.ops = cpy.ops + [op]
        return cpy

    def copy(self, **kwargs):
        return self.__class__(**{**self.__dict__, **kwargs})

    def shape_call(
            self,
            call, window = True, str_accessors = False,
            verb_name = None, arg_name = None
            ):
        # TODO: error if mutate receives a literal value?
        if str_accessors and isinstance(call, str):
            # verbs that can use strings as accessors, like group_by, or
            # arrange, need to convert those strings into a getitem call
            return str_to_get_item_call(call)
        elif isinstance(call, sql.elements.ColumnClause):
            return Lazy(call)
        elif not isinstance(call, Call):
            # verbs that use literal strings, need to convert them to a call
            # that returns a sqlalchemy "literal" object
            return Lazy(sql.literal(call))


        f_dict1 = self.funcs['scalar']
        f_dict2 = self.funcs['window' if window else 'aggregate']

        funcs = {**f_dict1, **f_dict2}
        call_shaper = CallTreeLocal(
                funcs,
                call_sub_attr = self.call_sub_attr
                )

        # raise informative error message if missing translation
        try:
            return call_shaper.enter(call)
        except FunctionLookupError as err:
            raise SqlFunctionLookupError.from_verb(
                    verb_name or "Unknown",
                    arg_name or "Unknown",
                    err,
                    short = True
                    )


    def track_call_windows(self, call, columns = None, window_cte = None):
        """Returns tuple of (new column expression, list of window exprs)"""

        columns = self.last_op.columns if columns is None else columns
        return track_call_windows(call, columns, self.group_by, self.order_by, window_cte)

    def get_ordered_col_names(self):
        ungrouped = [k for k in self.last_op.columns.keys() if k not in self.group_by]
        return list(self.group_by) + ungrouped

    @property
    def last_op(self):
        return self.ops[-1] if len(self.ops) else None

    @staticmethod
    def _create_table(tbl, columns = None, source = None):
        """Return a sqlalchemy.Table, autoloading column info if needed. 

        Arguments:
            tbl: a sqlalchemy.Table or string of form 'table_name' or 'schema_name.table_name'.
            columns: a tuple of column names for the table. Overrides source argument.
            source: a sqlalchemy engine, used to autoload columns.

        """
        if isinstance(tbl, sqlalchemy.Table):
            return tbl
        
        if not isinstance(tbl, str):
            raise ValueError("tbl must be a sqlalchemy Table or string, but was %s" %type(tbl))

        if columns is None and source is None:
            raise ValueError("One of columns or source must be specified")

        schema, table_name = tbl.split('.') if '.' in tbl else [None, tbl]

        columns = map(sqlalchemy.Column, columns) if columns is not None else tuple()

        return sqlalchemy.Table(
                table_name,
                sqlalchemy.MetaData(),
                *columns,
                schema = schema,
                autoload_with = source if not columns else None
                )

    def _get_preview(self):
        # need to make prev op a cte, so we don't override any previous limit
        new_sel = sql.select([self.last_op.alias()]).limit(5)
        tbl_small = self.append_op(new_sel)
        return collect(tbl_small)

    def __repr__(self):
        template = (
                "# Source: lazy query\n"
                "# DB Conn: {}\n"
                "# Preview:\n{}\n"
                "# .. may have more rows"
                )

        return template.format(repr(self.source.engine), repr(self._get_preview()))

    def _repr_html_(self):
        template = (
                "<div>"
                "<pre>"
                "# Source: lazy query\n"
                "# DB Conn: {}\n"
                "# Preview:\n"
                "</pre>"
                "{}"
                "<p># .. may have more rows</p>"
                "</div>"
                )

        data = self._get_preview()
        html_data = getattr(data, '_repr_html_', lambda: repr(data))()
        return template.format(self.source.engine, html_data)


def _repr_grouped_df_html_(self):
    return "<div><p>(grouped data frame)</p>" + self._selected_obj._repr_html_() + "</div>"



# Main Funcs 
# =============================================================================

# sql raw --------------

sql_raw = sql.literal_column

# show query -----------

from sqlalchemy.ext.compiler import compiles, deregister
from contextlib import contextmanager

@contextmanager
def use_simple_names():
    get_col_name = lambda el, *args, **kwargs: str(el.element.name)
    try:
        yield compiles(sql.compiler._CompileLabel)(get_col_name)
    except:
        pass
    finally:
        deregister(sql.compiler._CompileLabel)

@show_query.register(LazyTbl)
def _show_query(tbl, simplify = False):
    query = tbl.last_op #if not simplify else 
    compile_query = lambda: query.compile(
                dialect = tbl.source.dialect,
                compile_kwargs = {"literal_binds": True}
            )


    if simplify:
        # try to strip table names and labels where uneccessary
        with use_simple_names():
            print(compile_query())
    else:
        # use a much more verbose query
        print(compile_query())

    return tbl

# collect ----------

@collect.register(LazyTbl)
def _collect(__data, as_df = True):
    # TODO: maybe remove as_df options, always return dataframe
    # normally can just pass the sql objects to execute, but for some reason
    # psycopg2 completes about incomplete template.
    # see https://stackoverflow.com/a/47193568/1144523
    query = __data.last_op
    compiled = query.compile(
        dialect = __data.source.dialect,
        compile_kwargs = {"literal_binds": True}
    )
    if as_df:
        return pd.read_sql(compiled, __data.source)

    return __data.source.execute(compiled).fetchall()


@select.register(LazyTbl)
def _select(__data, *args, **kwargs):
    # see https://stackoverflow.com/questions/25914329/rearrange-columns-in-sqlalchemy-select-object
    if kwargs:
        raise NotImplementedError(
                "Using kwargs in select not currently supported. "
                "Use _.newname == _.oldname instead"
                )
    last_op = __data.last_op
    columns = {c.key: c for c in last_op.inner_columns}

    # same as for DataFrame
    colnames = Series(list(columns))
    vl = VarList()
    evaluated = (arg(vl) if callable(arg) else arg for arg in args)
    od = var_select(colnames, *evaluated)

    col_list = []
    for k,v in od.items():
        col = columns[k]
        col_list.append(col if v is None else col.label(v))

    return __data.append_op(last_op.with_only_columns(col_list))



@filter.register(LazyTbl)
def _filter(__data, *args, **kwargs):
    # TODO: aggregate funcs
    # Note: currently always produces 2 additional select statements,
    #       1 for window/aggs, and 1 for the where clause
    sel = __data.last_op.alias()
    win_sel = sql.select([sel], from_obj = sel)

    conds = []
    windows = []
    for ii, arg in enumerate(args):
        if isinstance(arg, Call):
            new_call = __data.shape_call(arg, verb_name = "Filter", arg_name = ii)
            #var_cols = new_call.op_vars(attr_calls = False)

            col_expr, win_cols = __data.track_call_windows(
                    new_call,
                    sel.columns,
                    window_cte = win_sel
                    )

            conds.append(col_expr)
        else:
            conds.append(arg)

    bool_clause = sql.and_(*conds)

    # move non-window functions to refer to win_sel clause (not the innermost)
    win_alias = win_sel.alias()
    bool_clause = sql.util.ClauseAdapter(win_alias).traverse(bool_clause)

    
    orig_cols = [win_alias.columns[k] for k in __data.get_ordered_col_names()]
    filt_sel = sql.select(orig_cols, from_obj = win_alias, whereclause = bool_clause)
    return __data.append_op(filt_sel)


@mutate.register(LazyTbl)
def _mutate(__data, **kwargs):
    # Cases
    #  - work with group by
    #  - window functions
    # TODO: verify it can follow a renaming select

    # track labeled columns in set
    sel = __data.last_op

    # evaluate each call
    for colname, func in kwargs.items():
        # keep set of columns labeled (aliased) in this select statement
        # need to use inner cols, since sel.columns uses ColumnClause, not Label
        labs = set(k for k,v in lift_inner_cols(sel).items() if isinstance(v, sql.elements.Label))
        new_call = __data.shape_call(func, verb_name = "Mutate", arg_name = colname)

        sel = _mutate_select(sel, colname, new_call, labs, __data)

    return __data.append_op(sel)


def _mutate_select(sel, colname, func, labs, __data):
    """Return select statement containing a new column, func expr as colname.
    
    Note: since a func can refer to previous columns generated by mutate, this
    function handles whether to add a column to the existing select statement,
    or to use it as a subquery.
    """
    replace_col = colname in sel.columns
    # Call objects let us check whether column expr used a derived column
    # e.g. SELECT a as b, b + 1 as c raises an error in SQL, so need subquery
    if not col_expr_requires_cte(func, sel, is_mutate = True):
        # New column may be able to modify existing select
        columns = lift_inner_cols(sel)

    else:
        # anything else requires a subquery
        cte = sel.alias(None)
        columns = cte.columns
        sel = sql.select([cte], from_obj = cte)

    # evaluate call expr on columns, making sure to use group vars
    new_col, windows = __data.track_call_windows(func, columns)

    # replacing an existing column, so strip it from select statement
    if replace_col:
        replaced = {**columns}
        replaced[colname] = new_col.label(colname)
        return sel.with_only_columns(list(replaced.values()))

    return sel.column(new_col.label(colname))


@transmute.register(LazyTbl)
def _transmute(__data, **kwargs):
    # will use mutate, then select some cols
    f_mutate = mutate.registry[type(__data)]

    # transmute keeps grouping cols, and any defined in kwargs
    cols_to_keep = ordered_union(__data.group_by, kwargs)

    sel = f_mutate(__data, **kwargs).last_op

    columns = lift_inner_cols(sel)
    sel_stripped = sel.with_only_columns([columns[k] for k in cols_to_keep])

    return __data.append_op(sel_stripped)


@arrange.register(LazyTbl)
def _arrange(__data, *args):
    last_op = __data.last_op
    cols = lift_inner_cols(last_op)

    
    new_calls = []
    for ii, expr in enumerate(args):
        if callable(expr):
            res = __data.shape_call(
                    expr, window = False,
                    verb_name = "Arrange", arg_name = ii
                    )

        else:
            res = expr

        new_calls.append(res)

    sort_cols = _create_order_by_clause(cols, *new_calls)

    order_by = __data.order_by + tuple(new_calls)
    return __data.append_op(last_op.order_by(*sort_cols), order_by = order_by)


# TODO: consolidate / pull expr handling funcs into own file?
def _create_order_by_clause(columns, *args):
    sort_cols = []
    for arg in args:
        # simple named column
        if isinstance(arg, str):
            sort_cols.append(columns[arg])
        # an expression
        elif callable(arg):
            #f, asc = _call_strip_ascending(arg)
            #col_op = f(cols) if asc else f(cols).desc()
            col_op = arg(columns)
            sort_cols.append(col_op)
        else:
            raise NotImplementedError("Must be string or callable")

    return sort_cols



@count.register(LazyTbl)
def _count(__data, *args, sort = False, wt = None, **kwargs):
    # TODO: if already col named n, use name nn, etc.. get logic from tidy.py
    if kwargs:
        raise NotImplementedError("TODO")

    if wt is not None:
        raise NotImplementedError("TODO")

    # similar to filter verb, we need two select statements,
    # an inner one for derived cols, and outer to group by them
    sel = __data.last_op.alias()
    sel_inner = sql.select([sel], from_obj = sel)

    # inner select ----
    # holds any mutation style columns
    group_cols = []
    for arg in args:
        col_name = simple_varname(arg)
        if col_name is None:
            # evaluate call
            col_expr = arg(sel.columns) if callable(arg) else arg

            # compile, so we can use the expr as its name (e.g. "id + 1")
            col_name = str(compile_el(__data, col_expr))
            label = col_expr.label(col_name)
            sel_inner.append_column(label)

        group_cols.append(col_name)

    # outer select ----
    # holds selected columns and tally (n)
    sel_inner_cte = sel_inner.alias()
    inner_cols = sel_inner_cte.columns
    sel_outer = sql.select(from_obj = sel_inner_cte)

    # apply any group vars from a group_by verb call first
    prev_group_cols = [inner_cols[k] for k in __data.group_by]
    if prev_group_cols:
        sel_outer.append_group_by(*prev_group_cols)
        sel_outer.append_column(*prev_group_cols)

    # now any defined in the count verb call
    for k in group_cols:
        sel_outer.append_group_by(inner_cols[k])
        sel_outer.append_column(inner_cols[k])

    sel_outer.append_column(sql.functions.count().label("n"))

    return __data.append_op(sel_outer)
    


@summarize.register(LazyTbl)
def _summarize(__data, **kwargs):
    # https://stackoverflow.com/questions/14754994/why-is-sqlalchemy-count-much-slower-than-the-raw-query
    # what if windowed mutate or filter has been done? 
    #   - filter is fine, since it uses a CTE
    #   - need to detect any window functions...
    sel = __data.last_op._clone()

    new_calls = {}
    for k, expr in kwargs.items():
        new_calls[k] = __data.shape_call(
                expr, window = False,
                verb_name = "Summarize", arg_name = k
                )

    needs_cte = [col_expr_requires_cte(call, sel) for call in new_calls.values()]

    # create select statement ----
    if any(needs_cte):
        # need a cte, due to alias cols or existing group by
        # current select stmt has group by clause, so need to make it subquery
        cte = sel.alias()
        columns = cte.columns
        sel = sql.select(from_obj = cte)
    else:
        # otherwise, can alter the existing select statement
        columns = lift_inner_cols(sel)
        old_froms = sel.froms

        sel = sel.with_only_columns([])
        sel.append_from(*old_froms)

    # add group by columns ----
    group_cols = [columns[k] for k in __data.group_by]
    sel.append_group_by(*group_cols)
    for col in group_cols:
        sel.append_column(col)

    # add each aggregate column ----
    # TODO: can't do summarize(b = mean(a), c = b + mean(a))
    #       since difficult for c to refer to agg and unagg cols in SQL
    for k, expr in new_calls.items():
        missing_cols = get_missing_columns(expr, columns)
        if missing_cols:
            raise NotImplementedError(
                    "Summarize cannot find the following columns: %s. "
                    "Note that it cannot refer to variables defined earlier in the "
                    "same summarize call." % missing_cols
                    )

        col = expr(columns).label(k)

        sel.append_column(col)

    new_data = __data.append_op(sel, group_by = tuple(), order_by = tuple())
    return new_data


@group_by.register(LazyTbl)
def _group_by(__data, *args, add = False, **kwargs):
    if kwargs:
        data = mutate(__data, **kwargs)
    else:
        data = __data

    cols = data.last_op.columns

    # put kwarg grouping vars last, so similar order to function call
    groups =  tuple(simple_varname(arg) for arg in args) + tuple(kwargs)
    if None in groups:
        raise NotImplementedError("Complex expressions not supported in sql group_by")

    unmatched = set(groups) - set(cols.keys())
    if unmatched:
        raise KeyError("group_by specifies columns missing from table: %s" %unmatched)

    if add:
        groups = ordered_union(data.group_by, groups)

    return data.copy(group_by = groups)


@ungroup.register(LazyTbl)
def _ungroup(__data):
    return __data.copy(group_by = tuple())


@case_when.register(sql.base.ImmutableColumnCollection)
def _case_when(__data, cases):
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

def _joined_cols(left_cols, right_cols, on_keys, full = False):
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
    if full:
        left_cols = {**left_cols}
        for lk, rk in on_keys.items():
            col = sql.functions.coalesce(left_cols[lk], right_cols[rk])
            left_cols[lk] = col.label(lk)

    # create labels ----
    l_labs = _relabeled_cols(left_cols, shared_labs, "_x")
    r_labs = _relabeled_cols(right_cols_no_keys, shared_labs, "_y")

    return l_labs + r_labs
    


def _relabeled_cols(columns, keys, suffix):
    # add a suffix to all columns with names in keys
    cols = []
    for k, v in columns.items():
        new_col = v.label(k + str(suffix)) if k in keys else v
        cols.append(new_col)
    return cols


@join.register(LazyTbl)
def _join(left, right, on = None, how = "inner"):
    # Needs to be on the table, not the select
    left_sel = left.last_op.alias()
    right_sel = right.last_op.alias()

    # handle arguments ----
    on  = _validate_join_arg_on(on)
    how = _validate_join_arg_how(how)
    
    if how == "right":
        # switch joins, since sqlalchemy doesn't have right join arg
        # see https://stackoverflow.com/q/11400307/1144523
        left_sel, right_sel = right_sel, left_sel

    # create join conditions ----
    bool_clause = _create_join_conds(left_sel, right_sel, on)

    # create join ----
    join = left_sel.join(
            right_sel,
            onclause = bool_clause,
            isouter = how != "inner",
            full = how == "full"
            )
    
    # note, shared_keys assumes on is a mapping...
    shared_keys = [k for k,v in on.items() if k == v]
    labeled_cols = _joined_cols(
            left_sel.columns,
            right_sel.columns,
            on_keys = on,
            full = how == "full"
            )

    sel = sql.select(labeled_cols, from_obj = join)
    return left.append_op(sel)


@semi_join.register(LazyTbl)
def _semi_join(left, right = None, on = None):

    left_sel = left.last_op.alias()
    right_sel = right.last_op.alias()

    # handle arguments ----
    on  = _validate_join_arg_on(on)
    
    # create join conditions ----
    bool_clause = _create_join_conds(left_sel, right_sel, on)

    # create inner join ----
    exists_clause = sql.select(
            [sql.literal(1)],
            from_obj = right_sel,
            whereclause = bool_clause
            )

    # only keep left hand select's columns ----
    sel = sql.select(
            left_sel.columns,
            from_obj = left_sel,
            whereclause = sql.exists(exists_clause)
            )

    return left.append_op(sel)


@anti_join.register(LazyTbl)
def _anti_join(left, right = None, on = None):
    left_sel = left.last_op.alias()
    right_sel = right.last_op.alias()

    # handle arguments ----
    on  = _validate_join_arg_on(on)
    
    # create join conditions ----
    bool_clause = _create_join_conds(left_sel, right_sel, on)

    # create inner join ----
    not_exists = ~sql.exists([1], from_obj = right_sel).where(bool_clause)
    sel = sql.select(left_sel.columns, from_obj = left_sel).where(not_exists)
    return left.append_op(sel)
       

def _validate_join_arg_on(on):
    if on is None:
        raise NotImplementedError("on arg must currently be dict")
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

    conds = []
    for l, r in on.items():
        col_expr = left_cols[l] == right_cols[r]
        conds.append(col_expr)
        
    return sql.and_(*conds)
    

# Head ------------------------------------------------------------------------

@head.register(LazyTbl)
def _head(__data, n = 5):
    sel = __data.last_op
    
    return __data.append_op(sel.limit(n))


# Rename ----------------------------------------------------------------------

@rename.register(LazyTbl)
def _rename(__data, **kwargs):
    sel = __data.last_op
    columns = lift_inner_cols(sel)

    # old_keys uses dict as ordered set
    old_to_new = {simple_varname(v):k for k,v in kwargs.items()}
    
    if None in old_to_new:
        raise KeyError("positional arguments must be simple column, "
                        "e.g. _.colname or _['colname']"
                        )

    labs = [c.label(old_to_new[k]) if k in old_to_new else c for k,c in columns.items()]

    new_sel = sel.with_only_columns(labs)

    return __data.append_op(new_sel)


# Distinct --------------------------------------------------------------------

@distinct.register(LazyTbl)
def _distinct(__data, *args, _keep_all = False, **kwargs):
    if (args or kwargs) and _keep_all:
        raise NotImplementedError("Distinct with variables specified in sql requires _keep_all = False")
    
    inner_sel = mutate(__data, **kwargs).last_op if kwargs else __data.last_op

    # TODO: this is copied from the df distinct version
    # cols dict below is used as ordered set
    cols = {simple_varname(x): True for x in args}
    cols.update(kwargs)

    if None in cols:
        raise KeyError("positional arguments must be simple column, "
                        "e.g. _.colname or _['colname']"
                        )

    # use all columns by default
    if not cols:
        cols = list(inner_sel.columns.keys())

    if not len(inner_sel._order_by_clause):
        # select distinct has to include any columns in the order by clause,
        # so can only safely modify existing statement when there's no order by
        sel_cols = lift_inner_cols(inner_sel)
        distinct_cols = [sel_cols[k] for k in cols]
        sel = inner_sel.with_only_columns(distinct_cols).distinct()
    else:
        # fallback to cte
        cte = inner_sel.alias()
        distinct_cols = [cte.columns[k] for k in cols]
        sel = sql.select(distinct_cols, from_obj = cte).distinct()

    return __data.append_op(sel)

    
# if_else ---------------------------------------------------------------------

@if_else.register(sql.elements.ColumnElement)
def _if_else(cond, true_vals, false_vals):
    whens = [(cond, true_vals)]
    return sql.case(whens, else_ = false_vals)


