"""
Implements LazyTbl to represent tables of SQL data, and registers it on verbs.

This module is responsible for the handling of the "table" side of things, while
translate.py handles translating column operations.


"""

import warnings

from siuba.dply.verbs import (
        show_query, collect,
        simple_varname,
        select,
        mutate,
        transmute,
        filter,
        arrange, _call_strip_ascending,
        summarize,
        count, add_count,
        group_by, ungroup,
        case_when,
        join, left_join, right_join, inner_join, semi_join, anti_join,
        head,
        rename,
        distinct,
        if_else,
        _select_group_renames,
        _var_select_simple
        )

from siuba.dply.tidyselect import VarList, var_select

from .translate import CustomOverClause, SqlColumn, SqlColumnAgg
from .utils import (
    get_dialect_translator,
    _FixedSqlDatabase,
    _is_dialect_duckdb,
    _sql_select,
    _sql_column_collection,
    _sql_add_columns,
    _sql_with_only_columns,
    _sql_simplify_select,
    MockConnection
)

from sqlalchemy import sql
import sqlalchemy
from siuba.siu import Call, Lazy, FunctionLookupError, singledispatch2
# TODO: currently needed for select, but can we remove pandas?
from pandas import Series
from functools import singledispatch

from sqlalchemy.sql import schema

from siuba.dply.across import _require_across, _set_data_context, _eval_with_context

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
        col_expr = node(self.columns)

        if not isinstance(col_expr, sql.elements.ClauseElement):
            return col_expr

        over_clauses = [x for x in self._get_over_clauses(col_expr) if isinstance(x, CustomOverClause)]

        # put groupings and orderings onto custom over clauses
        for over in over_clauses:
            # TODO: shouldn't mutate these over clauses
            group_by = sql.elements.ClauseList(
                    *[self.columns[name] for name in self.group_by]
                    )
            order_by = sql.elements.ClauseList(
                    *_create_order_by_clause(self.columns, *self.order_by)
                    )

            over.set_over(group_by, order_by)

        if len(over_clauses) and self.window_cte is not None:
            # custom name, or parameters like "%(...)s" may nest and break psycopg2
            # with columns you can set a key to fix this, but it doesn't seem to 
            # be an option with labels
            name = self._get_unique_name('win', lift_inner_cols(self.window_cte))
            label = col_expr.label(name)

            # put into CTE, and return its resulting column, so that subsequent
            # operations will refer to the window column on window_cte. Note that
            # the operations will use the actual column, so may need to use the
            # ClauseAdaptor to make it a reference to the label
            self.window_cte = _sql_add_columns(self.window_cte, [label])
            win_col = lift_inner_cols(self.window_cte).values()[-1]
            self.windows.append(win_col)

            return win_col
                
        return col_expr
    
    @staticmethod
    def _get_unique_name(prefix, columns):
        column_names = set(columns.keys())

        i = 1
        name = prefix + str(i)
        while name in column_names:
            i += 1
            name = prefix + str(i)


        return name

    @staticmethod
    def _get_over_clauses(clause):
        windows = []
        append_win = lambda col: windows.append(col)

        sql.util.visitors.traverse(clause, {}, {"over": append_win})

        return windows


class SqlLabelReplacer:
    """Create a visitor to replace source labels with destination.

    Note that this is meant to be used with sqlalchemy visitors.
    """

    def __init__(self, src_columns, dst_columns):
        self.src_columns = src_columns
        self.src_labels = set([x for x in src_columns if isinstance(x, sql.elements.Label)])
        self.dst_columns = dst_columns
        self.applied = False

    def __call__(self, clause):
        return sql.util.visitors.replacement_traverse(clause, {}, self.visit)
    
    def visit(self, el):
        from sqlalchemy.sql.elements import ColumnClause, Label, ClauseElement, TypeClause
        from sqlalchemy.sql.schema import Column

        if isinstance(el, TypeClause):
            # TODO: for some reason this type throws an error if unguarded
            return None

        if isinstance(el, ClauseElement):
            if el in self.src_labels:
                self.applied = True
                return self.dst_columns[el.name]
            elif el in self.src_columns:
                return self.dst_columns[el.name]

            elif isinstance(el, ColumnClause) and not isinstance(el, Column):
                # Raw SQL, which will need a subquery, but not substitution
                if el.key != "*":
                    self.applied = True
        
        return None
            

#def track_call_windows(call, columns, group_by, order_by, window_cte = None):
#    listener = WindowReplacer(columns, group_by, order_by, window_cte)
#    col = listener.enter(call)
#    return col, listener.windows, listener.window_cte


def track_call_windows(call, columns, group_by, order_by, window_cte = None):
    col_expr = call(columns)

    crnt_group_by = sql.elements.ClauseList(
            *[columns[name] for name in group_by]
            )
    crnt_order_by = sql.elements.ClauseList(
            *_create_order_by_clause(columns, *order_by)
            )
    return replace_call_windows(col_expr, crnt_group_by, crnt_order_by, window_cte)



@singledispatch
def replace_call_windows(col_expr, group_by, order_by, window_cte = None):
    raise TypeError(str(type(col_expr)))


@replace_call_windows.register(sql.base.ImmutableColumnCollection)
def _(col_expr, group_by, order_by, window_cte = None):
    all_over_clauses = []
    for col in col_expr:
        _, over_clauses, window_cte = replace_call_windows(
            col,
            group_by,
            order_by,
            window_cte
        )
        all_over_clauses.extend(over_clauses)

    return col_expr, all_over_clauses, window_cte


@replace_call_windows.register(sql.elements.ClauseElement)
def _(col_expr, group_by, order_by, window_cte = None):

    over_clauses = WindowReplacer._get_over_clauses(col_expr)

    for over in over_clauses:
        # TODO: shouldn't mutate these over clauses
        over.set_over(group_by, order_by)

    if len(over_clauses) and window_cte is not None:
        # custom name, or parameters like "%(...)s" may nest and break psycopg2
        # with columns you can set a key to fix this, but it doesn't seem to 
        # be an option with labels
        name = WindowReplacer._get_unique_name('win', lift_inner_cols(window_cte))
        label = col_expr.label(name)

        # put into CTE, and return its resulting column, so that subsequent
        # operations will refer to the window column on window_cte. Note that
        # the operations will use the actual column, so may need to use the
        # ClauseAdaptor to make it a reference to the label
        window_cte = _sql_add_columns(window_cte, [label])
        win_col = lift_inner_cols(window_cte).values()[-1]

        return win_col, over_clauses, window_cte
            
    return col_expr, over_clauses, window_cte


def lift_inner_cols(tbl):
    cols = list(tbl.inner_columns)

    return _sql_column_collection(cols)

def col_expr_requires_cte(call, sel, is_mutate = False):
    """Return whether a variable assignment needs a CTE"""

    call_vars = set(call.op_vars(attr_calls = False))

    sel_labs = get_inner_labels(sel)

    # I use the acronym fwg sol (frog soul) to remember sql clause eval order
    # from, where, group by, select, order by, limit
    # group clause evaluated before select clause, so not issue for mutate
    group_needs_cte = not is_mutate and len(sel._group_by_clause)
    
    return (   group_needs_cte
            # TODO: detect when a new var in mutate conflicts w/ order by
            #or len(sel._order_by_clause)
            or not sel_labs.isdisjoint(call_vars)
            )

def get_inner_labels(sel):
    columns = lift_inner_cols(sel)
    sel_labs = set(k for k,v in columns.items() if isinstance(v, sql.elements.Label))
    return sel_labs

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


def _warn_missing(missing_groups):
    warnings.warn(f"Adding missing grouping variables: {missing_groups}")


# Table -----------------------------------------------------------------------

class LazyTbl:
    def __init__(
            self, source, tbl, columns = None,
            ops = None, group_by = tuple(), order_by = tuple(),
            translator = None
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

        # get dialect name
        dialect = self.source.dialect.name
        self.translator = get_dialect_translator(dialect)

        self.tbl = self._create_table(tbl, columns, self.source)

        # important states the query can be in (e.g. grouped)
        self.ops = [self.tbl] if ops is None else ops

        self.group_by = group_by
        self.order_by = order_by


    def append_op(self, op, **kwargs):
        cpy = self.copy(**kwargs)
        cpy.ops = cpy.ops + [op]
        return cpy

    def copy(self, **kwargs):
        return self.__class__(**{**self.__dict__, **kwargs})

    def shape_call(
            self,
            call, window = True, str_accessors = False,
            verb_name = None, arg_name = None,
            ):
        return self.translator.shape_call(call, window, str_accessors, verb_name, arg_name)

    def track_call_windows(self, call, columns = None, window_cte = None):
        """Returns tuple of (new column expression, list of window exprs)"""

        columns = self.last_op.columns if columns is None else columns
        return track_call_windows(call, columns, self.group_by, self.order_by, window_cte)

    def get_ordered_col_names(self):
        """Return columns from current select, with grouping columns first."""
        ungrouped = [k for k in self.last_op.columns.keys() if k not in self.group_by]
        return list(self.group_by) + ungrouped

    #def label_breaks_order_by(self, name):
    #    """Returns True if a new column label would break the order by vars."""

    #    # TODO: arrange currently allows literals, which breaks this. it seems
    #    #       better to only allow calls in arrange.
    #    order_by_vars = {c.op_vars(attr_calls=False) for c in self.order_by}




    @property
    def last_op(self) -> "sql.Table | sql.Select":
        last_op = self.ops[-1]

        if last_op is None:
            raise TypeError()

        return last_op

    @property
    def last_select(self):
        last_op = self.last_op
        if not isinstance(last_op, sql.selectable.SelectBase):
            return last_op.select()

        return last_op

    @staticmethod
    def _create_table(tbl, columns = None, source = None):
        """Return a sqlalchemy.Table, autoloading column info if needed. 

        Arguments:
            tbl: a sqlalchemy.Table or string of form 'table_name' or 'schema_name.table_name'.
            columns: a tuple of column names for the table. Overrides source argument.
            source: a sqlalchemy engine, used to autoload columns.

        """
        if isinstance(tbl, sql.selectable.FromClause):
            return tbl
        
        if not isinstance(tbl, str):
            raise ValueError("tbl must be a sqlalchemy Table or string, but was %s" %type(tbl))

        if columns is None and source is None:
            raise ValueError("One of columns or source must be specified")

        schema, table_name = tbl.split('.') if '.' in tbl else [None, tbl]

        columns = map(sqlalchemy.Column, columns) if columns is not None else tuple()

        # TODO: pybigquery uses schema to mean project_id, so we cannot use
        # siuba's classic breakdown "{schema}.{table_name}". Basically
        # pybigquery uses "{schema=project_id}.{dataset_dot_table_name}" in its internal
        # logic. An important side effect is that bigquery errors for
        # `dataset`.`table`, but not `dataset.table`.
        if source and source.dialect.name == "bigquery":
            table_name = tbl
            schema = None

        return sqlalchemy.Table(
                table_name,
                sqlalchemy.MetaData(bind = source),
                *columns,
                schema = schema,
                autoload_with = source if not columns else None
                )

    def _get_preview(self):
        # need to make prev op a cte, so we don't override any previous limit
        new_sel = self.last_select.limit(5)
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

        # _repr_html_ can not exist or return None, to signify that repr should be used
        if not hasattr(data, '_repr_html_'):
            return None

        html_data = data._repr_html_()
        if html_data is None:
            return None

        return template.format(self.source.engine, html_data)


def _repr_grouped_df_html_(self):
    return "<div><p>(grouped data frame)</p>" + self._selected_obj._repr_html_() + "</div>"



# Main Funcs 
# =============================================================================

# sql raw --------------

sql_raw = sql.literal_column

# show query -----------

@show_query.register(LazyTbl)
def _show_query(tbl, simplify = False, return_table = True):
    #query = tbl.last_op #if not simplify else 
    compile_query = lambda query: query.compile(
                dialect = tbl.source.dialect,
                compile_kwargs = {"literal_binds": True}
            )


    if simplify:
        # try to strip table names and labels where unnecessary
        simple_sel = _sql_simplify_select(tbl.last_select)

        explained = compile_query(simple_sel)
    else:
        # use a much more verbose query
        explained = compile_query(tbl.last_select)

    if return_table:
        print(str(explained))
        return tbl

    return str(explained)



# collect ----------

@collect.register(LazyTbl)
def _collect(__data, as_df = True):
    # TODO: maybe remove as_df options, always return dataframe

    if isinstance(__data.source, MockConnection):
        # a mock sqlalchemy is being used to show_query, and echo queries.
        # it doesn't return a result object or have a context handler, so
        # we need to bail out early
        return

    # compile query ----

    if _is_dialect_duckdb(__data.source):
        # TODO: can be removed once next release of duckdb fixes:
        # https://github.com/duckdb/duckdb/issues/2972
        query = __data.last_select
        compiled = query.compile(
            dialect = __data.source.dialect,
            compile_kwargs = {"literal_binds": True}
        )
    else:
        compiled = __data.last_select

    # execute query ----

    with __data.source.connect() as conn:
        if as_df:
            sql_db = _FixedSqlDatabase(conn)

            if _is_dialect_duckdb(__data.source):
                # TODO: pandas read_sql is very slow with duckdb.
                # see https://github.com/pandas-dev/pandas/issues/45678
                # going to handle here for now. address once LazyTbl gets
                # subclassed per backend.
                duckdb_con = conn.connection.c
                return duckdb_con.query(str(compiled)).to_df()
            else:
                #
                return sql_db.read_sql(compiled)

        return conn.execute(compiled)


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
        last_sel.with_only_columns(col_list),
        group_by = group_keys
    )



@filter.register(LazyTbl)
def _filter(__data, *args):
    # Note: currently always produces 2 additional select statements,
    #       1 for window/aggs, and 1 for the where clause

    sel = __data.last_op.alias()                   # original select
    win_sel = sel.select()

    conds = []
    windows = []
    with _set_data_context(__data):
        for ii, arg in enumerate(args):

            if isinstance(arg, Call):
                new_call = __data.shape_call(arg, verb_name = "Filter", arg_name = ii)
                #var_cols = new_call.op_vars(attr_calls = False)

                # note that a new win_sel is returned, w/ window columns appended
                col_expr, win_cols, win_sel = __data.track_call_windows(
                        new_call,
                        sel.columns,
                        window_cte = win_sel
                        )

                if isinstance(col_expr, sql.base.ImmutableColumnCollection):
                    conds.extend(col_expr)
                else:
                    conds.append(col_expr)

                windows.extend(win_cols)
                
            else:
                conds.append(arg)

    bool_clause = sql.and_(*conds)

    # first cte, windows ----
    if len(windows):
        
        win_alias = win_sel.alias()

        # move non-window functions to refer to win_sel clause (not the innermost) ---
        bool_clause = sql.util.ClauseAdapter(win_alias) \
                .traverse(bool_clause)

        orig_cols = [win_alias.columns[k] for k in sel.columns.keys()]
    else:
        orig_cols = [sel]
    
    # create second cte ----
    filt_sel = _sql_select(orig_cols).where(bool_clause)
    return __data.append_op(filt_sel)


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


def _mutate_cols(__data, args, kwargs, verb_name, arrange_clause=False):
    result_names = {}     # used as ordered set
    result_expr = []
    sel = __data.last_select

    for ii, func in enumerate(args):
        # case 1: simple names ----
        simple_name = simple_varname(func)
        if simple_name is not None:
            result_names[simple_name] = True
            continue

        # case 2: across ----
        _require_across(func, verb_name)

        inner_cols = lift_inner_cols(sel)
        cols_result = _eval_with_context(__data, inner_cols, func)

        # TODO: remove or raise a more informative error
        assert isinstance(cols_result, sql.base.ImmutableColumnCollection), type(cols_result)

        # replace any labels that require a subquery ----
        if arrange_clause:
            result_expr.extend(cols_result)
        else:
            sel = _select_mutate_result(sel, cols_result)
            result_names.update({k: True for k in cols_result.keys()})

    
    for new_name, func in kwargs.items():
        inner_cols = lift_inner_cols(sel)

        expr_shaped = __data.shape_call(func, verb_name = verb_name, arg_name = new_name)
        new_col, windows, _ = __data.track_call_windows(expr_shaped, inner_cols)

        if isinstance(new_col, sql.base.ImmutableColumnCollection):
            raise TyepError(
                f"{verb_name} named arguments must return a single column, but `{k}` "
                "returned multiple columns."
            )
        
        labeled = new_col.label(new_name)

        if arrange_clause:
            result_expr.append(labeled)
        else:
            sel = _select_mutate_result(sel, labeled)
            result_names[new_name] = True


    if arrange_clause:
        return result_expr, sel

    return list(result_names), sel



def _mutate_select(sel, colname, func, labs, __data):
    """Return select statement containing a new column, func expr as colname.
    
    Note: since a func can refer to previous columns generated by mutate, this
    function handles whether to add a column to the existing select statement,
    or to use it as a subquery.
    """
    replace_col = colname in lift_inner_cols(sel)
    # Call objects let us check whether column expr used a derived column
    # e.g. SELECT a as b, b + 1 as c raises an error in SQL, so need subquery
    if not col_expr_requires_cte(func, sel, is_mutate = True):
        # New column may be able to modify existing select
        columns = lift_inner_cols(sel)

    else:
        # anything else requires a subquery
        cte = sel.alias(None)
        columns = cte.columns
        sel = cte.select()

    # evaluate call expr on columns, making sure to use group vars
    new_col, windows, _ = __data.track_call_windows(func, columns)

    # replacing an existing column, so strip it from select statement
    if replace_col:
        replaced = {**columns}
        replaced[colname] = new_col.label(colname)
        return sel.with_only_columns(list(replaced.values()))

    return _sql_add_columns(sel, [new_col.label(colname)])


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


@arrange.register(LazyTbl)
def _arrange(__data, *args):
    # Note that SQL databases often do not subquery order by clauses. Arrange
    # sets order_by on the backend, so it can set order by in over elements,
    # and handle when new columns are named the same as order by vars.
    # see: https://dba.stackexchange.com/q/82930

    last_sel = __data.last_select
    cols = lift_inner_cols(last_sel)

    # TODO: implement across in arrange
    #exprs, _ = _mutate_cols(__data, args, kwargs, "Arrange", arrange_clause=True)

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
    return __data.append_op(last_sel.order_by(*sort_cols), order_by = order_by)


# TODO: consolidate / pull expr handling funcs into own file?
def _create_order_by_clause(columns, *args):
    sort_cols = []
    for arg in args:
        # simple named column
        if isinstance(arg, str):
            sort_cols.append(columns[arg])
        # an expression
        elif callable(arg):
            # handle special case where -_.colname -> colname DESC
            f, asc = _call_strip_ascending(arg)
            col_op = f(columns) if asc else f(columns).desc()
            #col_op = arg(columns)
            sort_cols.append(col_op)
        else:
            raise NotImplementedError("Must be string or callable")

    return sort_cols



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
    arg_names = []
    for arg in args:
        name = simple_varname(arg)
        if name is None:
            raise NotImplementedError(
                    "Count positional arguments must be single column name. "
                    "Use a named argument to count using complex expressions."
                    )
        arg_names.append(name)

    result_names, sel_inner = _mutate_cols(__data, args, kwargs, "Count")

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


@summarize.register(LazyTbl)
def _summarize(__data, *args, **kwargs):
    # https://stackoverflow.com/questions/14754994/why-is-sqlalchemy-count-much-slower-than-the-raw-query
    old_sel = __data.last_select._clone()

    new_calls = {}
    for k, expr in kwargs.items():
        new_calls[k] = __data.shape_call(
                expr, window = False,
                verb_name = "Summarize", arg_name = k
                )

    needs_cte = [col_expr_requires_cte(call, old_sel) for call in new_calls.values()]
    group_on_labels = set(__data.group_by) & get_inner_labels(old_sel)

    # create select statement ----

    if any(needs_cte) or len(group_on_labels):
        # need a cte, due to alias cols or existing group by
        # current select stmt has group by clause, so need to make it subquery
        cte = old_sel.alias()
        columns = cte.columns
        sel = sql.select().select_from(cte)
    else:
        # otherwise, can alter the existing select statement
        columns = lift_inner_cols(old_sel)
        sel = old_sel

        # explicitly add original from clause tables, since we will be limiting
        # the columns this select uses, which causes sqlalchemy to remove
        # unreferenced tables
        for _from in sel.froms:
            sel = sel.select_from(_from)
        

    # add group by columns ----
    group_cols = [columns[k] for k in __data.group_by]

    # add each aggregate column ----
    # TODO: can't do summarize(b = mean(a), c = b + mean(a))
    #       since difficult for c to refer to agg and unagg cols in SQL
    expr_cols = []
    for k, expr in new_calls.items():
        missing_cols = get_missing_columns(expr, columns)
        if missing_cols:
            raise NotImplementedError(
                    "Summarize cannot find the following columns: %s. "
                    "Note that it cannot refer to variables defined earlier in the "
                    "same summarize call." % missing_cols
                    )

        col = expr(columns).label(k)
        expr_cols.append(col)
        

    all_cols = [*group_cols, *expr_cols]
    final_sel = _sql_with_only_columns(sel, all_cols).group_by(*group_cols)
    new_data = __data.append_op(final_sel, group_by = tuple(), order_by = tuple())
    return new_data


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
    

# Head ------------------------------------------------------------------------

@head.register(LazyTbl)
def _head(__data, n = 5):
    sel = __data.last_select
    
    return __data.append_op(sel.limit(n))


# Rename ----------------------------------------------------------------------

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

    new_sel = sel.with_only_columns(labs)

    missing_groups, group_keys = _select_group_renames(old_to_new, __data.group_by)

    return __data.append_op(new_sel, group_by=group_keys)


# Distinct --------------------------------------------------------------------

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

    
# if_else ---------------------------------------------------------------------

@if_else.register(sql.elements.ColumnElement)
def _if_else(cond, true_vals, false_vals):
    whens = [(cond, true_vals)]
    return sql.case(whens, else_ = false_vals)


