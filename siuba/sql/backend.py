"""
Implements LazyTbl to represent tables of SQL data, and registers it on verbs.

This module is responsible for the handling of the "table" side of things, while
translate.py handles translating column operations.


"""

import warnings

from .translate import CustomOverClause
from .utils import (
    get_dialect_translator,
    _sql_column_collection,
    _sql_add_columns,
)

from sqlalchemy import sql
import sqlalchemy
from siuba.siu import FunctionLookupError
from functools import singledispatch

# Helpers ---------------------------------------------------------------------

class SqlFunctionLookupError(FunctionLookupError): pass

class WindowReplacer:

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

            #elif isinstance(el, ColumnClause) and not isinstance(el, Column):
            #    # Raw SQL, which will need a subquery, but not substitution
            #    if el.key != "*":
            #        self.applied = True
        
        return None
            

def track_call_windows(call, columns, group_by, order_by, window_cte = None):
    col_expr = call(columns)

    crnt_group_by = sql.elements.ClauseList(
            *[columns[name] for name in group_by]
            )

    crnt_order_by = sql.elements.ClauseList(*order_by)

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

def get_single_from(sel):
    froms = sel.froms

    n_froms = len(froms)
    if n_froms != 1:
        raise ValueError(
            f"Expected a single table in the from clause, but found {n_froms}"
        )

    return froms[0]

def lift_inner_cols(tbl):
    cols = list(tbl.inner_columns)

    return _sql_column_collection(cols)

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

        from .verbs.arrange import _eval_arrange_args

        columns = self.last_op.columns if columns is None else columns

        order_by = _eval_arrange_args(self, self.order_by, columns)

        return track_call_windows(call, columns, self.group_by, order_by, window_cte)

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
        from siuba.dply.verbs import collect

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


sql_raw = sql.literal_column

