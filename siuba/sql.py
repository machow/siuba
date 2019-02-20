from .tidy import (
        select, VarList, var_select,
        mutate,
        filter,
        arrange, _call_strip_ascending,
        summarize,
        count
        )
from sqlalchemy import sql
from siuba.siu import strip_symbolic, Call
# TODO: currently needed for select, but can we remove pandas?
from pandas import Series


# TODO:
#   - summarize
#   - case_when
#   - distinct
#   - group_by
#   - head
#   - window funcs

def lift_inner_cols(tbl, monitor = False):
    cols = list(tbl.inner_columns)
    data = {col.key: col for col in cols}
    if monitor:
        return MonitoredColumns(data, cols)
    else:
        return sql.base.ImmutableColumnCollection(data, cols)

def get_labeled_cols(cols):
    return set(k for k,v in cols.items() if isinstance(v, sql.elements.Label))


def is_grouped_sel(select):
    return False

class LazyTbl:
    def __init__(self, source, tbl, ops = None):
        self.source = source
        self.tbl = tbl
        self.ops = [sql.Select([tbl])] if ops is None else ops

    def add_op_single(self, op, data, *args, **kwargs):
        op_dict = dict(op = op, data = data, args = args, kwargs = kwargs)

        return self.__class__(self.source, ops = self.ops + [op_dict])

    def append_op(self, op):
        return self.__class__(self.source, self.tbl, self.ops + [op])

    @property
    def last_op(self):
        return self.ops[-1] if len(self.ops) else None



@select.register(LazyTbl)
def _(__data, *args, **kwargs):
    # see https://stackoverflow.com/questions/25914329/rearrange-columns-in-sqlalchemy-select-object
    last_op = __data.last_op
    if isinstance(last_op, sql.Select):
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

    raise NotImplementedError("last op must be select statement")
    #return LazyTbl.append_op(sql.Select(col_list))



@filter.register(LazyTbl)
def _(__data, *args, **kwargs):
    # TODO: use inner_columns to prevent nesting selects?
    last_op = __data.last_op
    cols = lift_inner_cols(last_op)
    conds = [arg(cols) if callable(arg) else arg for arg in args]
    bool_clause = sql.and_(*conds)

    if isinstance(last_op, sql.Select):
        return __data.append_op(last_op.where(bool_clause))

    return __data.append_op(sql.Select(['*'], whereclause = bool_clause))


@mutate.register(LazyTbl)
def _(__data, **kwargs):
    # Cases
    #  - work with group by
    #  - window functions
    # TODO: can't re-use select, for example if it's following a select() that renames

    # track labeled columns in set
    # 
    last_op = __data.last_op
    labs = set(k for k,v in last_op.columns.items() if isinstance(v, sql.elements.Label))

    sel = last_op
    # TODO: could use copy of last_op plus append_column
    for colname, func in kwargs.items():
        inner_cols = lift_inner_cols(sel)
        replace_col = colname in sel.columns
        strip_f = strip_symbolic(func)
        # Call objects let us check whether column expr used a derived column
        # e.g. SELECT a as b, b + 1 as c raises an error in SQL, so need subquery
        if isinstance(strip_f, Call) and labs.isdisjoint(strip_f.op_vars()):
            # New column can also modify existing select
            if replace_col:
                new_col = strip_f(inner_cols).label(colname)
                sel = sel.with_only_columns([v for k,v in inner_cols.items() if k != colname]) \
                        .column(new_col)
            # Call is only a function of non-derived columns, so can modify select
            else:
                new_col = strip_f(inner_cols).label(colname)
                sel = sel.column(new_col)
        else:
            # anything else requires a subquery
            new_col = strip_f(sel.columns).label(colname)
            sel = sql.select([sel, new_col], from_obj = sel)
        
        labs.add(colname)

    return __data.append_op(sel)

    raise NotImplementedError("Must be select statement")


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
    # TODO: need group_by to do this
    last_op = __data.last_op
    __data.append_op(last_op)
    


@summarize.register(LazyTbl)
def _(__data, **kwargs):
    # https://stackoverflow.com/questions/14754994/why-is-sqlalchemy-count-much-slower-than-the-raw-query
    pass
