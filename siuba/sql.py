from .tidy import VarList, var_select, select, mutate, filter
from sqlalchemy import sql
from siuba.siu import strip_symbolic
# TODO: currently needed for select, but can we remove pandas?
from pandas import Series


# TODO:
#   - arrange
#   - summarize
#   - case_when
#   - distinct
#   - group_by
#   - head
#   - window funcs

def lift_inner_cols(tbl):
    cols = list(tbl.inner_columns)
    data = {col.key: col for col in cols}
    return sql.base.ImmutableColumnCollection(data, cols)

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
    last_op = __data.last_op
    cols = lift_inner_cols(last_op)
    
    if isinstance(last_op, sql.Select):
        # TODO: could use copy of last_op plus append_column
        tmp_op = last_op
        for k, v in kwargs.items():
            new_col = v(cols).label(k)
            tmp_op = tmp_op.column(new_col)

        return __data.append_op(tmp_op)

    raise NotImplementedError("Must be select statement")
    


