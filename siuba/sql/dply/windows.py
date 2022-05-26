from sqlalchemy import sql
from sqlalchemy.sql.base import ImmutableColumnCollection

from siuba.dply.windows import win_over, order_by
from siuba.siu import Call

from ..dialects.base import SqlColumn
from ..verbs import replace_call_windows



@win_over.register
def _win_over(
    codata: SqlColumn,
    __data: ImmutableColumnCollection,
    expr,
    partition = None,
    order = None,
    frame = None
    ):
    """Apply a window by default to all operations.

    Note that this function will not automatically change any sqlalchemy Over
    clause, but siuba.sql.translate.CustomOverClause.

    Examples
    --------
    >>> from siuba.data import cars_sql
    >>> from siuba import _, mutate
    >>> cars_sql >> mutate(win_over(_, _.hp.mean(), partition=_.cyl, order=_.mpg))
    """

    if frame is not None:
        raise NotImplementedError()

    if partition is None:
        partition = tuple()
    elif isinstance(partition, sql.ColumnElement):
        partition = [partition]

    if order is None:
        order = tuple()
    elif isinstance(order, sql.ColumnElement):
        order = [order]

    crnt_group_by = sql.elements.ClauseList(*partition)

    crnt_order_by = sql.elements.ClauseList( *order)


    res, _, _ = replace_call_windows(expr, crnt_group_by, crnt_order_by)

    return res
