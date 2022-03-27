import warnings

from sqlalchemy.sql.elements import ClauseElement 
from sqlalchemy.sql.base import ImmutableColumnCollection
from sqlalchemy import sql

from ..translate import (
        SqlColumn, SqlColumnAgg,
        win_cumul, AggOver, CumlOver, RankOver, warn_arg_default, win_absent
        )

from ..dialects.sqlite import SqliteColumn
from ..dialects.mysql import MysqlColumn
from ..dialects.bigquery import BigqueryColumn

from siuba.dply.vector import (
        #cumall, cumany, cummean,
        desc,
        dense_rank, percent_rank, min_rank, cume_dist,
        row_number,
        #ntile,
        between,
        coalesce,
        lead, lag,
        n,
        n_distinct,
        na_if,
        #near,
        nth, first, last
        )


from ..translate import SiubaSqlRuntimeWarning
warnings.simplefilter('once', SiubaSqlRuntimeWarning)

# desc ------------------------------------------------------------------------

@desc.register
def _desc_sql(codata: SqlColumn, x: ClauseElement) -> ClauseElement:
    """
    Example:
        >>> print(desc(SqlColumn(), sql.column('a')))
        a DESC
    """
    return x.desc()


# ranking functions -----------------------------------------------------------
# note: here we don't use the decorator syntax, but maybe we should for
#       consistency
# TODO: remove repetition in rank definitions

def _sql_rank_over(rank_func, col, partition, nulls_last):
    # partitioning ensures aggregates that use total length are correct,
    # e.g. percent rank, cume_dist and friends, by separating NULLs into their 
    # own partition
    over_clause = RankOver(
            rank_func(),
            order_by = col if not nulls_last else col.nullslast(),
            partition_by = col.isnot(None) if partition else None
            )

    return sql.case({col.isnot(None): over_clause})

def _sql_rank(func_name, partition = False, nulls_last = False):
    # partition controls whether to make partition by NOT NULL

    rank_func = getattr(sql.func, func_name)

    def f(_, col, na_option = None) -> RankOver:
        if na_option == "keep":
            return _sql_rank_over(rank_func, col, partition, nulls_last)

        warn_arg_default(func_name, 'na_option', None, "keep")

        return RankOver(rank_func(), order_by = col)

    return f


dense_rank  .register(SqlColumn, _sql_rank("dense_rank"))
percent_rank.register(SqlColumn, _sql_rank("percent_rank"))
cume_dist   .register(SqlColumn, _sql_rank("cume_dist", partition = True))
min_rank    .register(SqlColumn, _sql_rank("rank", partition = True))


dense_rank  .register(SqliteColumn, _sql_rank("dense_rank", nulls_last=True))
percent_rank.register(SqliteColumn, _sql_rank("percent_rank", nulls_last=True))
cume_dist   .register(SqliteColumn, _sql_rank("cume_dist", partition = True))
min_rank    .register(SqliteColumn, _sql_rank("rank", nulls_last=True))

# partition everything, since MySQL puts NULLs first
# see: https://stackoverflow.com/q/1498648/1144523
dense_rank  .register(MysqlColumn, _sql_rank("dense_rank", partition = True))
percent_rank.register(MysqlColumn, _sql_rank("percent_rank", partition = True))
cume_dist   .register(MysqlColumn, _sql_rank("cume_dist", partition = True))
min_rank    .register(MysqlColumn, _sql_rank("rank", partition = True))

dense_rank  .register(BigqueryColumn, _sql_rank("dense_rank", nulls_last = True))
percent_rank.register(BigqueryColumn, _sql_rank("percent_rank", nulls_last = True))



# row_number ------------------------------------------------------------------

@row_number.register
def _row_number_sql(codata: SqlColumn, col: ClauseElement) -> CumlOver:
    """
    Example:
        >>> print(row_number(SqlColumn(), sql.column('a')))
        row_number() OVER ()
        
    """
    return CumlOver(sql.func.row_number())

# between ---------------------------------------------------------------------

@between.register
def _between_sql(codata: SqlColumn, x, left, right, default = None) -> ClauseElement:
    """
    Example:
        >>> print(between(SqlColumn(), sql.column('a'), 1, 2))
        a BETWEEN :a_1 AND :a_2

        >>> print(between(SqlColumn(), sql.column('a'), 1, 2, default = False))
        coalesce(a BETWEEN :a_1 AND :a_2, :coalesce_1)

    """
    
    if default is not False:
        # TODO: warn
        pass

    if default is None:
        return x.between(left, right)

    return sql.functions.coalesce(x.between(left, right), default)

# coalesce --------------------------------------------------------------------

@coalesce.register
def _coalesce_sql(codata: SqlColumn, x, *args) -> ClauseElement:
    """
    Example:
        >>> print(coalesce(SqlColumn(), sql.column('a'), sql.column('b')))
        coalesce(a, b)

        >>> print(coalesce(SqlColumn(), 1, sql.column('a')))
        coalesce(:coalesce_1, a)
    """
    return sql.functions.coalesce(x, *args)


# lead and lag ----------------------------------------------------------------

@lead.register
def _lead_sql(codata: SqlColumn, x, n = 1, default = None) -> ClauseElement:
    """
    Example:
        >>> print(lead(SqlColumn(), sql.column('a'), 2, 99))
        lead(a, :lead_1, :lead_2) OVER ()
    """
    
    f = win_cumul("lead", rows=None)
    return f(codata, x, n, default)

@lag.register
def _lag_sql(codata: SqlColumn, x, n = 1, default = None) -> ClauseElement:
    """
    Example:
        >>> print(lag(SqlColumn(), sql.column('a'), 2, 99))
        lag(a, :lag_1, :lag_2) OVER ()
    """
    f = win_cumul("lag", rows=None)
    return f(codata, x, n , default)


# n ---------------------------------------------------------------------------

@n.register
def _n_sql(codata: SqlColumn, x) -> ClauseElement:
    """
    Example:
        >>> print(n(SqlColumn(), sql.column('a')))
        count(*) OVER ()
    """
    return AggOver(sql.func.count())

# TODO: MC-Note - fix

@n.register
def _n_sql_agg(codata: SqlColumnAgg, x) -> ClauseElement:
    """
    Example:
        >>>
        >> from siuba.sql.translate import SqlColumnAgg
        >> print(n(SqlColumnAgg(), None))
        count(*)
    """

    return sql.func.count()



# n_distinct ------------------------------------------------------------------

@n_distinct.register
def _n_distinct_sql(codata: SqlColumn, x: ClauseElement) -> ClauseElement:
    """
    Example:
        >>> print(n_distinct(SqlColumn(), sql.column('a')) )
        count(distinct(a))
    """
    return sql.func.count(sql.func.distinct(x))


# na_if -----------------------------------------------------------------------

@na_if.register
def _na_if_sql(codata: SqlColumn, x, y) -> ClauseElement:
    """
    Example:
        >>> print(na_if(SqlColumn(), sql.column('x'), 2))
        nullif(x, :nullif_1)
    """
    return sql.func.nullif(x, y)


# nth, first, last ------------------------------------------------------------
# note: first and last wrap around nth, so are not dispatchers.
#       this may need to change this in the future, since this means they won't
#       show their own name, when you print, e.g. first(_.x)

@nth.register
def _nth_sql(codata: SqlColumn, x, n, order_by = None, default = None) -> ClauseElement:
    if default is not None:
        raise NotImplementedError("default argument not implemented")

    if n < 0 and order_by is None:
        raise NotImplementedError(
                "must explicitly pass order_by when using last or nth with "
                "n < 0 in SQL."
                )

    if n < 0:
        # e.g. -1 in python is 0, -2 is 1.
        n = abs(n + 1)
        order_by = order_by.desc()


    #  note the adjustment for 1-based index in SQL
    return CumlOver(
            sql.func.nth_value(x, n + 1),
            order_by = order_by,
            rows = (None, None)
            )


@nth.register
def _nth_sql_agg(codata: SqlColumnAgg, x, n, order_by = None, default = None) -> ClauseElement:
    raise NotImplementedError("nth, first, and last not available in summarize")

