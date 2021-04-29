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

@desc.register(ClauseElement)
def _desc_sql(x) -> ClauseElement:
    """
    Example:
        >>> print(desc(sql.column('a')))
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
    rank_func = getattr(sql.func, func_name)

    def f(col, na_option = None) -> RankOver:
        if na_option == "keep":
            return _sql_rank_over(rank_func, col, partition, nulls_last)

        warn_arg_default(func_name, 'na_option', None, "keep")

        return RankOver(rank_func(), order_by = col)

    return f


dense_rank  .register(ClauseElement, _sql_rank("dense_rank"))
percent_rank.register(ClauseElement, _sql_rank("percent_rank"))
cume_dist   .register(ClauseElement, _sql_rank("cume_dist", partition = True))
min_rank    .register(ClauseElement, _sql_rank("rank", partition = True))


dense_rank  .register(SqliteColumn, win_absent("DENSE_RANK"))
percent_rank.register(SqliteColumn, win_absent("PERCENT_RANK"))
cume_dist   .register(SqliteColumn, win_absent("CUME_DIST"))
min_rank    .register(SqliteColumn, win_absent("MIN_RANK"))

# partition everything, since MySQL puts NULLs first
# see: https://stackoverflow.com/q/1498648/1144523
dense_rank  .register(MysqlColumn, _sql_rank("dense_rank", partition = True))
percent_rank.register(MysqlColumn, _sql_rank("percent_rank", partition = True))
cume_dist   .register(MysqlColumn, _sql_rank("cume_dist", partition = True))
min_rank    .register(MysqlColumn, _sql_rank("rank", partition = True))

# partition everything, since MySQL puts NULLs first
# see: https://stackoverflow.com/q/1498648/1144523
dense_rank  .register(BigqueryColumn, _sql_rank("dense_rank", nulls_last = True))
percent_rank.register(BigqueryColumn, _sql_rank("percent_rank", nulls_last = True))



# row_number ------------------------------------------------------------------

@row_number.register(ClauseElement)
def _row_number_sql(col) -> CumlOver:
    """
    Example:
        >>> print(row_number(sql.column('a')))
        row_number() OVER ()
        
    """
    return CumlOver(sql.func.row_number())

row_number.register(SqliteColumn, win_absent("ROW_NUMBER"))

# between ---------------------------------------------------------------------

@between.register(ClauseElement)
def _between_sql(x, left, right, default = None) -> ClauseElement:
    """
    Example:
        >>> print(between(sql.column('a'), 1, 2))
        a BETWEEN :a_1 AND :a_2

        >>> print(between(sql.column('a'), 1, 2, default = False))
        coalesce(a BETWEEN :a_1 AND :a_2, :coalesce_1)

    """
    
    if default is not False:
        # TODO: warn
        pass

    if default is None:
        return x.between(left, right)

    return sql.functions.coalesce(x.between(left, right), default)

# coalesce --------------------------------------------------------------------

@coalesce.register(ClauseElement)
def _coalesce_sql(x, *args) -> ClauseElement:
    """
    Example:
        >>> print(coalesce(sql.column('a'), sql.column('b')))
        coalesce(a, b)

        >>> coalesce(1, sql.column('a'))
        Traceback (most recent call last):
            ...
        TypeError: ...
    """
    return sql.functions.coalesce(x, *args)


# lead and lag ----------------------------------------------------------------

@lead.register(ClauseElement)
def _lead_sql(x, n = 1, default = None) -> ClauseElement:
    """
    Example:
        >>> print(lead(sql.column('a'), 2, 99))
        lead(a, :lead_1, :lead_2) OVER ()
    """
    
    f = win_cumul("lead", rows=None)
    return f(x, n, default)

@lag.register(ClauseElement)
def _lag_sql(x, n = 1, default = None) -> ClauseElement:
    """
    Example:
        >>> print(lag(sql.column('a'), 2, 99))
        lag(a, :lag_1, :lag_2) OVER ()
    """
    f = win_cumul("lag", rows=None)
    return f(x, n , default)


# n ---------------------------------------------------------------------------

@n.register(ClauseElement)
@n.register(ImmutableColumnCollection)
def _n_sql(x) -> ClauseElement:
    """
    Example:
        >>> print(n(sql.column('a')))
        count(*) OVER ()
    """
    return AggOver(sql.func.count())

@n.register(SqlColumnAgg)
def _n_sql_agg(x) -> ClauseElement:
    """
    Example:
        >>> from siuba.sql.translate import SqlColumnAgg
        >>> print(n(SqlColumnAgg('x')))
        count(*)
    """

    return sql.func.count()


n.register(SqliteColumn, win_absent("N"))
row_number.register(SqliteColumn, win_absent("ROW_NUMBER"))

# n_distinct ------------------------------------------------------------------

@n_distinct.register(ClauseElement)
def _n_distinct_sql(x) -> ClauseElement:
    """
    Example:
        >>> print(n_distinct(sql.column('a')) )
        count(distinct(a))
    """
    return sql.func.count(sql.func.distinct(x))


# na_if -----------------------------------------------------------------------

@na_if.register(ClauseElement)
def _na_if_sql(x, y) -> ClauseElement:
    """
    Example:
        >>> print(na_if(sql.column('x'), 2))
        nullif(x, :nullif_1)
    """
    return sql.func.nullif(x, y)


# nth, first, last ------------------------------------------------------------
# note: first and last wrap around nth, so are not dispatchers.
#       this may need to change this in the future, since this means they won't
#       show their own name, when you print, e.g. first(_.x)

@nth.register(ClauseElement)
def _nth_sql(x, n, order_by = None, default = None) -> ClauseElement:
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


@nth.register(SqlColumnAgg)
def _nth_sql_agg(x, n, order_by = None, default = None) -> ClauseElement:
    raise NotImplementedError("nth, first, and last not available in summarize")

