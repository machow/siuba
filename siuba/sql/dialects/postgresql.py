# sqlvariant, allow defining 3 namespaces to override defaults
from ..translate import base_scalar, base_agg, base_win, SqlTranslator
import sqlalchemy.sql.sqltypes as sa_types
from sqlalchemy import sql

def sql_round(col, n):
    return sql.func.round(sql.cast(col, sa_types.Numeric()), n)

scalar = SqlTranslator(
        base_scalar,
        round = sql_round
        )

aggregate = SqlTranslator(
        base_agg
        )

window = SqlTranslator(
        base_win
        )

funcs = dict(scalar = scalar, aggregate = aggregate, window = window)
