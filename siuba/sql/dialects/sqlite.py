# sqlvariant, allow defining 3 namespaces to override defaults
from ..translate import (
        SqlColumn, SqlColumnAgg, SqlTranslator, win_agg,
        create_sql_translators
        )

from .base import base_scalar, base_agg, base_nowin
import sqlalchemy.sql.sqltypes as sa_types
from sqlalchemy import sql

# Custom dispatching in call trees ============================================

class SqliteColumn(SqlColumn): pass
class SqliteColumnAgg(SqlColumnAgg, SqliteColumn): pass

scalar = SqlTranslator(
        base_scalar,
        )

aggregate = SqlTranslator(
        base_agg
        )

window = SqlTranslator(
        # TODO: should check sqlite version, since < 3.25 can't use windows
        base_nowin,
        sd = win_agg("stddev")
        )

funcs = dict(scalar = scalar, aggregate = aggregate, window = window)

translators = create_sql_translators(funcs, SqliteColumn, SqliteColumnAgg)
