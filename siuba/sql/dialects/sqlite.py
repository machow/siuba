# sqlvariant, allow defining 3 namespaces to override defaults
from ..translate import (
        SqlColumn, SqlColumnAgg, SqlTranslations, win_agg,
        create_sql_translators
        )

from .base import base_scalar, base_agg, base_nowin
import sqlalchemy.sql.sqltypes as sa_types
from sqlalchemy import sql

# Custom dispatching in call trees ============================================

class SqliteColumn(SqlColumn): pass
class SqliteColumnAgg(SqlColumnAgg, SqliteColumn): pass

scalar = SqlTranslations(
        base_scalar,
        )

aggregate = SqlTranslations(
        base_agg
        )

window = SqlTranslations(
        # TODO: should check sqlite version, since < 3.25 can't use windows
        base_nowin,
        sd = win_agg("stddev")
        )

funcs = dict(scalar = scalar, aggregate = aggregate, window = window)

translator = create_sql_translators(
        scalar, aggregate, window,
        SqliteColumn, SqliteColumnAgg
        )
