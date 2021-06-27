# sqlvariant, allow defining 3 namespaces to override defaults
from ..translate import (
        SqlColumn, SqlColumnAgg, extend_base, win_agg,
        SqlTranslator
        )

from .base import base_scalar, base_agg, base_nowin
import sqlalchemy.sql.sqltypes as sa_types
from sqlalchemy import sql

# Custom dispatching in call trees ============================================

class SqliteColumn(SqlColumn): pass
class SqliteColumnAgg(SqlColumnAgg, SqliteColumn): pass

scalar = extend_base(
        base_scalar,
        )

aggregate = extend_base(
        base_agg
        )

window = extend_base(
        # TODO: should check sqlite version, since < 3.25 can't use windows
        base_nowin,
        sd = win_agg("stddev")
        )


translator = SqlTranslator.from_mappings(
        scalar, window, aggregate,
        SqliteColumn, SqliteColumnAgg
        )
