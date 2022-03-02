# sqlvariant, allow defining 3 namespaces to override defaults
from ..translate import (
        SqlColumn, SqlColumnAgg, extend_base,
        SqlTranslator
        )

from .base import base_nowin

import sqlalchemy.sql.sqltypes as sa_types
from sqlalchemy import sql

# Custom dispatching in call trees ============================================

# Note that aggs do not inherit SqlColumnAgg, since we disable aggregate functions
# for sqlite. Could add them in, as recent versions support a wide range of aggs.
class SqliteColumn(SqlColumn): pass
class SqliteColumnAgg(SqlColumnAgg, SqliteColumn): pass

scalar = extend_base(
        SqliteColumn,
        )

aggregate = extend_base(
        SqliteColumnAgg,
        )

window = extend_base(
        SqliteColumn,
        **base_nowin
        # TODO: should check sqlite version, since < 3.25 can't use windows
        )


translator = SqlTranslator.from_mappings(
        SqliteColumn, SqliteColumnAgg
        )
