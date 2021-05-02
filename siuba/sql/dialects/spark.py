# sqlvariant, allow defining 3 namespaces to override defaults
from ..translate import (
        SqlColumn, SqlColumnAgg, 
        win_agg, sql_scalar, sql_agg
        )

from .base import base_scalar, base_agg, base_win

from .postgresql import PostgresqlColumn, PostgresqlColumnAgg

import sqlalchemy.sql.sqltypes as sa_types
from sqlalchemy import sql


# Custom dispatching in call trees ============================================

class SparkColumn(SqlColumn): pass

class SparkColumnAgg(SqlColumnAgg, PostgresqlColumn): pass


