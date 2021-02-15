from ..translate import create_sql_translators

from .postgresql import scalar, aggregate, window, funcs, PostgresqlColumn, PostgresqlColumnAgg

class RedshiftColumn(PostgresqlColumn): pass
class RedshiftColumnAgg(PostgresqlColumnAgg): pass

translator = create_sql_translators(
        scalar, aggregate, window,
        RedshiftColumn, RedshiftColumnAgg
        )
