from ..translate import SqlTranslator

from .postgresql import scalar, aggregate, window, funcs, PostgresqlColumn, PostgresqlColumnAgg

class RedshiftColumn(PostgresqlColumn): pass
class RedshiftColumnAgg(PostgresqlColumnAgg): pass

translator = SqlTranslator.from_mappings(
        scalar, window, aggregate,
        RedshiftColumn, RedshiftColumnAgg
        )
