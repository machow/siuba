from ..translate import SqlTranslator

from .postgresql import PostgresqlColumn, PostgresqlColumnAgg

class RedshiftColumn(PostgresqlColumn): pass
class RedshiftColumnAgg(PostgresqlColumnAgg, RedshiftColumn): pass

translator = SqlTranslator.from_mappings(
        RedshiftColumn, RedshiftColumnAgg
        )
