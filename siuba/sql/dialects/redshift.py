from ..translate import SqlTranslator

from .postgresql import PostgresqlColumn, PostgresqlColumnAgg

class RedshiftColumn(PostgresqlColumn): pass
class RedshiftColumnAgg(PostgresqlColumnAgg): pass

translator = SqlTranslator.from_mappings(
        {}, {}, {},
        RedshiftColumn, RedshiftColumnAgg
        )
