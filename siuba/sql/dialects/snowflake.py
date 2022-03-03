from ..translate import SqlTranslator

from .postgresql import PostgresqlColumn, PostgresqlColumnAgg

class SnowflakeColumn(PostgresqlColumn): pass
class SnowflakeColumnAgg(PostgresqlColumnAgg, SnowflakeColumn): pass

translator = SqlTranslator.from_mappings(
        SnowflakeColumn, SnowflakeColumnAgg
        )

