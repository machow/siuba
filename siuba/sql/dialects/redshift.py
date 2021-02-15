from ..translate import create_sql_translators

from .postgresql import funcs, PostgresqlColumn, PostgresqlColumnAgg

class RedshiftColumn(PostgresqlColumn): pass
class RedshiftColumnAgg(PostgresqlColumnAgg): pass

translators = create_sql_translators(funcs, RedshiftColumn, RedshiftColumnAgg)
