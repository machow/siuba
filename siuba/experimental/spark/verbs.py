from siuba.dply.verbs import collect
from siuba.sql.verbs import LazyTbl
from siuba.sql.utils import mock_sqlalchemy_engine

# TODO: still need to define a set of core properties for a remote table interface,
#       so in the meantime, use inheritance to implement spark like it were postgres.
#       alternatively, could add a compile method.
class SparkTbl(LazyTbl):
    def __init__(self, source, tbl, columns = None, *args, _spark_source = None, **kwargs):
        mock_engine = mock_sqlalchemy_engine("postgresql")
        super().__init__(mock_engine, tbl, columns, *args, **kwargs)

        self._spark_source = source if _spark_source is None else _spark_source


@collect.register(SparkTbl)
def _collect_spark_tbl(__data, as_df = True):
    query = __data.last_op
    compiled = query.compile(
        dialect = __data.source.dialect,
        compile_kwargs = {"literal_binds": True}
    )

    res = __data._spark_source.sql(str(compiled))
    if as_df:
        return res.toPandas()

    return res


