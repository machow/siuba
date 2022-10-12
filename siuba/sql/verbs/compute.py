from siuba.dply.verbs import collect

from ..backend import LazyTbl
from ..utils import _FixedSqlDatabase, _is_dialect_duckdb, MockConnection

# collect ----------

@collect.register(LazyTbl)
def _collect(__data, as_df = True):
    # TODO: maybe remove as_df options, always return dataframe

    if isinstance(__data.source, MockConnection):
        # a mock sqlalchemy is being used to show_query, and echo queries.
        # it doesn't return a result object or have a context handler, so
        # we need to bail out early
        return

    # compile query ----

    if _is_dialect_duckdb(__data.source):
        # TODO: can be removed once next release of duckdb fixes:
        # https://github.com/duckdb/duckdb/issues/2972
        query = __data.last_select
        compiled = query.compile(
            dialect = __data.source.dialect,
            compile_kwargs = {"literal_binds": True}
        )
    else:
        compiled = __data.last_select

    # execute query ----

    with __data.source.connect() as conn:
        if as_df:
            sql_db = _FixedSqlDatabase(conn)

            if _is_dialect_duckdb(__data.source):
                # TODO: pandas read_sql is very slow with duckdb.
                # see https://github.com/pandas-dev/pandas/issues/45678
                # going to handle here for now. address once LazyTbl gets
                # subclassed per backend.
                duckdb_con = conn.connection.c
                return duckdb_con.query(str(compiled)).to_df()
            else:
                #
                return sql_db.read_sql(compiled)

        return conn.execute(compiled)
