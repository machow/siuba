from siuba import _, group_by, mutate, filter, summarize
from siuba.sql import sql_raw
import sqlalchemy.exc

import pytest

from .helpers import assert_equal_query, data_frame, backend_sql

DATA = data_frame(x = ['a','a'], y = [1,2])

@pytest.fixture(scope = "module")
def df(backend):
    return backend.load_df(DATA)


@backend_sql
def test_raw_sql_mutate(backend, df):
    assert_equal_query(
            df,
            mutate(z = sql_raw("y + 1")),
            DATA.assign(z = lambda d: d.y + 1)
            )


@backend_sql
def test_raw_sql_mutate_grouped(backend, df):
    assert_equal_query(
            df,
            group_by("x") >> mutate(z = sql_raw("y + 1")),
            DATA.assign(z = lambda d: d.y + 1)
            )


@backend_sql
def test_raw_sql_mutate_refer_previous_raise_dberror(backend, df):
    # Note: unlikely will be able to support this case. Normally we analyze
    # the expression to know whether we need to create a subquery.
    with pytest.raises(sqlalchemy.exc.DatabaseError):
        assert_equal_query(
                df,
                group_by("x") >> mutate(z1 = sql_raw("y + 1"), z2 = sql_raw("z1 + 1")),
                DATA.assign(z1 = lambda d: d.y + 1, z2 = lambda d: d.z1 + 1)
                )

@backend_sql
def test_raw_sql_filter(backend, df):
    assert_equal_query(
            df,
            filter(sql_raw("y = 1")),
            data_frame(x = ['a'], y = [1])
            )

@backend_sql
def test_raw_sql_summarize(backend, df):
    assert_equal_query(
            df, 
            summarize(z = sql_raw("SUM(y)")) >> mutate(z = _.z.astype(int)),
            data_frame(z = [3])
            )

