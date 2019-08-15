from siuba import _, transmute, group_by

import pytest
from .helpers import assert_equal_query, data_frame, backend_notimpl, backend_sql

DATA = data_frame(a = [1,2,3], b = [9,8,7])

@pytest.fixture(scope = "module")
def dfs(backend):
    return backend.load_df(DATA)


def test_transmute_basic(dfs):
    assert_equal_query(dfs, transmute(x = _.a + 1), data_frame(x = [2,3,4]))

def test_transmute_grouped(dfs):
    assert_equal_query(
            dfs,
            group_by(_.a) >> transmute(x = _.a + 1),
            data_frame(a = [1,2,3], x = [2,3,4])
            )

@pytest.mark.skip("TODO (#111)")
def test_transmute_grouped_no_mutate_grouping(dfs):
    assert_equal_query(
            dfs,
            group_by(_.a) >> transmute(a = _.a + 1),
            data_frame(a = [1,2,3], x = [2,3,4])
            )
