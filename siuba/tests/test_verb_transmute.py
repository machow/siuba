from siuba import _, transmute

import pytest
from .helpers import assert_equal_query, data_frame, backend_notimpl, backend_sql

DATA = data_frame(a = [1,2,3], b = [9,8,7])

@pytest.fixture(scope = "module")
def dfs(backend):
    return backend.load_df(DATA)


def test_mutate_basic(dfs):
    assert_equal_query(dfs, transmute(x = _.a + 1), data_frame(x = [2,3,4]))
