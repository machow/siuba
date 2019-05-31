"""
Note: this test file was heavily influenced by its dbplyr counterpart.

https://github.com/tidyverse/dbplyr/blob/master/tests/testthat/test-verb-select.R
"""
    
from siuba import _, mutate, select, group_by, rename

import pytest
from .helpers import assert_equal_query, data_frame, backend_notimpl, backend_sql
from string import ascii_lowercase 

DATA = data_frame(a = 1, b = 2, c = 3)

@pytest.fixture(scope = "module")
def dfs(backend):
    return backend.load_df(DATA)

@pytest.mark.parametrize("query, output", [
    ( select(_.c), data_frame(c = 3) ),
    ( select(_.b == _.c), data_frame(b = 3) ),
    ( select(_["a":"c"]), data_frame(a = 1, b = 2, c = 3) ),
    ( select(_[_.a:_.c]), data_frame(a = 1, b = 2, c = 3) ),
    ( select(_.a, _.b) >> select(_.b), data_frame(b = 2) ),
    ( mutate(a = _.b + _.c) >> select(_.a), data_frame(a = 5) ),
    pytest.param( group_by(_.a) >> select(_.b), data_frame(b = 2, a = 1), marks = pytest.mark.xfail),
    ])
def test_select_siu(dfs, query, output):
    assert_equal_query(dfs, query, output)


@pytest.mark.skip("TODO: #63")
def test_select_kwargs(dfs):
    assert_equal_query(dfs, select(x = _.a), data_frame(x = 1))


# Rename ----------------------------------------------------------------------

@pytest.mark.parametrize("query, output", [
    ( rename(A = _.a), data_frame(A = 1, b = 2, c = 3) ),
    ( rename(A = "a"), data_frame(A = 1, b = 2, c = 3) ),
    ( rename(A = _.a, B = _.c), data_frame(A = 1, b = 2, B = 3) ),
    ( rename(A = "a", B = "c"), data_frame(A = 1, b = 2, B = 3) )
    ])
def test_rename_siu(dfs, query, output):
    assert_equal_query(dfs, query, output)


@backend_sql("TODO: pandas - grouped df rename")
@pytest.mark.parametrize("query, output", [
    ( group_by(_.a) >> rename(z = _.a), data_frame(z = 1, b = 2, c = 3) ),
    ( group_by(_.a) >> rename(z = "a"), data_frame(z = 1, b = 2, c = 3) )
    ])
def test_grouped_rename_siu(backend, dfs, query, output):
    assert_equal_query(dfs, query, output)

