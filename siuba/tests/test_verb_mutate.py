"""
Note: this test file was heavily influenced by its dbplyr counterpart.

https://github.com/tidyverse/dbplyr/blob/master/tests/testthat/test-verb-mutate.R
"""
    
from siuba import _, mutate, select, group_by, summarize, filter, arrange
from siuba.dply.vector import row_number

import pytest
from .helpers import assert_equal_query, data_frame, backend_notimpl, backend_sql
from string import ascii_lowercase 

DATA = data_frame(a = [1,2,3], b = [9,8,7])

@pytest.fixture(scope = "module")
def dfs(backend):
    return backend.load_df(DATA)

@pytest.mark.parametrize("query, output", [
    (mutate(x = _.a + _.b), DATA.assign(x = [10, 10, 10])),
    pytest.param( mutate(x = _.a + _.b) >> summarize(ttl = _.x.sum()), data_frame(ttl = 30.0), marks = pytest.mark.skip("TODO: failing sqlite?")),
    (mutate(x = _.a + 1, y = _.b - 1), DATA.assign(x = [2,3,4], y = [8,7,6])),
    (mutate(x = _.a + 1) >> mutate(y = _.b - 1), DATA.assign(x = [2,3,4], y = [8,7,6])),
    (mutate(x = _.a + 1, y = _.x + 1), DATA.assign(x = [2,3,4], y = [3,4,5]))
    ])
def test_mutate_basic(dfs, query, output):
    assert_equal_query(dfs, query, output)

@pytest.mark.parametrize("query, output", [
    (mutate(x = 1), DATA.assign(x = 1)),
    (mutate(x = "a"), DATA.assign(x = "a")),
    (mutate(x = 1.2), DATA.assign(x = 1.2))
    ])
def test_mutate_literal(dfs, query, output):
    assert_equal_query(dfs, query, output)


def test_select_mutate_filter(dfs):
    assert_equal_query(
            dfs,
            select(_.x == _.a) >> mutate(y = _.x * 2) >> filter(_.y == 2),
            data_frame(x = 1, y = 2)
            )

@backend_sql
def test_mutate_smart_nesting(backend, dfs):
    # y and z both use x, so should create only 1 extra query
    lazy_tbl = dfs >> mutate(x = _.a + 1, y = _.x + 1, z = _.x + 1)

    # should have form
    # SELECT ..., x + 1 as y, x + 1 as z FROM (
    # SELECT ..., a + 1 as x FROM
    # <TABLENAME>) some_alias
    inner_alias = lazy_tbl.last_op.froms[0]
    inner_select = inner_alias.element
    orig_table = inner_select.froms[0]

    assert orig_table is lazy_tbl.tbl


def test_mutate_reassign_column_ordering(dfs):
    assert_equal_query(
            dfs,
            mutate(c = 3, a = 1),
            data_frame(a = [1,1,1], b = [9,8,7], c = [3,3,3])
            )

@pytest.mark.skip("TODO: in SQL this returns a table with 1 row")
def test_mutate_reassign_all_cols_keeps_rowsize(dfs):
    assert_equal_query(
            dfs,
            mutate(a = 1, b = 2),
            data_frame(a = [1,1,1], b = [2,2,2])
            )

@backend_sql
@backend_notimpl("sqlite")
def test_mutate_window_funcs(backend):
    data = data_frame(idx = range(0, 4), x = range(1, 5), g = [1,1,2,2])
    dfs = backend.load_df(data)
    assert_equal_query(
            dfs,
            arrange(_.idx) >> group_by(_.g) >> mutate(row_num = row_number(_).astype(float)),
            data.assign(row_num = [1., 2, 1, 2])
            )


@backend_notimpl("sqlite")
def test_mutate_using_agg_expr(backend):
    data = data_frame(x = range(1, 5), g = [1,1,2,2])
    dfs = backend.load_df(data)
    assert_equal_query(
            dfs,
            group_by(_.g) >> mutate(y = _.x - _.x.mean()),
            data.assign(y = [-.5, .5, -.5, .5])
            )

@backend_sql # TODO: pandas outputs a int column
@backend_notimpl("sqlite")
def test_mutate_using_cuml_agg(backend):
    data = data_frame(idx = range(0, 4), x = range(1, 5), g = [1,1,2,2])
    dfs = backend.load_df(data)

    # cuml window without arrange before generates warning
    with pytest.warns(None):
        assert_equal_query(
                dfs,
                arrange(_.idx) >> group_by(_.g) >> mutate(y = _.x.cumsum()),
                data.assign(y = [1, 3, 3, 7]),
                check_dtype=False       # bigquery returns int, postgres float
                )

def test_mutate_overwrites_prev(backend):
    # TODO: check that query doesn't generate a CTE
    dfs = backend.load_df(data_frame(x = range(1, 5), g = [1,1,2,2]))
    assert_equal_query(
            dfs,
            mutate(x = _.x + 1) >> mutate(x = _.x + 1),
            data_frame(x = [3,4,5,6], g = [1,1,2,2])
            )



