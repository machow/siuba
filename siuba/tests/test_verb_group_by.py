"""
Note: this test file was heavily influenced by its dbplyr counterpart.

https://github.com/tidyverse/dbplyr/blob/master/tests/testthat/test-verb-group_by.R
"""
    
from siuba import _, group_by, ungroup, summarize
from siuba.dply.vector import row_number, n

import pytest
from .helpers import assert_equal_query, data_frame, backend_notimpl, SqlBackend
from string import ascii_lowercase 

DATA = data_frame(x = [1,2,3], y = [9,8,7], g = ['a', 'a', 'b'])

@pytest.fixture(scope = "module")
def df(backend):
    if not isinstance(backend, SqlBackend):
        pytest.skip("TODO: generalize tests to pandas")
    return backend.load_df(DATA)


def test_group_by_no_add(df):
    gdf = group_by(df, _.x, _.y)
    assert gdf.group_by == ("x", "y")

def test_group_by_override(df):
    gdf = df >> group_by(_.x, _.y) >> group_by(_.g)
    assert gdf.group_by == ("g",)

def test_group_by_add(df):
    gdf = group_by(df, _.x) >> group_by(_.y, add = True)

    assert gdf.group_by == ("x", "y")

def test_group_by_ungroup(df):
    q1 = df >> group_by(_.g)
    assert q1.group_by == ("g",)

    q2 = q1 >> ungroup()
    assert q2.group_by == tuple()


@pytest.mark.skip("TODO: need to test / validate joins first")
def test_group_by_before_joins(df):
    assert False

def test_group_by_performs_mutate(df):
    assert_equal_query(
            df,
            group_by(z = _.x + _.y) >> summarize(n = n(_)),
            data_frame(z = 10, n = 3)
            )

