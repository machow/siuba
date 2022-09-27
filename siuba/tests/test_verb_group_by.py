"""
Note: this test file was heavily influenced by its dbplyr counterpart.

https://github.com/tidyverse/dbplyr/blob/master/tests/testthat/test-verb-group_by.R
"""
    
from siuba import _, group_by, ungroup, summarize, collect
from siuba.dply.vector import row_number, n
from siuba.dply import verbs

import pytest
from .helpers import assert_equal_query, data_frame, backend_notimpl, SqlBackend
from string import ascii_lowercase 

DATA = data_frame(x = [1,2,3], y = [9,8,7], g = ['a', 'a', 'b'])

@pytest.fixture(scope = "module")
def df(backend):
    if not isinstance(backend, SqlBackend):
        pytest.skip("TODO: generalize tests to pandas")
    return backend.load_df(DATA)


def test_group_by_two(df):
    gdf = group_by(df, _.x, _.y)
    assert gdf.group_by == ("x", "y")

def test_group_by_override(df):
    gdf = df >> group_by(_.x, _.y) >> group_by(_.g)
    assert gdf.group_by == ("g",)

def test_group_by_no_add(df):
    # without add argument, group_by overwrites prev grouping
    gdf1 = group_by(df, _.x)
    gdf2 = group_by(gdf1, _.y)

    assert gdf2.group_by == ("y",)

def test_group_by_add(df):
    gdf = group_by(df, _.x) >> group_by(_.y, add = True)

    assert gdf.group_by == ("x", "y")

def test_group_by_ungroup(df):
    q1 = df >> group_by(_.g)
    assert q1.group_by == ("g",)

    q2 = q1 >> ungroup()
    assert q2.group_by == tuple()

def test_group_by_using_string(df):
    gdf = group_by(df, "g") >> summarize(res = _.x.mean())
    

def test_group_by_performs_mutate(df):
    assert_equal_query(
            df,
            group_by(z = _.x + _.y) >> summarize(n = n(_)),
            data_frame(z = 10, n = 3)
            )


@pytest.mark.parametrize("query", [
    (verbs.rename(_, z = "y")),
    (verbs.mutate(_, z = _.x + 1)),
    (verbs.filter(_, _.x > 1)),
    (verbs.arrange(_, _.x)),
    (verbs.add_count(_, _.x)),
    (verbs.head(_)),
])
def test_group_by_equiv_ungrouped(backend, query):
    # TODO: check indexes
    df = data_frame(x = [1, 1, 2], y = [4, 5, 6], g = ["a", "a", "a"])
    src = backend.load_df(df)
    
    assert_equal_query(
        src,
        group_by(_.g) >> query,
        src >> query >> collect()
    )


@pytest.mark.parametrize("query", [
    (verbs.select(_, _.x, _.y)),
    (verbs.transmute(_, z = _.x + 1)),
    (verbs.summarize(_, avg = _.x.mean())),
    (verbs.count(_, _.x)),
    (verbs.distinct(_, _.x)),
])
def test_group_by_equiv_with_groupings(backend, query):
    df = data_frame(x = [1, 1, 2], y = [4, 5, 6], g = ["a", "a", "a"])
    src = backend.load_df(df)

    res = df >> query >> collect()
    res.insert(0, "g", "a")

    assert_equal_query(
        src,
        group_by(_.g) >> query,
        res
    )


@pytest.mark.parametrize("f_join", [
    (verbs.inner_join),
    (verbs.left_join),
    (verbs.right_join),
    (verbs.full_join),
    (verbs.semi_join),
    (verbs.anti_join),
])
def test_group_by_equiv_joins(xfail_backend, backend, f_join):
    if f_join is verbs.full_join and backend.name in {"sqlite", "mysql"}:
        pytest.xfail()

    df = data_frame(id = [1, 2, 3], val = ['lhs.1', 'lhs.2', 'lhs.3'])
    df2 = data_frame(id = [1, 2, 4], val = ['rhs.1', 'rhs.2', 'rhs.3'])

    src = backend.load_df(df)
    src2 = backend.load_df(df2)

    dst = src >> f_join(_, src2, by = "id") >> collect()

    assert_equal_query(
        src,
        group_by(_.id) >> f_join(_, src2, by = "id"),
        dst
    )
    

