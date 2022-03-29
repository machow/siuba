"""
Note: this test file was heavily influenced by its dbplyr counterpart.

https://github.com/tidyverse/dbplyr/blob/master/tests/testthat/test-verb-filter.R
"""
    
from siuba import _, filter, group_by, arrange, summarize
from siuba.dply.vector import row_number, desc
import pandas as pd

import pytest

from .helpers import assert_equal_query, data_frame, backend_notimpl, backend_sql

DATA = pd.DataFrame({
    "x": [1,1,1,1],
    "y": [1,1,2,2],
    "z": [1,2,1,2]
    })


def test_filter_basic(backend):
    df = data_frame(x = [1,2,3,4,5], y = [5,4,3,2,1])
    dfs = backend.load_df(df)

    assert_equal_query(dfs, filter(_.x > 3), df[lambda _: _.x > 3])

def test_filter_basic_two_args(backend):
    df = data_frame(x = [1,2,3,4,5], y = [5,4,3,2,1])
    dfs = backend.load_df(df)

    assert_equal_query(dfs, filter(_.x > 3, _.y < 2), df[lambda _: (_.x > 3) & (_.y < 2)])

def test_filter_via_group_by(backend):
    df = data_frame(
            x = range(1, 11),
            g = [1]*5 + [2]*5
            )

    dfs = backend.load_df(df)

    assert_equal_query(
            dfs,
            # arrange is required to ensure order in sql dbs
            arrange(_.x) >> group_by(_.g) >> filter(row_number(_) < 3),
            data_frame(x = [1,2,6,7], g = [1,1,2,2])
            )


def test_filter_via_group_by_agg(backend):
    dfs = backend.load_df(x = range(1,11), g = [1]*5 + [2]*5)

    assert_equal_query(
            dfs,
            group_by(_.g) >> filter(_.x > _.x.mean()),
            data_frame(x = [4, 5, 9, 10], g = [1, 1, 2, 2])
            )


def test_filter_via_group_by_agg_two_args(backend):
    dfs = backend.load_df(x = range(1,11), g = [1]*5 + [2]*5)

    assert_equal_query(
            dfs,
            group_by(_.g) >> filter(_.x > _.x.mean(), _.x != _.x.max()),
            data_frame(x = [4, 9], g = [1, 2])
            )


@backend_sql("TODO: pandas - implement arrange over group by")
def test_filter_via_group_by_arrange(backend):
    dfs = backend.load_df(x = [3,2,1] + [2,3,4], g = [1]*3 + [2]*3)

    assert_equal_query(
            dfs,
            group_by(_.g) >> arrange(_.x) >> filter(_.x.cumsum() > 3),
            data_frame(x = [3, 3, 4], g = [1, 2, 2])
            )

@backend_sql("TODO: pandas - implement arrange over group by")
def test_filter_via_group_by_desc_arrange(backend):
    dfs = backend.load_df(x = [3,2,1] + [2,3,4], g = [1]*3 + [2]*3)

    assert_equal_query(
            dfs,
            group_by(_.g) >> arrange(desc(_.x)) >> filter(_.x.cumsum() > 3),
            data_frame(x = [2, 1, 4, 3, 2], g = [1, 1, 2, 2, 2])
            )

def test_filter_before_summarize(backend):
    dfs = backend.load_df(x = [1,2,3], g = ["a", "b", "b"])

    assert_equal_query(
            dfs,
            filter(_.x > 2) >> summarize(z=_.x.mean()),
            data_frame(z = [3]),
            # sql backends vary in the type .mean() returns
            check_dtype=False
            )

def test_filter_before_summarize_grouped(backend):
    dfs = backend.load_df(x = [1,2,3], g = ["a", "a", "b"])

    assert_equal_query(
            dfs,
            group_by(_.g) >> filter(_.x.mean() > 2) >> summarize(z=_.x.mean()),
            data_frame(g = ["b"], z = [3]),
            # sql backends vary in the type .mean() returns
            check_dtype=False
            )

