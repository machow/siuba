"""
Note: this test file was heavily influenced by its dbplyr counterpart.

https://github.com/tidyverse/dbplyr/blob/master/tests/testthat/test-verb-mutate.R
"""
    
from siuba import _, group_by, summarize, count

import pytest
from .helpers import assert_equal_query, data_frame, backend_notimpl, backend_sql

DATA = data_frame(x = [1,2,3,4], g = ['a', 'a', 'b', 'b'])

@pytest.fixture(scope = "module")
def df(backend):
    return backend.load_df(DATA)

@pytest.mark.parametrize("query, output", [
    (count(_.g), data_frame(g = ['a', 'b'], n = [2, 2])),
    (count("g"), data_frame(g = ['a', 'b'], n = [2, 2])),
    (count("x", "g"), DATA.assign(n = 1)),
    (count(_.x, "g"), DATA.assign(n = 1))
    ])
def test_basic_count(df, query, output):
    assert_equal_query(df, query, output)


@pytest.mark.skip("TODO: sql fix unnamed expression labels in count (#69)")
def test_count_with_expression(df):
    assert_equal_query(
            df,
            count(_.x - _.x),
            pd.DataFrame({"x - x": [0], "n": [4]})
            )


@pytest.mark.skip("TODO: sql support kwargs in count (#68)")
def test_count_with_kwarg_expression(df):
    assert_equal_query(
            df,
            count(y = _.x - _.x),
            pd.DataFrame({"y": [0], "n": [4]})
            )

