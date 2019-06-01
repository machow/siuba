"""
Note: this test file was heavily influenced by its dbplyr counterpart.

https://github.com/tidyverse/dbplyr/blob/master/tests/testthat/test-verb-mutate.R
"""
    
from siuba import _, mutate, select, group_by, summarize, filter
from siuba.dply.vector import row_number, n

import pytest
from .helpers import assert_equal_query, data_frame, backend_notimpl, backend_sql
from string import ascii_lowercase 

DATA = data_frame(x = [1,2,3,4], g = ['a', 'a', 'b', 'b'])

@pytest.fixture(scope = "module")
def df(backend):
    return backend.load_df(DATA)

@pytest.fixture(scope = "module")
def df_float(backend):
    return backend.load_df(DATA.assign(x = lambda d: d.x.astype(float)))

@pytest.fixture(scope = "module")
def gdf(df):
    return df >> group_by(_.g)


@pytest.mark.parametrize("query, output", [
    (summarize(y = n(_)), data_frame(y = 4)),
    (summarize(y = _.x.min()), data_frame(y = 1)),
    ])
def test_summarize_ungrouped(df, query, output):
    assert_equal_query(df, query, output)


@pytest.mark.skip("TODO: should return 1 row (#63)")
def test_ungrouped_summarize_literal(df, query, output):
    assert_equal_query(df, summarize(y = 1), data_frame(y = 1)) 


@backend_notimpl("sqlite")
def test_summarize_after_mutate_cuml_win(backend, df_float):
    assert_equal_query(
            df_float,
            mutate(y = _.x.cumsum()) >> summarize(z = _.y.max()),
            data_frame(z = [10.])
            )


@backend_sql
def test_summarize_keeps_group_vars(backend, gdf):
    q = gdf >> summarize(n = n(_))
    assert list(q.last_op.c.keys()) == ["g", "n"]


@pytest.mark.parametrize("query, output", [
    (summarize(y = 1), data_frame(g = ['a', 'b'], y = [1, 1])),
    (summarize(y = n(_)), data_frame(g = ['a', 'b'], y = [2,2])),
    (summarize(y = _.x.min()), data_frame(g = ['a', 'b'], y = [1, 3])),
    # TODO: same issue as above
    #(mutate(y = _.x.cumsum()) >> summarize(z = _.y.max()), data_frame(y = [3, 7]))
    ])
def test_summarize_grouped(gdf, query, output):
    assert_equal_query(gdf, query, output)


@pytest.mark.skip("TODO: (#48)")
def test_summarize_removes_1_grouping(backend):
    data = data_frame(a = 1, b = 2, c = 3)
    df = backend.load_df(data)

    q1 = df >> group_by(_.a, _.b) >> summarize(n = n(_))
    assert q1.group_by == ("a")

    q2 = q1 >> summarize(n = n(_))
    assert not len(q2.group_by)


@backend_sql("TODO: pandas -  need to implement or raise this warning")
def test_summarize_no_same_call_var_refs(backend, df):
    with pytest.raises(NotImplementedError):
        df >> summarize(y = _.x.min(), z = _.y + 1)


@backend_sql
def test_summarize_removes_order_vars(backend, df):
    lazy_tbl = df >> summarize(n = n(_))

    assert not len(lazy_tbl.order_by)


@pytest.mark.skip("TODO (see #50)")
def test_summarize_unnamed_args(df):
    assert_equal_query(
            df,
            summarize(n(_)),
            pd.DataFrame({'n(_)': 4})
            )

