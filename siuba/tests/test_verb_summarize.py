"""
Note: this test file was heavily influenced by its dbplyr counterpart.

https://github.com/tidyverse/dbplyr/blob/master/tests/testthat/test-verb-mutate.R
"""
    
from siuba import _, mutate, select, group_by, summarize, filter, show_query
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
def test_ungrouped_summarize_literal(df):
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
    assert list(q.last_op.alias().columns.keys()) == ["g", "n"]


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


def test_summarize_validates_length():
    with pytest.raises(ValueError):
        summarize(data_frame(x = [1,2]), res = _.x + 1)


def test_frame_mode_returns_many():
    # related to length validation above
    with pytest.raises(ValueError):
        df = data_frame(x = [1, 2, 3])
        res = summarize(df, result = _.x.mode())


def test_summarize_removes_series_index():
    # Note: currently wouldn't work in postgresql, since _.x + _.y not an agg func
    df = data_frame(g = ['a', 'b', 'c'], x = [1,2,3], y = [4,5,6])

    assert_equal_query(
            df,
            group_by(_.g) >> summarize(res = _.x + _.y),
            df.assign(res = df.x + df.y).drop(columns = ["x", "y"])
            )
            

@backend_sql
def test_summarize_subquery_group_vars(backend, df):
    query = mutate(g2 = _.g.str.upper()) >> group_by(_.g2) >> summarize(low = _.x.min())
    assert_equal_query(
            df,
            query,
            data_frame(g2 = ['A', 'B'], low = [1, 3])
            )

    # check that is uses a subquery, since g2 is defined in first query
    text = str(query(df).last_op)
    assert text.count('FROM') == 2


@backend_sql
def test_summarize_subquery_op_vars(backend, df):
    query = mutate(x2 = _.x + 1) >> group_by(_.g) >> summarize(low = _.x2.min())
    assert_equal_query(
            df,
            query,
            data_frame(g = ['a', 'b'], low = [2, 4])
            )

    # check that is uses a subquery, since x2 is defined in first query
    text = str(query(df).last_op)
    assert text.count('FROM') == 2

