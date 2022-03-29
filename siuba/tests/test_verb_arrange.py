from siuba.dply.verbs import simple_varname
from siuba import _, filter, group_by, arrange, mutate, ungroup
from siuba.dply.vector import row_number, desc
import pandas as pd

import pytest

from .helpers import assert_equal_query, data_frame, backend_notimpl, backend_sql

DATA = data_frame(x = [2,2,1], y = [2,1,1], z = ['z']*3)

@pytest.fixture(scope = "module")
def df(backend):
    return backend.load_df(DATA)


@pytest.mark.parametrize("query, output", [
    (arrange(_.x), DATA.sort_values(['x'])),
    (arrange("x"), DATA.sort_values(['x'])),
    (arrange(_.x, _.y), DATA.sort_values(['x', 'y'])),
    (arrange("x", "y"), DATA.sort_values(['x', 'y'])),
    (arrange(_.x, "y"), DATA.sort_values(['x', 'y']))
    ])
def test_basic_arrange(df, query, output):
    assert_equal_query(df, query, output)


@pytest.mark.parametrize("query, output", [
    (arrange(-_.x), DATA.sort_values(['x'], ascending = [False])),
    (arrange(-_.x, _.y), DATA.sort_values(['x', 'y'], ascending = [False, True])),
    (arrange(-_.x, "y"), DATA.sort_values(['x', 'y'], ascending = [False, True]))
    ])
def test_arrange_desc(df, query, output):
    assert_equal_query(df, query, output)


@pytest.mark.parametrize("query, output", [
    (arrange(_.x - _.x), DATA),
    (arrange(_.x * -1), DATA.sort_values(['x'], ascending = [False])),
    ])
def test_arrange_with_expr(df, query, output):
    assert_equal_query(df, query, output)


def test_arrange_grouped_trivial(df):
    # note: only 1 level for z
    assert_equal_query(
            df,
            group_by(_.z) >> arrange(_.x),
            DATA.sort_values(['x'])
            )

def test_arrange_grouped(backend, df):
    q = group_by(_.y) >> arrange(_.x)
    assert_equal_query(
            df,
            q,
            DATA.sort_values(['x'])
            )

    # arrange w/ mutate is the same, whether used before or after group_by
    assert_equal_query(
            df,
            q >> mutate(res = row_number(_)),
            mutate(DATA.sort_values(['x']).groupby('y'), res = row_number(_))
            )


# SQL -------------------------------------------------------------------------

@backend_sql
def test_no_arrange_before_cuml_window_warning(backend):
    data = data_frame(x = range(1, 5), g = [1,1,2,2])
    dfs = backend.load_df(data)
    with pytest.warns(RuntimeWarning):
        dfs >> mutate(y = _.x.cumsum())

@backend_sql
def test_arranges_back_to_back(backend):
    data = data_frame(x = range(1, 5), g = [1,1,2,2])
    dfs = backend.load_df(data)

    lazy_tbl = dfs >> arrange(_.x) >> arrange(_.g)
    order_by_vars = tuple(simple_varname(call) for call in lazy_tbl.order_by)

    assert order_by_vars == ("x", "g")
    assert [c.name for c in lazy_tbl.last_op._order_by_clause] == ["x", "g"]


