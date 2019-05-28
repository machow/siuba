from siuba.dply.verbs import simple_varname
from siuba import _, filter, group_by, arrange, mutate
from siuba.dply.vector import row_number, desc
import pandas as pd

import pytest

from .helpers import assert_equal_query, data_frame, backend_notimpl, backend_sql


@backend_sql
@backend_notimpl("sqlite")
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


