"""
Note: this test file was heavily influenced by its dbplyr counterpart.

https://github.com/tidyverse/tidyr/blob/master/tests/testthat/test-pivot-long.R
"""

# TODO: pivot_longer rows in different order from gather. Currently using
#       a row order invariant form of test. Should fix to conform to pivot_longer.
# TODO: need to be careful about index. Should preserve original indices for rows.

from . import pivot_longer, pivot_longer_spec

import pytest
from siuba.siu import Symbolic
from siuba.tests.helpers import data_frame, assert_frame_sort_equal
from pandas.testing import assert_frame_equal, assert_series_equal

_ = Symbolic()


def test_pivot_all_cols_to_long():
    "can pivot all cols to long"

    src = data_frame(x = [1,2], y = [3,4])
    dst = data_frame(name = ["x", "y", "x", "y"], value = [1, 3, 2, 4])
    
    res = pivot_longer(src, _["x":"y"])

    assert_frame_sort_equal(res, dst)


@pytest.mark.xfail
def test_values_interleaved_correctly():
    # TODO: fix order issue
    df = data_frame(x = [1,2], y = [10, 20], z = [100, 200])

    pv = pivot_longer(df, _[0:3])
    assert pv["value"].tolist() == [1, 10, 100, 2, 20, 200]

@pytest.mark.xfail
def test_spec_add_multi_columns():
    df = data_frame(x = [1,2], y = [3,4])

    # TODO: is this the right format for a spec
    sp = data_frame(_name = ["x", "y"], _value = "v", a = 1, b = 2)
    pv = pivot_longer_spec(df, spec = sp)

    assert pv.columns.tolist() == ["a", "b", "v"]
    
@pytest.mark.xfail
def test_preserves_original_keys():
    df = data_frame(x = [1,2], y = 2, z = [1,2])
    pv = pivot_longer(df, _["y":"z"])

    assert pv.columns.tolist() == ["x", "name", "value"]
    assert assert_series_equal(
            pv["x"],
            pd.Series(df["x"].repeat(2))
            )

