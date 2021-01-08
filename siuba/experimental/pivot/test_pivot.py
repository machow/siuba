"""
Note: this test file was heavily influenced by its dbplyr counterpart.

https://github.com/tidyverse/tidyr/blob/master/tests/testthat/test-pivot-long.R
"""

# TODO: pivot_longer rows in different order from gather. Currently using
#       a row order invariant form of test. Should fix to conform to pivot_longer.
# TODO: need to be careful about index. Should preserve original indices for rows.
# TODO: Current behaviour of pivot_longer is to preserve the index. Therefore
#       it needs to be reset for any assertion.

from . import pivot_longer, pivot_longer_spec

import pytest
import pandas as pd
import numpy as np

from siuba.siu import Symbolic
from siuba import group_by

from siuba.tests.helpers import data_frame, assert_frame_sort_equal
from pandas.testing import assert_frame_equal, assert_series_equal


_ = Symbolic()


def test_pivot_all_cols_to_long():
    "can pivot all cols to long"

    src = data_frame(x = [1,2], y = [3,4])
    dst = data_frame(name = ["x", "y", "x", "y"], value = [1, 3, 2, 4])
    
    res = pivot_longer(src, _["x":"y"])

    assert_frame_equal(res.reset_index(drop=True), dst)


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


def test_preserves_original_keys():
    df = data_frame(x = [1,2], y = [2,2], z = [1,2])
    pv = pivot_longer(df, _["y":"z"])

    assert pv.columns.tolist() == ["x", "name", "value"]
    assert_series_equal(
        pv["x"],
        pd.Series(df["x"].repeat(2))
        )


def test_can_drop_missing_values():
    df = data_frame(x = [1, np.nan], y = [np.nan, 2])
    pv = pivot_longer(df, _["x":"y"], values_drop_na=True)

    assert pv["name"].tolist() == ["x", "y"]
    assert pv["value"].tolist() == [1, 2]


def test_can_handle_missing_combinations():
    df = data_frame(id = ["A", "B"], x_1 = [1, 3], x_2 = [2, 4], y_2 = ["a", "b"])
    pv = pivot_longer(df, -_.id, names_to = ("_value", "n"), names_sep = "_")

    pv_expected = pd.Series([np.nan, "a", np.nan, "b"],
                            index = [0, 0, 1, 1],
                            name = 'y')

    assert pv.columns.tolist() == ["id", "n", "x", "y"]
    assert pv["x"].tolist() == [1, 2, 3, 4]
    pd.testing.assert_series_equal(pv["y"], pv_expected)


@pytest.mark.xfail
def test_mixed_columns_are_auto_coerced():
    # TODO: pandas stack (and melt) coerces categorical data when stacking.
    df = data_frame(x = pd.Categorical(["a"]), y = pd.Categorical(["b"]))
    pv = pivot_longer(df, _["x":"y"])

    assert_series_equal(pv["value"], pd.Categorical(['a', 'b']))


def test_can_override_default_output_col_type():
    df = data_frame(x = "x", y = 1)
    pv = pivot_longer(df, _["x":"y"], values_transform = {"value": list})

    assert pv["value"].tolist() == [["x"], [1]]


@pytest.mark.xfail
def test_spec_can_pivot_to_multi_measure_cols():
    df = data_frame(x = "x", y = 1)
    sp = data_frame(_name = ["x", "y"], _value = ["X", "Y"], row = [1, 1])

    pv = pivot_longer_spec(df, sp)

    assert pv.columns.tolist() == ["row", "X", "Y"]
    assert pv["X"] == "x"
    assert pv["Y"] == 1


@pytest.mark.xfail
def test_original_col_order_is_preserved():
    df = data_frame(id = ["A", "B"],
        z_1 = [1, 7], y_1 = [2, 8], x_1 = [3, 9],
        z_2 = [4, 10], y_2 = [5, 11], x_2 = [6, 12]
    )
    pv = pivot_longer(df, -_.id, names_to = ("_value", "n"), names_sep = "_")

    assert pv.columns.tolist() == ["id", "n", "z", "y", "x"]


def test_handles_duplicate_column_names():
    # Cannot initiate data_frame with duplicate keys
    # df = data_frame(x = 1, a = 1, a = 2, b = 3, b = 4)
    df = pd.DataFrame.from_records(
        [(1, 1, 2, 3, 4)], columns = ["x", "a", "a", "b", "b"]
    )
    pv = pivot_longer(df, -_.x)

    assert pv.columns.tolist() == ["x", "name", "value"]
    assert pv["name"].tolist() == ["a", "a", "b", "b"]
    assert pv["value"].tolist() == [1, 2, 3, 4]


def test_can_pivot_duplicate_names_to_value():
    df = data_frame(x = 1, a_1 = 1, a_2 = 2, b_1 = 3, b_2 = 4)
    pv1 = pivot_longer(df, -_.x, names_to = ("_value", np.nan), names_sep = "_")
    pv2 = pivot_longer(df, -_.x, names_to = ("_value", np.nan), names_pattern = "(.)_(.)")
    pv3 = pivot_longer(df, -_.x, names_to = "_value", names_pattern = "(.)_.")

    assert pv1.columns.tolist() == ["x", "a", "b"]
    assert pv1["a"].tolist() == [1, 2]
    assert_frame_equal(pv2, pv1)
    assert_frame_equal(pv3, pv1)


def test_value_can_be_any_pos_in_names_to():
    samp = data_frame(
        i = np.arange(1, 5),
        y_t1 = np.random.standard_normal(4),
        y_t2 = np.random.standard_normal(4),
        z_t1 = [3] * 4,
        z_t2 = [-2] * 4,
    )

    value_first = pivot_longer(samp, -_.i,
                               names_to = ("_value", "time"), names_sep = "_")

    samp2 = samp.rename(columns={"y_t1": "t1_y", "y_t2": "t2_y",
                                 "z_t1": "t1_z", "z_t2": "t2_z"})
    
    value_second = pivot_longer(samp2, -_.i,
                                names_to = ("time", "_value"), names_sep = "_")
    
    assert_frame_equal(value_first, value_second)


def test_type_error_message_uses_var_names():
    # Error handling is by default 'better' in python than R
    # This test is tricky, as python doesn't care when stacking data of different types.
    df = data_frame(abc = 1, xyz = "b")
    try:
        # This should by default pivot everything, as with tidyr
        pivot_longer(df, _[:])
    except:
        # Ideally we'd print an error message here and compare if the keys are
        # printed correctly, but `pivot_longer` doesn't brake in python as in R
        # when stacking different data types. Not sure if we should 'make' it brake?
        print(err)


def test_grouping_is_preserved():
    # In siuba this actually tests 3 things:
    # 1) Can we pipe a grouped DataFrame in to `pivot_longer`? - Not yet
    # 2) Does it retain grouping? - Not yet
    # 3) Can we get the names of grouping _columns_ ie variables? - Not yet?
    df = data_frame(g = [1, 2], x1 = [1, 2], x2 = [3, 4])
    # Breaks; `pivot_longer` needs singledispatch for grouped DataFrames
    out = (
        df
        >> group_by(_.g)
        >> pivot_longer(_["x1":"x2"], names_to = "x", values_to = "v")
    )

    # Breaks, as group_vars does not exist yet.
    # For now, in pandas it is probably better to check if the DataFrame remains
    # grouped, and if it matches the expected output
    # assert group_vars(out) == "g"
    expected = data_frame(
        g = [1, 1, 2, 2],
        x = ["x1", "x2", "x1", "x2"],
        v = [1, 3, 2, 4],
        _index = [0, 0, 1, 1]
    ).groupby("g")

    # assert_frame_equal does not work with DataFrameGroupBy.
    isinstance(out, expected.__class__)
    assert_frame_equal(out.obj, expected.obj)
