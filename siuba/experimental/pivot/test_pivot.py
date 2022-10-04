"""
Note: this test file was heavily influenced by its dbplyr counterpart.

https://github.com/tidyverse/tidyr/blob/master/tests/testthat/test-pivot-long.R
"""

# TODO: need to be careful about index. Should preserve original indices for rows.
# TODO: test when original input has a multi-index (should be okay)

from . import pivot_longer, pivot_longer_spec, build_longer_spec

import pytest
import pandas as pd
import numpy as np

from siuba.siu import Symbolic
from siuba import group_by, collect, show_query

from siuba.tests.helpers import data_frame, assert_equal_query, assert_frame_sort_equal
from pandas.testing import assert_frame_equal, assert_series_equal


_ = Symbolic()



def assert_equal_query2(left, right, *args, sql_ordered=False, sql_kwargs=None, **kwargs):
    from siuba.sql import LazyTbl
    from siuba.sql.utils import _is_dialect_duckdb
    from pandas.core.groupby import DataFrameGroupBy
    out = collect(left)

    if isinstance(left, LazyTbl) and _is_dialect_duckdb(left.source):
        # TODO: find a nice way to remove duckdb specific code from here
        # duckdb does not use pandas.DataFrame.to_sql method, which coerces
        # everything to 64 bit. So we need to coerce any results it returns
        # as 32 bit to 64 bit, to match to_sql.
        int_cols = out.select_dtypes('int32').columns
        flt_cols = out.select_dtypes('float32').columns
        out[int_cols] = out[int_cols].astype('int64')
        out[flt_cols] = out[flt_cols].astype('float64')

    if isinstance(left, LazyTbl):
        if sql_kwargs:
            final_kwargs = {**kwargs, **sql_kwargs}
        else:
            final_kwargs = kwargs
        if not sql_ordered:
            assert_frame_sort_equal(out, right, *args, **final_kwargs)
        else:
            assert_frame_equal(out, right, *args, **final_kwargs)
    else:
        assert_frame_equal(out, right, *args, **kwargs)


def assert_series_equal2(left, right, *args, sql_ordered=True, **kwargs):
    from siuba.sql import LazyTbl

    if isinstance(left, LazyTbl):
        left = collect(left)

        if not sql_ordered:
            left = left.sort_values()
            right = right.sort_values()

        assert_series_equal(
            left.reset_index(drop=True),
            right.reset_index(drop=True),
            *args,
            **kwargs
        )
    else:
        assert_series_equal(left, right, *args, **kwargs)


    
def test_pivot_all_cols_to_long(backend):
    "can pivot all cols to long"

    src = backend.load_df(data_frame(x = [1,2], y = [3,4]))
    dst = data_frame(name = ["x", "y", "x", "y"], value = [1, 3, 2, 4])
    
    query = pivot_longer(_, _["x":"y"])

    assert_equal_query(src, query, dst)


def test_values_interleaved_correctly():
    df = data_frame(x = [1,2], y = [10, 20], z = [100, 200])

    pv = pivot_longer(df, _[0:3])
    assert pv["value"].tolist() == [1, 10, 100, 2, 20, 200]


def test_spec_add_multi_columns():
    # TODO: SQL backends
    df = data_frame(x = [1,2], y = [3,4])

    sp = data_frame(**{".name": ["x", "y"], ".value": "v", "a": 1, "b": 2})
    pv = pivot_longer_spec(df, spec = sp)

    assert pv.columns.tolist() == ["a", "b", "v"]


def test_preserves_original_keys(backend):
    src = data_frame(x = [1,2], y = [2,2], z = [1,2])
    dst = data_frame(x = [1, 1, 2, 2], name = ["y", "z"]*2, value = [2,1,2,2])
    dst.index = [0, 0, 1, 1]

    remote = backend.load_df(src)
        
    pv = pivot_longer(remote, _["y":"z"])

    assert_equal_query2(pv, dst)


def test_can_drop_missing_values(backend):
    src = backend.load_df(
        data_frame(x = [1, np.nan], y = [np.nan, 2])
    )
    dst = data_frame(name = ["x", "y"], value = [1, 2])
    pv = pivot_longer(src, _["x":"y"], values_drop_na=True)

    # TODO: sql databases sometimes return float. we need assertions to take
    # backend (or sql vs pandas) specific options.
    assert_equal_query2(pv, dst, check_dtype=False)


def test_can_handle_missing_combinations(backend):
    df = data_frame(id = ["A", "B"], x_1 = [1, 3], x_2 = [2, 4], y_2 = ["a", "b"])
    dst = data_frame(
        id = ["A", "A", "B", "B"],
        n = ["1", "2"]*2,
        x = [1, 2, 3, 4],
        y = [np.nan, "a", np.nan, "b"]
    )
    dst.index = [0, 0, 1, 1]

    src = backend.load_df(df)
    pv = pivot_longer(src, -_.id, names_to = (".value", "n"), names_sep = "_")

    assert_equal_query2(pv, dst)


@pytest.mark.xfail
def test_mixed_columns_are_auto_coerced():
    # TODO: pandas stack (and melt) coerces categorical data when stacking.
    df = data_frame(x = pd.Categorical(["a"]), y = pd.Categorical(["b"]))
    pv = pivot_longer(df, _["x":"y"])

    assert_series_equal(pv["value"], pd.Categorical(['a', 'b']))


def test_can_override_default_output_col_type():
    df = data_frame(x = "x", y = 1)
    pv = pivot_longer(df, _["x":"y"], values_transform = {"value": lambda x: x.astype("str")})

    assert pv["value"].tolist() == ["x", "1"]


def test_spec_can_pivot_to_multi_measure_cols(backend):
    df = backend.load_df(data_frame(x = "x", y = 1))
    sp = data_frame(**{".name": ["x", "y"], ".value": ["X", "Y"], "row": [1, 1]})

    pv = collect(pivot_longer_spec(df, sp))

    assert pv.columns.tolist() == ["row", "X", "Y"]
    assert pv["X"].tolist() == ["x"]
    assert pv["Y"].tolist() == [1]


def test_original_col_order_is_preserved(backend):
    df = data_frame(
        id = ["A", "B"],
        z_1 = [1, 7], y_1 = [2, 8], x_1 = [3, 9],
        z_2 = [4, 10], y_2 = [5, 11], x_2 = [6, 12]
    )
    src = backend.load_df(df)
    pv = collect(
        pivot_longer(src, -_.id, names_to = (".value", "n"), names_sep = "_")
    )

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


def test_can_pivot_duplicate_names_to_value(backend):
    df = data_frame(x = 1, a_1 = 1, a_2 = 2, b_1 = 3, b_2 = 4)
    src = backend.load_df(df)

    pv1 = pivot_longer(df, -_.x, names_to = (".value", np.nan), names_sep = "_")
    pv2 = pivot_longer(df, -_.x, names_to = (".value", np.nan), names_pattern = "(.)_(.)")
    pv3 = pivot_longer(df, -_.x, names_to = ".value", names_pattern = "(.)_.")

    assert pv1.columns.tolist() == ["x", "a", "b"]
    assert pv1["a"].tolist() == [1, 2]
    assert_frame_equal(pv2, pv1)
    assert_frame_equal(pv3, pv1)


def test_value_can_be_any_pos_in_names_to():
    # TODO: should work in SQL
    samp = data_frame(
        i = np.arange(1, 5),
        y_t1 = np.random.standard_normal(4),
        y_t2 = np.random.standard_normal(4),
        z_t1 = [3] * 4,
        z_t2 = [-2] * 4,
    )

    value_first = pivot_longer(samp, -_.i,
                               names_to = (".value", "time"), names_sep = "_")

    samp2 = samp.rename(columns={"y_t1": "t1_y", "y_t2": "t2_y",
                                 "z_t1": "t1_z", "z_t2": "t2_z"})
    
    value_second = pivot_longer(samp2, -_.i,
                                names_to = ("time", ".value"), names_sep = "_")
    
    assert_frame_equal(value_first, value_second)


@pytest.mark.skip("TODO")
def test_type_error_message_uses_var_names():
    """type error message use variable names"""
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


@pytest.mark.skip("wontdo?")
def test_values_ptypes_raises_variable_name():
    assert False


@pytest.mark.skip("wontdo?")
def test_names_ptypes_raises_names_to_names():
    assert False


def test_error_overwriting_existing_column():
    src = data_frame(x = 1, y = 2)

    # TODO: test exact message
    with pytest.raises(ValueError) as exc_info:
        pivot_longer(src, _.y, names_to = "x")

    pv = pivot_longer(src, _.y, names_to = "x", names_repair="unique")
    assert_frame_equal(
        pv,
        data_frame(x___0=1, x___1="y", value=2)
    )

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


def test_zero_row_frames_work():
    src = pd.DataFrame({"x": [], "y": []}, dtype=int)
    pv = pivot_longer(src, _[_.x, _.y])

    assert_frame_equal(
        pv,
        pd.DataFrame({
            "name": np.array([], dtype="object"),
            "value": np.array([], dtype="int")
        })
    )


@pytest.mark.skip("unreleased in tidyr")
def test_cols_vary_adjust_result_row_order():
    src = data_frame(x = [1, 2], y = [3, 4])

    assert_frame_equal(
        pivot_longer(src, _[_.x, _.y], cols_vary = "fastest"),
        data_frame(name = ["x", "y", "x", "y"], value = [1, 3, 2, 4])
    )

    assert_frame_equal(
        pivot_longer(src, _[_.x, _.y], cols_vary = "slowest"),
        data_frame(name = ["x", "x", "y", "y"], value = [1, 2, 3, 4])
    )


@pytest.mark.skip("unreleased in tidyr")
def test_cols_vary_works_with_id_cols():
    """"`cols_vary` works with id columns not part of the pivoting process"""
    src = data_frame(id = ["a", "b"], x = [1, 2], y = [3, 4])

    pv = pivot_longer(src, _[_.x, _.y], cols_vary = "fastest")

    assert list(pv["id"]) == ["a", "a", "b", "b"]
    assert_frame_equal(
        pv[["name", "value"]],
        pivot_longer(src[["x", "y"]], _[_.x, _.y], cols_vary = "fastest")
    )


    pv2 = pivot_longer(src, _[_.x, _.y], cols_vary = "slowest")

    assert pv2["id"] == ["a", "b", "a", "b"]
    assert_frame_equal(
        pv2,
        pivot_longer(src[["x", "y"]], _[_.x, _.y], cols_vary = "slowest")
    )


@pytest.mark.skip("unreleased in tidyr")
def test_cols_vary_works_with_values_drop_na():
    src = data_frame(id = ["a", "b"], x = [1, None], y = [3, 4])

    assert_frame_equal(
        pivot_longer(src, _[_.x, _.y], cols_vary="slowest", values_drop_na = True),
        data_frame(id = ["a", "a", "b"], name = ["x", "y", "y"], value = [1, 3, 4])
    )


# spec ------------------------------------------------------------------------

def test_spec_validates_input():
    src = data_frame(x = 1)

    # TODO: more specific check: class = "vctrs_error_assert"
    with pytest.raises(Exception):
        build_longer_spec(src, _.x, values_to = ["a", "b"])


@pytest.mark.xfail
def test_names_to_empty_no_names_in_result():
    src = data_frame(x = 1)

    assert_frame_equal(
        build_longer_spec(src, _.x, names_to = []),
        spec,
        data_frame({".name": "x", ".value": "value"})
    )

    assert_frame_equal(
        build_longer_spec(src, names_to = None),
        spec
    )


    # TODO: test what happens when you pivot this? should result in...
    # data_frame(x = 1)


@pytest.mark.xfail
def test_multiple_names_requires_name_sep_or_pattern():
    """multiple names requires names_sep/names_pattern"""

    src = data_frame(x_y = 1)

    # TODO: assert this raises an error
    build_longer_spec(src, _.x_y, names_to = c("a", "b"))

    # TODO: assert this raises an error
    build_longer_spec(src, _.x_y, names_to = c("a", "b"), names_sep = "x", names_pattern = "x")


def test_spec_names_sep():
    src = data_frame(x_y = 1)
    spec = build_longer_spec(src, _.x_y, names_to = ["a", "b"], names_sep = "_")

    assert_frame_equal(
        spec,
        data_frame(**{".name": "x_y", ".value": "value", "a": "x", "b": "y"})
    )


def test_spec_names_sep_fails_single_name():
    src = data_frame(x_y = 1)

    # TODO: better test of exception
    with pytest.raises(Exception):
        build_longer_spec(df, _.x_y, names_to = "x", names_sep = "_")


def test_names_pattern_generates_correct_spec():
    src = data_frame(zx_y = 1)
    spec = build_longer_spec(
        src,
        _.zx_y,
        names_to = ["a", "b"],
        names_pattern = "z(.)_(.)"
    )

    dst = data_frame(**{".name": "zx_y", ".value": "value", "a": "x", "b": "y"})
    assert_frame_equal(
        spec,
        dst
    )

    spec2 = build_longer_spec(src, _.zx_y, names_to = "a", names_pattern = "z(.)")
    assert_frame_equal(
        spec2,
        dst.drop(columns="b")
    )


def test_names_to_can_override_value_to():
    src = data_frame(x_y = 1)
    spec = build_longer_spec(src, _.x_y, names_to = ["a", ".value"], names_sep = "_")

    assert_frame_equal(
        spec,
        # TODO: tidyr puts .value after a -- is that a bug?
        data_frame(**{".name": "x_y", ".value": "y", "a": "x"})
    )


def test_names_prefix_strips_off_from_beginning():
    src = data_frame(zzyz = 1)
    spec = build_longer_spec(src, _.zzyz, names_prefix = "z")

    assert_frame_equal(
        spec,
        data_frame(**{".name": "zzyz", ".value": "value", "name": "zyz"})
    )


def test_spec_can_cast_to_custom_type():
    src = data_frame(w1 = 1)
    spec = build_longer_spec(
        src,
        _.w1,
        names_prefix = "w",
        names_transform = {"name": lambda x: x.astype(int)}
    )

    assert spec["name"].dtype == "int64"


@pytest.mark.skip("wontdo? uses ptype")
def test_spec_transform_applied_before_cast():
    src = data_frame(w1 = 1)
    assert False


@pytest.mark.skip("wontdo? uses ptype")
def test_names_ptype_works_with_single_value():
    assert False


def test_names_transform_works_with_single_value():
    src = data_frame(**{"1x2": 1})
    spec = build_longer_spec(
        src,
        _["1x2"],
        names_to = ["one", "two"],
        names_sep = "x",
        names_transform = lambda x: x.astype(int)
    )

    assert list(spec["one"]) == [1]
    assert list(spec["two"]) == [2]


@pytest.mark.skip("wontdo? ptype")
def test_values_ptypes_works_with_single_empty_ptypes():
    assert False


def test_values_transform_works_with_single_functions():
    src = data_frame(x_1 = 1, y_1 = 2)
    pv = pivot_longer(
        src, _[:],
        names_to = [".value", "set"],
        names_sep = "_",
        values_transform = lambda x: x.astype(str)
    )


@pytest.mark.skip("wontdo? ptypes")
def test_names_and_values_ptypes_empty_dict():
    """`names/values_ptypes = list()` is currently the same as `NULL` (#1296)"""
    assert False


def test_error_if_col_no_match():
    src = data_frame(x = 1, y = 2)

    # TODO: better exception handling
    with pytest.raises(Exception):
        pivot_longer(src, _.startswith("z"))


def test_names_to_is_validated():
    src = data_frame(x = 1)

    # TODO: better exception testing
    with pytest.raises(Exception):
        build_longer_spec(src, _.x, names_to = 1)

    with pytest.raises(Exception):
        build_longer_spec(src, _.x, names_to = ["x", "y"])

    with pytest.raises(Exception):
        build_longer_spec(src, _.x, names_to = ["x", "y"], names_sep="_", names_pattern="x")
    

@pytest.mark.skip("wontdo? uses ptypes")
def test_that_names_ptypes_is_validated():
    assert False


def test_names_transform_is_validated():
    src = data_frame(x = 1)

    with pytest.raises(Exception):
        build_longer_spec(src, _.x, names_transform = 1)

    with pytest.raises(Exception):
        build_longer_spec(src, _.x, names_transform = [lambda x: x])


@pytest.mark.skip("wontdo? ptypes")
def test_values_ptypes_is_validated():
    assert False


def test_values_transform_is_validated():
    src = data_frame(x = 1)
    
    with pytest.raises(Exception):
        pivot_longer(src, _.x, values_transform = 1)

    with pytest.raises(Exception):
        pivot_longer(src, _.x, values_transform = [lambda x: x])


def test_cols_vary_is_validated():
    src = data_frame(x = 1)

    with pytest.raises(Exception):
        pivot_longer(src, _.x, cols_vary = "fast")

    with pytest.raises(Exception):
        pivot_longer(src, _.x, cols_vary = 1)
