"""
Note: this test file was heavily influenced by its dbplyr counterpart.

https://github.com/tidyverse/dbplyr/blob/master/tests/testthat/test-verb-group_by.R
"""
    
from siuba import (
        _, group_by,
        join, inner_join, left_join, right_join, full_join,
        semi_join, anti_join
        )
from siuba.dply.vector import row_number, n
from siuba.sql.verbs import collect

import pytest
from .helpers import assert_equal_query, assert_frame_sort_equal, data_frame, backend_notimpl, backend_sql


DF1 = data_frame(
        ii = [1,2,3,4],
        x = ["a", "b", "c", "d"]
        )

DF2 = data_frame(
        ii = [1,2,26],
        y = ["a", "b", "z"]
        )

DF3 = data_frame(
        ii = [26],
        z = ["z"]
        )

@pytest.fixture(scope = "module")
def df1(backend):
    return backend.load_df(DF1)

@pytest.fixture(scope = "module")
def df2(backend):
    return backend.load_df(DF2)

@pytest.fixture(scope = "module")
def df2_jj(backend):
    return backend.load_df(DF2.rename(columns = {"ii": "jj"}))

@pytest.fixture(scope = "module")
def df3(backend):
    return backend.load_df(DF3)



@backend_sql("TODO: pandas")
def test_join_diff_vars_keeps_left(backend, df1, df2_jj):
    out = inner_join(df1, df2_jj, {"ii": "jj"}) >> collect()

    assert out.columns.tolist() == ["ii", "x", "y"]

def test_join_on_str_arg(df1, df2):
    out = inner_join(df1, df2, "ii") >> collect()

    target = DF1.iloc[:2,].assign(y = ["a", "b"])
    assert_frame_sort_equal(out, target)

def test_join_on_list_arg(backend):
    # TODO: how to validate how cols are being matched up?
    data = DF1.assign(jj = lambda d: d.ii)
    df_a = backend.load_df(data)
    df_b = backend.load_df(DF2.assign(jj = lambda d: d.ii))
    out = inner_join(df_a, df_b, ["ii", "jj"]) >> collect()

    assert_frame_sort_equal(out, data.iloc[:2, :].assign(y = ["a", "b"]))

@pytest.mark.skip("TODO: note, unsure of this syntax")
def test_join_on_same_col_multiple_times():
    data = data_frame(ii = [1,2,3], jj = [1,2, 9])
    df_a = backend.load_df(data)
    df_b = backend.load_df(data_frame(ii = [1,2,3]))

    out = inner_join(df_a, df_b, {("ii", "jj"): "ii"}) >> collect()
    # keeps all but last row
    assert_frame_sort_equal(out, data.iloc[:2,])

def test_join_on_missing_col(df1, df2):
    with pytest.raises(KeyError):
        inner_join(df1, df2, {"ABCDEF": "ii"})

    with pytest.raises(KeyError):
        inner_join(df1, df2, {"ii": "ABCDEF"})

def test_join_suffixes_dupe_names(df1):
    out = inner_join(df1, df1, {"ii": "ii"}) >> collect()
    non_index_cols = DF1.columns[DF1.columns != "ii"]
    assert all((non_index_cols + "_x").isin(out))
    assert all((non_index_cols + "_y").isin(out))



# Test basic join types -------------------------------------------------------

def test_basic_left_join(df1, df2):
    out = left_join(df1, df2, {"ii": "ii"}) >> collect()
    target = DF1.assign(y = ["a", "b", None, None])
    assert_frame_sort_equal(out, target)

@backend_sql("TODO: pandas returns columns in rev name order")
def test_basic_right_join(backend, df1, df2):
    # same as left join, but flip df arguments
    out = right_join(df2, df1, {"ii": "ii"}) >> collect()
    target = DF1.assign(y = ["a", "b", None, None])
    assert_frame_sort_equal(out, target)

def test_basic_inner_join(df1, df2):
    out = inner_join(df1, df2, {"ii": "ii"}) >> collect()
    target = DF1.iloc[:2,:].assign(y = ["a", "b"])
    assert_frame_sort_equal(out, target)

@pytest.mark.skip_backend("sqlite")
def test_basic_full_join(skip_backend, backend, df1, df2):
    out = full_join(df1, df2, {"ii": "ii"}) >> collect()
    target = DF1.merge(DF2, on = "ii", how = "outer")
    assert_frame_sort_equal(out, target)

def test_basic_semi_join(backend, df1, df2):
    assert_frame_sort_equal(
            semi_join(df1, df2, {"ii": "ii"}) >> collect(),
            DF1.iloc[:2,]
            )

def test_semi_join_no_cross(backend, df1, df2):
    df_ii = backend.load_df(data_frame(ii = [1,1]))
    assert_frame_sort_equal(
            semi_join(df1, df_ii, {"ii": "ii"}) >> collect(),
            DF1.iloc[:1,]
            )

def test_basic_anti_join(backend, df1, df2):
    assert_frame_sort_equal(
            anti_join(df1, df2, on = {"ii": "ii"}) >> collect(),
            DF1.iloc[2:,]
            )

def test_basic_anti_join(backend, df1, df2):
    assert_frame_sort_equal(
            anti_join(df1, df2, on = {"ii": "ii"}) >> collect(),
            DF1.iloc[2:,]
            )

def test_basic_anti_join(backend, df1, df2):
    assert_frame_sort_equal(
            anti_join(df1, df2, on = {"ii": "ii", "x": "y"}) >> collect(),
            DF1.iloc[2:,]
            )
