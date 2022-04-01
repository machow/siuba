import pandas as pd
import pytest

from datetime import timedelta, date, datetime
from string import ascii_letters
from pandas.testing import assert_frame_equal, assert_series_equal

from siuba.dply.verbs import bind_cols, bind_rows
from .helpers import data_frame

@pytest.mark.skip
def test_bind_cols_shallow_copies():
    # https://github.com/tidyverse/dplyr/blob/main/tests/testthat/test-bind.R#L3
    pass


@pytest.mark.skip
def test_bind_cols_lists():
    # see https://github.com/tidyverse/dplyr/issues/1104
    # the siuba analog would probably be dictionaries?
    exp = data_frame(x = 1, y = "a", z = 2)

    pass


# Note: omitting other bind_cols list-based tests

@pytest.mark.skip
def test_that_bind_cols_repairs_names():
    pass


@pytest.mark.skip
def test_that_bind_cols_honors_name_repair():
    pass


# rows ------------------------------------------------------------------------

@pytest.fixture
def df_var():
    today = date.today()
    now = datetime.now()
    return data_frame(
        l = [True, False, False],
        i = [1, 1, 2],
        d = [today + timedelta(days=i) for i in [1, 1, 2]],
        f = pd.Categorical(["a", "a", "b"]),
        n = [1.5, 1.5, 2.5],
        t = [now + timedelta(seconds=i) for i in [1, 1, 2]],
        c = ["a", "a", "b"],
    )


def test_bind_rows_equiv_to_concat(df_var):
    exp = pd.concat([df_var, df_var, df_var], axis=0)
    res = bind_rows(df_var, df_var, df_var)

    assert_frame_equal(res, exp)


def test_bind_rows_reorders_columns(df_var):
    new_order = list(df_var.columns[3::-1]) + list(df_var.columns[:3:-1])
    df_var_scramble = df_var[new_order]

    assert_frame_equal(
        bind_rows(df_var, df_var_scramble),
        bind_rows(df_var, df_var)
    )


@pytest.mark.skip
def test_bind_rows_ignores_null():
    pass


def test_bind_rows_list_columns():
    vals = [[1,2], [1,2,3]]

    dfl = data_frame(x = vals)
    res = bind_rows(dfl, dfl)

    exp = data_frame(x = vals*2, _index = [0,1]*2)

    assert_frame_equal(res, exp)


@pytest.mark.xfail
def test_bind_rows_list_of_dfs():
    # https://github.com/tidyverse/dplyr/issues/1389
    df = data_frame(x = 1)

    res = bind_rows([df, df], [df, df])
    assert length(res) == 4
    assert_frame_equal(res, bind_rows(*[df]*4))


def test_bind_rows_handles_dfs_no_rows():
    df1 = data_frame(x = 1, y = pd.Categorical(["a"]))
    df0 = df1.loc[pd.Index([]), :]

    assert_frame_equal(bind_rows(df0), df0)
    assert_frame_equal(bind_rows(df0, df0), df0)
    assert_frame_equal(bind_rows(df0, df1), df1)


def test_bind_rows_handles_dfs_no_cols():
    df1 = data_frame(x = 1, y = pd.Categorical(["a"]))
    df0 = df1.loc[:,pd.Index([])]

    assert_frame_equal(bind_rows(df0), df0)
    assert bind_rows(df0, df0).shape == (2, 0)

@pytest.mark.skip
def test_bind_rows_lists_with_nulls():
    pass


@pytest.mark.skip
def test_bind_rows_lists_with_list_values():
    pass


def test_that_bind_rows_order_even_no_cols():
    df2 = data_frame(x = 2, y = "b")
    df1 = df2.loc[:, pd.Index([])]

    res = bind_rows(df1, df2).convert_dtypes()

    indx = [0,0]
    assert_series_equal(res.x, pd.Series([pd.NA, 2], index=indx, dtype="Int64", name="x"))
    assert_series_equal(res.y, pd.Series([pd.NA, "b"], index=indx, dtype="string", name="y"))


# Column coercion -------------------------------------------------------------

# Note: I think most of these are handled by pandas or unavoidable

@pytest.mark.xfail
def test_bind_rows_creates_column_of_identifiers():
    df = data_frame(x = [1,2,3], y = ["a", "b", "c"])
    data1 = df.iloc[1:,]
    data2 = df.iloc[:1,]

    out = bind_rows(data1, data2, _id = "col")

    # Note: omitted test of bind_rows(list(...))

    assert out.columns[0] == "col"

    # TODO(question): should it use 0 indexing? Would say yes, since then it just
    # corresponds to the arg index
    assert (out.col == ["0", "0", "1"]).all()

    out_labelled = bind_rows(zero = data1, one = data2)
    assert out_labelled.col == ["zero", "zero", "one"]


@pytest.mark.xfail
def test_bind_cols_accepts_null():
    df1 = data_frame(a = list(range(10)), b = list(range(10)))
    df2 = data_frame(c = list(range(10)), d = list(range(10)))

    res1 = bind_cols(df1, df2)
    res2 = bind_cols(None, df1, df2)
    res3 = bind_cols(df1, None, df2)
    res4 = bind_cols(df1, df2, None)

    assert_frame_equal(res1, res2)
    assert_frame_equal(res1, res3)
    assert_frame_equal(res1, res4)


@pytest.mark.skip
def test_bind_rows_handles_0_len_named_list():
    pass


@pytest.mark.xfail
def test_bind_rows_infers_classes_from_first_result():
    # TODO(question): is this what pd.concat does? DataFrames are subclassable..
    pass


@pytest.mark.skip
def test_bind_rows_sub_df_columns():
    pass


@pytest.mark.xfail
def test_bind_rows_handles_rowwises_vectors():
    tbl = bind_rows(
        data_frame(a = "foo", b = "bar"),
        dict(a = "A", b = "B"),
    )

    assert_frame_equal(tbl, data_frame(a = ["foo", "A"], b = ["bar", "B"]))


@pytest.mark.skip
def test_bind_rows_lists_of_df_like_lists():
    # I think this mostly exists because R has to use do.call(...), while
    # python can easily splat with *[...]
    pass


def test_bind_rows_handles_lists():
    # see https://github.com/tidyverse/dplyr/issues/1104
    [dict(x = 1, y = "a"), dict(x = 2, y = "b")]


# Vectors ---------------------------------------------------------------------

# Note: seems like bind_col tests here are overkill?
# bind_cols vector features are similar to mutate

