import numpy as np
import pandas as pd
import pytest

from pandas.testing import assert_frame_equal, assert_series_equal

from siuba.dply.verbs import collect
from siuba.siu import Symbolic
from siuba.tests.helpers import data_frame
from . import pivot_wider, pivot_wider_spec, build_wider_spec

from .test_pivot import assert_equal_query2, assert_series_equal2

_ = Symbolic()

def test_pivot_all_cols(backend):
    # TODO: SQL - bigquery distinct not ordered, so col order is out
    src = backend.load_df(
        data_frame(key = ["x", "y", "z"], val = [1, 2, 3])
    )
    dst = data_frame(x = 1, y = 2, z = 3)

    pv = pivot_wider(src, names_from=_.key, values_from=_.val)

    # Note: duckdb is ordered
    assert_equal_query2(pv, dst, sql_kwargs = {"check_like": True}, sql_ordered=True)


def test_pivot_id_cols_default_preserve(backend):
    src = backend.load_df(
        data_frame(a = 1, key = ["x", "y"], val = [1, 2])
    )
    dst = data_frame(a = [1], x = [1], y = [2])

    pv = pivot_wider(src, names_from = _.key, values_from = _.val)

    # Note: duckdb is ordered
    assert_equal_query2(pv, dst, sql_kwargs = {"check_like": True}, sql_ordered=True)


def test_pivot_implicit_missings_to_explicit():
    src = data_frame(a = [1, 2], key = ["x", "y"], val = [1, 2])
    dst = data_frame(a = [1, 2], x = [1., None], y = [None, 2.])

    pv = pivot_wider(src, names_from = _.key, values_from = _.val)

    assert_equal_query2(pv, dst, sql_ordered=True)


def test_pivot_implicit_missings_to_explicit_from_spec(backend):
    df = data_frame(a = [1, 2], key = ["x", "y"], val = [1, 2])
    dst = data_frame(a = [1, 2], x = [1., None], y = [None, 2.])

    sp = pd.DataFrame({".name": ["x", "y"], ".value": ["val"]*2, "key": ["x", "y"]})

    src = backend.load_df(df)
    pv = pivot_wider_spec(src, sp)

    assert_equal_query2(
        pv,
        dst,
        check_dtype=False,
        # Note: duckdb is ordered
        sql_ordered=False,
    )


@pytest.mark.skip_backend("bigquery")
def test_error_overwriting_existing_column(skip_backend, backend):
    # TODO: SQL - bigquery distinct not ordered, so col order is out
    src = backend.load_df(
        data_frame(a = [1, 1], key = ["a", "b"], val = [1, 2])
    )
    
    # a bunch of snapshot tests...
    # should error
    with pytest.raises(ValueError) as exc_info:
        pivot_wider(src, names_from = _.key, values_from = _.val)

    assert "1 duplicate name(s)" in exc_info.value.args[0]

    pv = collect(
        pivot_wider(src, names_from = _.key, values_from = _.val, names_repair = "unique")
    )

    # TODO: note that our names start with 0-based indexing
    assert list(pv.columns) == ["a___0", "a___1", "b"]


@pytest.mark.skip_backend("bigquery")
def test_names_repair_unique(skip_backend, backend):
    src = backend.load_df(
        data_frame(test = ["a", "b"], name = ["test", "test2"], value = [1, 2])
    )
    dst = data_frame(test_0 = ["a", "b"], test_1 = [1., None], test2_2 = [None, 2.])

    pv = pivot_wider(src, names_repair=lambda x: [f"{v}_{ii}" for ii, v in enumerate(x)])

    assert_equal_query2(pv, dst, sql_ordered=True)


def test_names_repair_minimal():
    src = data_frame(test = ["a", "b"], name = ["test", "test2"], value = [1, 2])
    dst = pd.DataFrame(
        [
            ["a", 1., None],
            ["b", None, 2.]
        ],
        columns = ["test", "test", "test2"]
    )

    pv = pivot_wider(src, names_repair = "minimal")

    assert_frame_equal(pv, dst)


def test_grouping_preserved():
    gdf = data_frame(g = 1, k = "x", v = 2).groupby("g")

    pv = pivot_wider(gdf, names_from = _.k, values_from = _.v)

    assert isinstance(pv, pd.core.groupby.DataFrameGroupBy)


@pytest.mark.skip_backend("bigquery")
def test_weird_column_name_select(skip_backend, backend):
    src = backend.load_df(
        pd.DataFrame({"...8": ["x", "y", "z"], "val": [1, 2, 3]})
    )
    dst = data_frame(x = 1, y = 2, z = 3)

    pv = pivot_wider(src, names_from = _["...8"], values_from = _.val)

    assert_equal_query2(pv, dst, sql_kwargs = {"check_like": True}, sql_ordered=True)


@pytest.mark.skip("Won't do")
def test_data_frame_columns_pivot_correctly():
    # this concept doesn't exist in pandas. here is the R code for frame construction.
    # note that the columns x and y belong to a side-by-side data.frame
    # df <- tibble(
    #   i = c(1, 2, 1, 2),
    #   g = c("a", "a", "b", "b"),
    #   d = tibble(x = 1:4, y = 5:8)
    # )
    pass


def test_names_from_required_error(backend):
    # column name doesn't exist
    src = backend.load_df(
        data_frame(key = "x", val = 1)
    )

    with pytest.raises(ValueError) as exc_info:
        pivot_wider(src, values_from = _.val)

    assert "name" in exc_info.value.args[0]


def test_values_from_required_error(backend):
    src = backend.load_df(
        data_frame(key = "x", val = 1)
    )

    with pytest.raises(ValueError) as exc_info:
        pivot_wider(src, names_from = _.key)

    assert "value" in exc_info.value.args[0]


def test_names_from_no_match_error(backend):
    src = backend.load_df(
        data_frame(key = "x", val = 1)
    )

    with pytest.raises(ValueError) as exc_info:
        pv = pivot_wider(src, names_from = _.startswith("foo"), values_from = _.val)

    assert "`names_from` must" in exc_info.value.args[0]


def test_values_from_no_match_error(backend):
    src = backend.load_df(
        data_frame(key = "x", val = 1)
    )

    with pytest.raises(ValueError) as exc_info:
        pv = pivot_wider(src, names_from = _.key, values_from = _.startswith("foo"))

    assert "`values_from` must" in exc_info.value.args[0]


def test_values_fn_informative_error_pandas_fastpath():
    src = data_frame(name = ["a", "a"], value = [1, 2])

    with pytest.raises(ValueError) as exc_info:
        pivot_wider(src, values_fn = {"value": "cumsum"})

    assert "bug in pandas" in exc_info.value.args[0]
    assert "cumsum" in exc_info.value.args[0]


def test_values_fn_informative_error_lambda():
    src = data_frame(name = ["a", "a"], value = [1, 2])

    with pytest.raises(ValueError) as exc_info:
        pivot_wider(src, values_fn = {"value": lambda x: x.cumsum()})

    assert "Must produce aggregated value" in exc_info.value.args[0]


@pytest.mark.skip("TODO: pivot from spec")
def test_pivot_manual_spec_with_columns_that_dont_identify_rows():
    ## Looking for `x = 1L`
    #spec <- tibble(.name = "name", .value = "value", x = 1L)
    #
    ## But that doesn't exist here...
    #df <- tibble(key = "a", value = 1L, x = 2L)
    #expect_identical(
    #  pivot_wider_spec(df, spec, id_cols = key),
    #  tibble(key = "a", name = NA_integer_)
    #)
    #
    ## ...or here
    #df <- tibble(key = character(), value = integer(), x = integer())
    #expect_identical(
    #  pivot_wider_spec(df, spec, id_cols = key),
    #  tibble(key = character(), name = integer())
    #)
    #})
    pass


def test_pivot_manual_spec_zero_rows():
    spec = pd.DataFrame({".name": ["name"], ".value": "value", "x": 1})
    src = data_frame(value = [], x = [])

    assert_frame_equal(
        pivot_wider_spec(src, spec),
        pd.DataFrame({"name": []}, dtype = "object")
    )


def test_names_expand_basic():
    """can use `names_expand` to get sorted and expanded column names (#770)"""

    name1 = pd.Categorical([None, "x"], ["x", "y"])
    src = data_frame(name1 = name1, name2 = ["c", "d"], value = [1, 2])

    assert_frame_equal(
        pivot_wider(src, names_from = _[_.name1, _.name2], names_expand=True),
        data_frame(x_c = None, x_d = 2, y_c = None, y_d = None, nan_c = 1, nan_d = None)
    )


def test_names_expand_fills_implicit_missings():
    """can fill only implicit missings from `names_expand`"""
    name1 = pd.Categorical([None, "x"], ["x", "y"])
    src = data_frame(name1 = name1, name2 = ["c", "d"], value = [1, None])

    pv = pivot_wider(
        src,
        names_from = _[_.name1, _.name2],
        names_expand = True,
        values_fill = 0
    )

    assert_frame_equal(
        pv,
        data_frame(x_c = 0, x_d = np.nan, y_c = 0, y_d = 0, nan_c = 1., nan_d = 0)
    )


def test_id_expand_and_names_expand_works_with_zero_row_frames():
    src = data_frame(
        id = pd.Categorical([], ["b", "a"]),
        name = pd.Categorical([], ["a", "b"]),
        value = np.array([], dtype="int")
    )

    pv = pivot_wider(src, names_expand=True, id_expand=True)

    assert_frame_equal(
        pv,
        data_frame(
            id=["b", "a"],        # TODO: should preserve categorical
            a=[None, None],       # TODO: surprising these are not float64?
            b=[None, None]
        )
    )


# Column names ================================================================

@pytest.mark.xfail
def test_names_glue_basic(backend):
    # TODO: switch to use build_spec
    # TODO: prefix internal spec names with .
    src = backend.load_df(
        data_frame(x = ["X", "Y"], y = [1, 2], a = [1, 2], b = [1, 2])
    )

    pv = collect(
        pivot_wider(src, names_from=_[_.x, _.y], values_from=_[_.a, _.b], names_glue="{x}{y}_{.value}")
    )
    assert list(pv.columns) == ["X1_a", "Y2_a", "X1_b", "Y2_b"]
 

def test_names_sort_basic():
    # TODO: use spec
    from siuba.dply.forcats import fct_inorder
    src = data_frame(
        num = [1, 3, 2],
        fac = pd.Categorical(["Mon", "Wed", "Tue"], ["Mon", "Tues", "Wed"]),
        value = ["a", "b", "c"],
    )

    # should have cols ordered by position in the week
    pivot_wider(names_from = _[_.num, _.fac], names_sort = True)


def test_names_vary_slowest():
    src = data_frame(name = ["name1", "name2"], value1 = [1, 2], value2 = [4, 5])

    kwargs = {"names_from": _.name, "values_from": _[_.value1, _.value2]}

    pv_fastest = pivot_wider(src, **kwargs)
    assert (
        pv_fastest.columns.tolist() ==
            ["value1_name1", "value1_name2", "value2_name1", "value2_name2"]
    )

    pv_slowest = pivot_wider(src, names_vary="slowest", **kwargs)
    assert (
        pv_slowest.columns.tolist() ==
            ["value1_name1", "value2_name1", "value1_name2", "value2_name2"]
    )


def test_names_vary_is_validated():
    src = data_frame(name = ["a", "b"], value = [1, 2])

    with pytest.raises(ValueError) as exc_info:
        pivot_wider(src, names_vary=1)

    assert "received argument: 1" in exc_info.value.args[0]


    with pytest.raises(ValueError) as exc_info:
        pivot_wider(src, names_vary="x")

    assert "received argument: 'x'" in exc_info.value.args[0]


def test_names_expand_always_sorts_columns():
    # TODO(ask): should we turn null values into "NA" in names?
    src = data_frame(
        name1 = pd.Categorical([None, "x"], ["x", "y"]),
        name2 = ["c", "d"],
        value = [1, 2]
    )

    pv = pivot_wider(src, names_from = _[_.name1, _.name2], names_expand=True)
    assert_frame_equal(
        pv,
        data_frame(x_c = None, x_d = 2, y_c = None, y_d = None, nan_c = 1, nan_d = None)
    )


@pytest.mark.xfail
def test_names_expand_always_sorts_column_names():
    """`names_expand` generates sorted column names even if no expansion is done"""
    # TODO: tidyr converts .name to string?

    src = data_frame(name = [2, 1], value = [1, 2])
    spec = build_wider_spec(src, names_expand = True)
    assert list(spec[".name"]) == ["1", "2"]


def test_names_expand_uses_all_categorical_levels():
    src = data_frame(name1 = ["a", "b"], name2 = ["c", "d"], value = [1, 2])
    spec = build_wider_spec(src, names_from = _[_.name1, _.name2], names_expand = True)

    assert list(spec[".name"]) == ["a_c", "a_d", "b_c", "b_d"]


def test_names_expand_is_validated():
    # TODO: test messages
    # TODO: use build spec
    src = data_frame(name = ["a", "b"], value = [1, 2])

    with pytest.raises(TypeError):
        pivot_wider(src, names_expand = 1)

    with pytest.raises(TypeError):
        pivot_wider(src, names_expand = "x")


# keys ========================================================================
# 11 tests

@pytest.mark.xfail
def test_pivot_can_override_default_keys():
    # TODO: pandas automatically orders the id columns when unstacking. Can we 
    # work around this?
    src = data_frame(
        row = [1, 2, 3],
        name = ["Sam", "Sam", "Bob"],
        var = ["age", "height", "age"],
        value = [10, 1.5, 20]
    )

    dst = data_frame(
        name = ["Sam", "Bob"],
        age = [10, 20],
        height = [1.5, None]
    )

    pv = pivot_wider(src, id_cols = _.name, names_from = _.var, values_from = _.value)

    assert_frame_equal(pv, dst)


def test_selecting_all_id_cols_excludes_names_from_values_from(backend):
    src = backend.load_df(
        data_frame(key = "x", name = "a", value = 1)
    )
    dst = data_frame(key = "x", a = 1)
    pv = pivot_wider(src, _[:])

    assert_equal_query2(pv, dst, sql_ordered=True)

    # TODO: also test pivot_wider_spec


def test_id_cols_overlaps_other_vars_error(backend):
    src = backend.load_df(
        data_frame(name = ["x", "y"], value = [1, 2])
    )

    with pytest.raises(ValueError) as exc_info:
        pivot_wider(src, id_cols = _.name, names_from = _.name, values_from = _.value)

    with pytest.raises(ValueError) as exc_info:
        pivot_wider(src, id_cols = _.value, names_from = _.name, values_from = _.value)


def test_id_cols_no_column_match_error(backend):
    src = backend.load_df(
        data_frame(name = ["x", "y"], value = [1, 2])
    )
    with pytest.raises(ValueError) as exc_info:
        pivot_wider(src, id_cols = _.foo)

    msg = exc_info.value.args[0]
    assert "id_cols must" in msg
    assert "Could not find these columns: {'foo'}" in msg


def test_pivot_zero_row_frame_id_excludes_values_from(backend):
    src = backend.load_df(
        data_frame(key = pd.Series([], dtype="int"), name = [], value = [])
    )
    dst = data_frame(key = pd.Series([], dtype="int"))

    pv = pivot_wider(src, names_from = _.name, values_from = _.value)

    # SQL backends return a empty Index, pandas an empty RangeIndex ¯\_(ツ)_/¯
    assert_equal_query2(
        pv,
        dst,
        sql_kwargs = {"check_index_type": False, "check_dtype": False},
        sql_ordered=True
    )


#TODO:
#test_that("known bug - building a wider spec with a zero row data frame loses `values_from` info (#1249)", {


# id_expand ===============================================================-===

def test_id_expand_generates_sorted_rows():
    src = data_frame(id = [2, 1], name = ["a", "b"], value = [1, 2])
    pv = pivot_wider(src, id_expand = True)

    assert (pv.id == [1, 2]).all()


def test_id_expand_crosses_columns():
    src = data_frame(id1 = [1, 2], id2 = [3, 4], name = ["a", "b"], value = [1, 2])

    assert_frame_equal(
        pivot_wider(src, id_expand = True),
        data_frame(
            id1 = [1, 1, 2, 2],
            id2 = [3, 4, 3, 4],
            a = [1, None, None, None],
            b = [None, None, None, 2.0]
        )
    )


def test_id_expand_expands_all_levels_of_factor():
    id1 = pd.Categorical([None, "x"], ["x", "y"])
    src = data_frame(id1 = id1, id2 = [1, 2], name = ["a", "b"], value = [1, 2])

    pv = pivot_wider(src, id_expand=True)

    assert pv.id1.equals(pd.Series(["x", "x", "y", "y", None, None]))
    assert (pv.id2 == [1, 2, 1, 2, 1, 2]).all()


def test_id_expand_with_values_fill():
    """`id_expand` with `values_fill` only fills implicit missings"""

    id1 = pd.Categorical(["x", "x"], ["x", "y"])
    src = data_frame(id1 = id1, id2 = [1, 2], name = ["a", "b"], value = [1, None])

    pv = pivot_wider(src, id_expand = True, values_fill = 0)
    assert (pv.a == [1, 0, 0, 0]).all()
    assert pv.b.equals(pd.Series([0, None, 0, 0]))


@pytest.mark.xfail
def test_id_expand_values_fill_excludes_id_cols():
    # TODO: how does values_fill work?
    # TODO: it's possible we can't values_fill taking a dict, since we use pivot methods
    src = data_frame(id1 = ["none", "x"], id2 = [1, 2], name = ["a", "b"], value = [1, 2])

    pv = pivot_wider(src, id_expand=True, values_fill = list(id1 = 0))


def test_id_expand_arg_error():
    src = data_frame(name = ["a", "b"], value = [1, 2])

    with pytest.raises(TypeError):
        pivot_wider(src, id_expand = 1)

    with pytest.raises(TypeError):
        pivot_wider(src, id_expand = "x")


# non-unique keys =============================================================
# 8 tests

@pytest.mark.xfail
def test_duplciate_keys_produce_list_column_with_warning():
    # TODO: raises error, but this may be a wontdo
    src = data_frame(a = [1, 1, 2], key = ["x", "x", "x"], val = [1, 2, 3])
    pv = pivot_wider(src, names_from = _.key, values_from = _.val)

    assert False


@pytest.mark.xfail
def test_duplicated_key_warning_mentions_every_column():
    # TODO: similar to above -- wontdo?
    src = data_frame(key = ["x", "x"], a = [1, 2], b = [3, 4], c = [5, 6])

    pv = pivot_wider(src, names_from = _.key, values_from = _[_.a, _.b, _.c])

    assert False


@pytest.mark.xfail
def test_duplicated_key_warning_backticks_non_syntactic_names():
    # TODO: similar to above -- wontdo?
    assert False


def test_duplicate_error_resolved_by_values_fn():
    src = data_frame(a = [1, 1, 2], key = ["x", "x", "x"], val = [1, 2, 3])
    pv = pivot_wider(src, names_from = _.key, values_from = _.val, values_fn=list)

    assert_frame_equal(
        pv,
        data_frame(a = [1, 2], x = [[1, 2], [3]])
    )


def test_values_fn_arg_str(backend):
    src = backend.load_df(
        data_frame(a = [1, 1, 2], key = ["x", "x", "x"], val = [1, 2, 3])
    )
    pv = pivot_wider(src, names_from = _.key, values_from = _.val, values_fn="sum")

    assert_equal_query2(
        pv,
        data_frame(a = [1, 2], x = [3, 3]),
        sql_kwargs={"check_dtype": False}
    )


def test_values_fn_arg_lambda():
    f = lambda x: x.sum()
    src = data_frame(a = [1, 1, 2], key = ["x", "x", "x"], val = [1, 2, 3])
    pv = pivot_wider(src, names_from = _.key, values_from = _.val, values_fn=f)

    assert_frame_equal(
        pv,
        data_frame(a = [1, 2], x = [3, 3])
    )


def test_values_fn_on_no_duplicates():
    src = data_frame(a = [1, 2], key = ["x", "x"], val = [1, 2])
    pv = pivot_wider(src, names_from = _.key, values_from = _.val, values_fn=list)

    assert_frame_equal(
        pv,
        data_frame(a = [1, 2], x = [[1], [2]])
    )


# can fill missing cells ======================================================
# 3 tests
# TODO:

# multiple values =============================================================

def test_pivot_multiple_measure_cols():
    # TODO: use spec instead
    src = data_frame(row = 1, var = ["x", "y"], a = [1, 2], b = [3, 4])
    dst = data_frame(row = 1, a_x = 1, a_y = 2, b_x = 3, b_y = 4)

    pv = pivot_wider(src, names_from = _.var, values_from = _[_.a, _.b])

    assert_frame_equal(pv, dst)


def test_pivot_multiple_measure_no_id_cols():
    src = data_frame(var = ["x", "y"], a = [1, 2], b = [3, 4])
    dst = data_frame(a_x = 1, a_y = 2, b_x = 3, b_y = 4)

    pv = pivot_wider(src, names_from = _.var, values_from = _[_.a, _.b])

    assert_frame_equal(pv, dst)


@pytest.mark.skip("TODO: spec")
def test_pivot_column_order_matches_spec():
    """column order in output matches spec"""
    # TODO
    assert False

# unused_fn ===================================================================
# 6 tests
# TODO
