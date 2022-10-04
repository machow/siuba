import pandas as pd
import pytest

from pandas.testing import assert_frame_equal
from pandas.core.groupby import DataFrameGroupBy
from siuba.siu import symbolic_dispatch, Symbolic, Fx
from siuba.dply.verbs import mutate, filter, summarize, group_by, collect, ungroup
from siuba.dply.across import across

from siuba.experimental.pivot.test_pivot import assert_equal_query2
from siuba.sql.translate import SqlColumn, SqlColumnAgg, sql_scalar

# TODO: test transmute
# TODO: test verb(data, _.simple_name)
# TODO: test changing a group var (e.g. mutate, transmute, add_count), then summarizing
# TODO: group_by(cyl) >> count(cyl = cyl + 1)
# TODO: SQL mutate requires immediate CTE (e.g. due to GROUP BY clause)
# TODO: count "n" name


# Helpers =====================================================================

_ = Symbolic()

# round function ----

@symbolic_dispatch(cls = pd.Series)
def f_round(x) -> pd.Series:
    return round(x)


f_round.register(SqlColumn, sql_scalar("round"))

# mean function ----

@symbolic_dispatch(cls = pd.Series)
def f_mean(x) -> pd.Series:
    return x.mean()


def assert_grouping_names(gdf, names):
    from siuba.sql import LazyTbl

    if isinstance(gdf, LazyTbl):
        grouping_names = list(gdf.group_by)
    else:
        assert isinstance(gdf, DataFrameGroupBy)
        groupings = gdf.grouper.groupings
        grouping_names = [g.name for g in groupings]

    assert len(grouping_names) == len(names)
    assert grouping_names == names


# Fixtures ====================================================================

@pytest.fixture
def df():
    return pd.DataFrame({
        "a_x": [1, 2],
        "a_y": [3, 4],
        "b_x": [5., 6.],    # note the floats
        "g": ["m", "n"]
    })


# Tests =======================================================================

TRANSFORMATION_FUNCS = [
    f_round,
    Fx.round(),
    f_round(Fx),
]


@pytest.mark.parametrize("func", TRANSFORMATION_FUNCS)
def test_across_func_transform(df, func):
    res = across(df, _[_.a_x, _.a_y], func)
    dst = pd.DataFrame({
        "a_x": df.a_x.round(),
        "a_y": df.a_y.round()
    })

    assert_equal_query2(res, dst)


def test_across_func_transform_lambda(df):
    res = across(df, _[_.a_x, _.a_y], lambda x: x.round())
    dst = pd.DataFrame({
        "a_x": df.a_x.round(),
        "a_y": df.a_y.round()
    })

    assert_frame_equal(res, dst)


@pytest.mark.parametrize("func", [
    (f_mean),
    (Fx.mean()),
    (f_mean(Fx)),
    (lambda x: x.mean()),
])
def test_across_func_aggregate(df, func):
    res = across(df, _[_.a_x, _.a_y], func)
    dst = pd.DataFrame({
        "a_x": [df.a_x.mean()],
        "a_y": [df.a_y.mean()]
    })

    assert_frame_equal(res, dst)


@pytest.mark.parametrize("func", [
    (lambda x: x % 2 > 1),
    (Fx % 2 > 1),
])
def test_across_func_bool(df, func):
    res = across(df, _[_.a_x, _.a_y], func)
    dst = pd.DataFrame({
        "a_x": df.a_x % 2 > 1,
        "a_y": df.a_y % 2 > 1
    })

    assert_frame_equal(res, dst)


@pytest.mark.parametrize("selection", [
    (_[0,1]),
    (_[0:"a_y"]),
    (lambda x: x.dtype == "int64"),
    #(where(Fx.dtype == "int64")),
    #(where(Fx.dtype != "float64") & ~_.g),
    (~_[_.b_x, _.g]),

])
def test_across_selection(df, selection):
    res = across(df, selection, lambda x: x + 1)
    dst = df[["a_x", "a_y"]] + 1

    assert_frame_equal(res, dst)


def test_across_selection_rename(df):
    res = across(df, _.zzz == _.a_x, lambda x: x + 1)
    assert res.columns.tolist() == ["zzz"]

    assert_frame_equal(res, (df[["a_x"]] + 1).rename(columns={"a_x": "zzz"}))


@pytest.mark.parametrize("func", TRANSFORMATION_FUNCS)
def test_across_in_mutate(backend, df, func):
    src = backend.load_df(df)
    res_explicit = mutate(src, across(_, _[_.a_x, _.a_y], f_round))
    res_implicit = mutate(src, across(_[_.a_x, _.a_y], f_round))

    dst = df.copy()
    dst["a_x"] = df.a_x.round()
    dst["a_y"] = df.a_y.round()

    sql_kwargs = {"check_dtype": False}
    assert_equal_query2(res_explicit, dst, sql_kwargs=sql_kwargs)
    assert_equal_query2(res_implicit, dst, sql_kwargs=sql_kwargs)


def test_across_in_mutate_grouped_equiv_ungrouped(backend, df):
    src = backend.load_df(df)
    g_src = group_by(src, "g")

    expr_across = across(_, _[_.a_x, _.a_y], f_round)
    g_res = mutate(g_src, expr_across)
    dst = mutate(src, expr_across)

    assert_grouping_names(g_res, ["g"])
    assert_equal_query2(ungroup(g_res), collect(dst))


def test_across_in_summarize(df):
    res = summarize(df, across(_, _[_.a_x, _.a_y], f_mean))
    dst = pd.DataFrame({
        "a_x": [df.a_x.mean()],
        "a_y": [df.a_y.mean()]
    })

    assert_frame_equal(res, dst)


def test_across_in_summarize_equiv_ungrouped():
    # note that summarize does not automatically regroup on any keys
    src = pd.DataFrame({
        "a_x": [1, 2],
        "a_y": [3, 4],
        "b_x": [5., 6.],
        "g": ["ZZ", "ZZ"]   # Note: all groups the same
    })

    g_src = src.groupby("g")

    expr_across = across(_, _[_.a_x, _.a_y], f_mean)
    g_res = summarize(g_src, expr_across)
    dst = summarize(src, expr_across)

    assert g_res.columns.tolist() == ["g", "a_x", "a_y"]
    assert g_res["g"].tolist() == ["ZZ"]

    assert_frame_equal(g_res.drop(columns="g"), dst)


def test_across_in_filter(backend, df):
    src = backend.load_df(df)
    res = filter(src, across(_, _[_.a_x, _.a_y], Fx % 2 > 0))

    dst = df[(df[["a_x", "a_y"]] % 2 > 0).all(axis=1)]

    assert_equal_query2(res, dst)


def test_across_in_filter_equiv_ungrouped(df):
    gdf = df.groupby("g")

    expr_across = across(_, _[_.a_x, _.a_y], lambda x: x % 2 > 0)
    g_res = filter(gdf, expr_across)
    dst = filter(df, expr_across)

    assert_grouping_names(g_res, ["g"])
    assert_frame_equal(g_res.obj, dst)


def test_across_formula_and_underscore(df):
    res = across(df, _[_.a_x, _.a_y], f_round(Fx) / _.b_x)

    dst = pd.DataFrame({
        "a_x": df.a_x.round() / df.b_x,
        "a_y": df.a_y.round() / df.b_x
    })

    assert_frame_equal(res, dst)


def test_across_names_arg(df):
    res = across(df, _[_.a_x, _.a_y], Fx + 1, names="{col}_funkyname")
    assert list(res.columns) == ["a_x_funkyname", "a_y_funkyname"]

    dst = (df[["a_x", "a_y"]] + 1).rename(columns = lambda s: s + "_funkyname")
    assert_frame_equal(res, dst)


def test_across_func_dict(df):
    res = across(df, _[_.a_x, _.a_y], {"plus1": Fx + 1, "plus2": Fx + 2})

    dst = pd.DataFrame({
        "a_x_plus1": df.a_x + 1,
        "a_x_plus2": df.a_x + 2,
        "a_y_plus1": df.a_y + 1,
        "a_y_plus2": df.a_y + 2
    })

    assert_frame_equal(res, dst)


def test_across_func_dict_names_arg(df):
    # TODO: also test aggregation
    funcs = {"plus1": Fx + 1, "plus2": Fx + 2}
    res = across(df, _[_.a_x, _.a_y], funcs, names="{fn}_{col}")

    dst = pd.DataFrame({
        "plus1_a_x": df.a_x + 1,
        "plus2_a_x": df.a_x + 2,
        "plus1_a_y": df.a_y + 1,
        "plus2_a_y": df.a_y + 2,
    })

    assert_frame_equal(res, dst)


