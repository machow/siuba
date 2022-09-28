from siuba import spread, gather, _
from pandas.testing import assert_frame_equal
import pandas as pd

import pytest

@pytest.fixture
def df():
    return pd.DataFrame({
        'id': [1, 1, 2],
        'm': ['a', 'b', 'b'],
        'v': [1,2,3]
        })

@pytest.fixture
def wide_df():
    return pd.DataFrame({
        'id': [1,2],
        'a': [1, None],
        'b': [2., 3.]
        })

@pytest.fixture
def df_no_drop():
    return pd.DataFrame({
        'id': [1,2,1,2],
        'm': ['a', 'a', 'b', 'b'],
        'v': [1, None, 2, 3]
        })

@pytest.mark.parametrize('key, value', [
    ("m", "v"),
    (_.m, _.v),
    (_["m"], _["v"]),
    (1, 2),
    ])
def test_spread_selection(df, wide_df, key, value):
    res = spread(df, key, value)


    assert_frame_equal(res, wide_df)
    

def test_spread_fill(df, wide_df):
    res = spread(df, "m", "v", fill = 99)

    assert_frame_equal(res, wide_df.fillna(99.))


def test_spread_grouped_df(df, wide_df):
    gdf = df.groupby('id')
    res = spread(gdf, "m", "v")

    gdf_wide = wide_df.groupby('id')
    assert_frame_equal(res.obj, gdf_wide.obj)

    assert len(res.grouper.groupings) == 1
    assert res.grouper.groupings[0].name == "id"


def test_gather(df, wide_df):
    res = gather(wide_df, "m", "v", "a", "b") 

    # note, unlike df, includes an NA now
    long = pd.DataFrame({
        'id': [1,2,1,2],
        'm': ['a', 'a', 'b', 'b'],
        'v': [1, None, 2, 3]
        })

    assert_frame_equal(res, long)


def test_gather_drop_na(df, wide_df):
    res = gather(wide_df, "m", "v", "a", "b", drop_na = True) 

    # note that .dropna doesn't work here, since coerces floats to ints
    assert_frame_equal(res, df.assign(v = df.v.astype(float)))


def test_gather_drop_na_varselect(df, wide_df):
    res = gather(wide_df, "m", "v", _["a": "b"], drop_na = True) 

    # note that .dropna doesn't work here, since coerces floats to ints
    assert_frame_equal(res, df.assign(v = df.v.astype(float)))


def test_gather_no_selection_gathers_all(wide_df):
    res_implicit = gather(wide_df, "m", "v")
    res_explicit = gather(wide_df, "m", "v", _[:])

    assert res_implicit.columns.tolist() == ["m", "v"]
    assert "id" in res_implicit["m"].values

    assert_frame_equal(res_implicit, res_explicit)


def test_gather_no_match_gathers_none(wide_df):
    res = gather(wide_df, "m", "v", _.startswith("zzzz"))

    assert_frame_equal(res, wide_df)


def test_gather_group_by_no_match_still_grouped(wide_df):
    wide_gdf = wide_df.groupby("id")
    res = gather(wide_gdf, "m", "v", _.startswith("zzzz"))

    assert res is wide_gdf


def test_gather_group_by_preserves_groups(df_no_drop, wide_df):
    res = gather(wide_df.groupby("id"), "m", "v", _.a, _.b)
    
    groupings = res.grouper.groupings
    
    assert len(groupings) == 1
    assert groupings[0].name == "id"

    assert_frame_equal(res.obj, df_no_drop)


def test_gather_group_by_drops_gathered_groups(df_no_drop, wide_df):
    g_res = gather(wide_df.groupby(["id", "a"]), "m", "v", _.a, _.b)

    groupings = g_res.grouper.groupings
    assert len(groupings) == 1
    assert groupings[0].name == "id"

    assert_frame_equal(g_res.obj, df_no_drop)


def test_gather_group_by_drops_all_groups(df_no_drop, wide_df):
    res = gather(wide_df.groupby(["a", "b"]), "m", "v", _.a, _.b)

    assert isinstance(res, pd.DataFrame)

    assert_frame_equal(res, df_no_drop)


