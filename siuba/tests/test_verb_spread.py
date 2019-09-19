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

