"""
This file is for pandas specific verb operations.
"""

import pytest
from siuba.dply.verbs import mutate, arrange, filter, ungroup
from siuba.siu import _

import pandas as pd
from pandas.testing import assert_frame_equal

@pytest.fixture(scope = "function")
def df1():
    yield pd.DataFrame({
        "repo": ["pandas", "dplyr", "ggplot2", "plotnine"],
        "owner": ["pandas-dev", "tidyverse", "tidyverse", "has2k1"],
        "language": ["python", "R", "R", "python"],
        "stars": [17800, 2800, 3500, 1450],
        "x": [1,2,3,None]
        })


# verify indexes are reset ----

@pytest.mark.parametrize('f, expr', [
    (filter, lambda d: d.x < 2),
    (arrange, lambda d: -d.x)
    ])
def test_verb_accepts_non_range_indexing(df1, f, expr):
    tmp_df = df1.copy()
    tmp_df['orig_index'] = df1.index.values
    tmp_df.index = [3,2,1,0]

    res1 = f(tmp_df, expr)
    res2 = f(df1, expr)

    assert list(res1.orig_index) == list(res2.index)

    assert_frame_equal(
            res1.drop(columns = 'orig_index').reset_index(drop = True),
            res2.reset_index(drop = True)
            )


# mutate ------

def test_dply_mutate(df1):
    op_stars_1k = lambda d: d.stars * 1000
    out1 = mutate(df1, stars_1k = op_stars_1k)
    out2 = df1.assign(stars_1k = op_stars_1k)

    assert_frame_equal(out1, out2)

def test_dply_grouped_mutate_of_agg_order():
    # see issue #139
    df = pd.DataFrame({
        'g': ['b', 'a', 'b'],
        'x':[0, 1, 2]
        })
    gdf = df.groupby('g')
    
    out = mutate(gdf, g_min = lambda d: d.x.min())

    assert_frame_equal(ungroup(out), df.assign(g_min = [0, 1, 0]))

def test_dply_mutate_sym(df1):
    op_stars_1k = _.stars * 1000
    out1 = mutate(df1, stars_1k = op_stars_1k)
    out2 = df1.assign(stars_1k = op_stars_1k)

    assert_frame_equal(out1, out2)

# VarList and friends ------

from siuba.dply.verbs import flatten_var, Var, VarList

def test_flatten_vars():
    v_a, v_b = flatten_var(-Var(["a", "b"]))
    assert v_a.name == "a"
    assert v_b.name == "b"
    assert all([v_a.negated, v_b.negated])

@pytest.mark.parametrize("x", [
    ["x"],
    [Var("x")],
    [slice(0,1)]
    ])
def test_flatten_vars_noop(x):
    assert x is flatten_var(x)[0]

def test_VarList_getitem_siu():
    vl = VarList()
    f = lambda _: _[_.a, _.b]
    var = f(vl)
    v_a, v_b = var.name
    assert v_a.name == "a"
    assert v_b.name == "b"

def test_VarList_getitem():
    vl = VarList()
    var = vl["a":"b", "c"]
    assert isinstance(var.name[0], slice)
    assert var.name[1] == "c"



# Select ----------------------------------------------------------------------

from siuba.dply.verbs import select

def test_varlist_multi_slice(df1):
    out = select(df1, lambda _: _["repo", "owner"])
    assert out.columns.tolist() == ["repo", "owner"]

def test_varlist_multi_slice_negate(df1):
    out = select(df1, lambda _: -_["repo", "owner"])
    assert out.columns.tolist() == ["language", "stars", "x"]


# Distinct --------------------------------------------------------------------

from siuba.dply.verbs import distinct

def test_distinct_no_args():
    df =pd.DataFrame({'x': [1,1,2], 'y': [1,1,2]})
    assert_frame_equal(distinct(df), df.drop_duplicates().reset_index(drop = True))


# Nest ------------------------------------------------------------------------

from siuba.dply.verbs import nest, unnest

def test_nest_grouped(df1):
    out = nest(df1.groupby("language"))
    assert out.columns[0] == "language"

    entry = out.loc[0, "data"]
    assert isinstance(entry, pd.DataFrame)
    assert entry.shape == (2, df1.shape[1] - 1)
    
def test_nest_unnest(df1):
    # important that repo is first column, since grouping vars moved to front
    out = unnest(nest(df1, -Var("repo")))

    # since a nest implies grouping, we need to sort before comparing
    sorted_df = df1.sort_values(["repo"]).reset_index(drop = True)
    assert_frame_equal(out, sorted_df)

def test_unnest_lists():
    df = pd.DataFrame({'id': [1,2], 'data': [['a'], ['x', 'y']]})
    out = unnest(df)
    assert_frame_equal(
            out,
            pd.DataFrame({'id': [1,2,2], 'data': ['a','x','y']})
            )


