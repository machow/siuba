import pytest
from siuba.tests.helpers import data_frame
import pandas as pd

from siuba.experimental.pd_groups.translate import method_agg_op, method_el_op, method_el_op2
from siuba.experimental.pd_groups.groupby import broadcast_agg
#TODO: 
#  - what if they have mandatory, non-data args?
#  - support accessor methods like _.x.str.upper()
#  - support .expanding and .rolling


data_dt = data_frame(
    g = ['a', 'a', 'b', 'b'],
    x = pd.to_datetime(["2019-01-01 01:01:01", "2020-04-08 02:02:02", "2021-07-15 03:03:03", "2022-10-22 04:04:04"])
    )

data_str = data_frame(
    g = ['a', 'a', 'b', 'b'],
    x = ['abc', 'cde', 'fg', 'h']
    )

data_default = data_frame(
    g = ['a', 'a', 'b', 'b'],
    x = [10, 11, 12, 13],
    y = [1,2,3,4]
    )

data = {
    'dt': data_dt,
    'str': data_str,
    None: data_default
}


# Test translator =============================================================

from pandas.testing import assert_frame_equal, assert_series_equal
from siuba.experimental.pd_groups.groupby import GroupByAgg, SeriesGroupBy

f_min = method_agg_op('min', is_property = False, accessor = None)
f_add = method_el_op2('add', is_property = False, accessor = None)
f_abs = method_el_op('abs', is_property = False, accessor = None)
f_df_size = lambda x: GroupByAgg.from_result(x.size(), x)

# GroupByAgg is liskov substitutable, so check that our functions operate
# like similarly substitutable subtypes. This means that...
# * input type is the same or more general, and
# * output type is the same or more specific

@pytest.mark.parametrize('f_op, f_dst, cls_result', [
        # aggregation 1-arity
        # f(SeriesGroupBy) -> GroupByAgg <= f(GroupByAgg) -> GroupByAgg 
        (lambda g: f_min(g.x),        lambda g: g.x.min(), GroupByAgg),
        (lambda g: f_min(f_min(g.x)), lambda g: g.x.min(), GroupByAgg),
        # elementwise 1-arity
        # f(GroupByAgg) -> GroupByAgg <= f(SeriesGroupBy) -> SeriesGroupBy 
        (lambda g: f_abs(f_min(g.x)), lambda g: g.x.min().abs(), GroupByAgg),
        (lambda g: f_abs(g.x),        lambda g: g.obj.x.abs(), SeriesGroupBy),
        # elementwise 2-arity
        # f(GroupByAgg, GroupByAgg) -> GroupByAgg <= f(GroupByAgg, SeriesGroupBy) -> SeriesGroupBy
        (lambda g: f_add(f_min(g.x), f_min(g.y)), lambda g: g.x.min() + g.y.min(), GroupByAgg),
        (lambda g: f_add(g.x, f_min(g.y)),        lambda g: g.obj.x + g.y.transform('min'), SeriesGroupBy),
        (lambda g: f_add(g.x, g.y),               lambda g: g.obj.x + g.obj.y, SeriesGroupBy),
        ])
def test_grouped_translator_methods(f_op, f_dst, cls_result):
    g = data_default.groupby('g')
    res = f_op(g)

    # needs to be exact, since GroupByAgg is subclass of SeriesGroupBy
    assert type(res) is cls_result

    dst = f_dst(g)
    assert_series_equal(res.obj, dst, check_names = False)


@pytest.mark.parametrize('f_op, f_dst', [
        (lambda g: f_add(f_min(g.x), f_min(g.y)), lambda g: g.x.transform('min') + g.y.transform('min')),
        (lambda g: f_min(g.x),        lambda g: g.x.transform('min')),
        (lambda g: f_min(f_min(g.x)), lambda g: g.x.transform('min')),
        (lambda g: f_abs(f_min(g.x)), lambda g: g.x.transform('min').abs()),

        # Note that there's no way to transform a DF method, so use an arbitrary column
        (lambda g: f_df_size(g), lambda g: g.x.transform('size')),
        ])
def test_agg_groupby_broadcasted_equal_to_transform(f_op, f_dst):
    g = data_default.groupby('g')
    res = f_op(g)

    # needs to be exact, since GroupByAgg is subclass of SeriesGroupBy
    assert type(res) is GroupByAgg

    dst = f_dst(g)
    broadcasted = broadcast_agg(res)
    assert_series_equal(broadcasted, dst, check_names = False)


# Test generic functions ======================================================

def test_fast_mutate_basic():
    # sanity check of https://github.com/machow/siuba/issues/355
    from siuba.siu import _

    res_df = data_default.groupby("g") >> fast_mutate(num = _.x / _.y * 100)

    res = res_df.num
    dst = data_default.x / data_default.y * 100

    assert_series_equal(res.obj, dst, check_names=False)
    

# Test user-defined functions =================================================

from .dialect import fast_mutate, fast_summarize, fast_filter, _transform_args
from siuba.siu import symbolic_dispatch, _, FunctionLookupError
from typing import Any

def test_transform_args():
    pass


def test_fast_grouped_custom_user_funcs():
    @symbolic_dispatch
    def f(x):
        raise NotImplementedError()

    @f.register(SeriesGroupBy)
    def _f_grouped(x) -> GroupByAgg:
        return GroupByAgg.from_result(x.mean() + 10, x)

    gdf = data_default.groupby('g')
    g_out = fast_mutate(gdf, result1 = f(_.x), result2 = _.x.mean() + 10)
    out = g_out.obj
    assert (out.result1 == out.result2).all()


def test_fast_grouped_custom_user_func_default():
    @symbolic_dispatch
    def f(x) -> Any:
        return GroupByAgg.from_result(x.mean() + 10, x)

    gdf = data_default.groupby('g')
    g_out = fast_mutate(gdf, result1 = f(_.x), result2 = _.x.mean() + 10)
    out = g_out.obj
    assert (out.result1 == out.result2).all()

def test_fast_grouped_custom_user_func_fail():
    @symbolic_dispatch
    def f(x):
        return x.mean()

    @f.register(SeriesGroupBy)
    def _f_gser(x):
        # note, no return annotation, so translator will raise an error
        return GroupByAgg.from_result(x.mean(), x)


    gdf = data_default.groupby('g')
    
    with pytest.warns(UserWarning):
        g_out = fast_mutate(gdf, result1 = f(_.x), result2 = _.x.mean() + 10)


def test_fast_methods_constant():
    gdf = data_default.groupby('g')

    # mutate ----
    out = fast_mutate(gdf, y = 1)
    assert_frame_equal(data_default.assign(y = 1), out.obj)

    # summarize ----
    out = fast_summarize(gdf, y = 1)
    agg_frame = gdf.grouper.result_index.to_frame().reset_index(drop = True)
    assert_frame_equal(agg_frame.assign(y = 1), out)

    # filter ----
    out = fast_filter(gdf, True)
    assert_frame_equal(gdf.obj, out.obj)

    # note: two empty DataFrames can differ if their (empty) indices are different types
    #       e.g. Index vs RangeIndex
    out = fast_filter(gdf, False)
    assert_frame_equal(
            gdf.obj.iloc[0:0,:],
            out.obj,
            check_index_type = False
            )

    
def test_fast_methods_lambda():
    # testing ways to do operations via slower apply route

    gdf = data_default.groupby('g')

    # mutate ----
    out = fast_mutate(gdf, y = lambda d: len(d['x']))
    assert_frame_equal(
            gdf.obj.assign(y = gdf['x'].transform('size')),
            out.obj
            )

    # summarize ----
    out = fast_summarize(gdf, y = lambda d: len(d['x']))

    agg_frame = gdf.grouper.result_index.to_frame()
    assert_frame_equal(
            agg_frame.assign(y = gdf['x'].agg('size')).reset_index(drop = True),
            out
            )

    # filter ----
    out = fast_filter(gdf, lambda d: d['x'] > d['x'].values.min())
    assert_frame_equal(
            gdf.obj[gdf.obj['x'] > gdf['x'].transform('min')],
            out.obj
            )
