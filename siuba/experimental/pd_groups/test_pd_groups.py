import pytest
from siuba.tests.helpers import data_frame
import pandas as pd

from siuba.experimental.pd_groups.translate import method_agg_op, method_el_op, method_el_op2
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
        ])
def test_agg_groupby_broadcasted_equal_to_transform(f_op, f_dst):
    g = data_default.groupby('g')
    res = f_op(g)

    # needs to be exact, since GroupByAgg is subclass of SeriesGroupBy
    assert type(res) is GroupByAgg

    dst = f_dst(g)
    broadcasted = res._broadcast_agg_result()
    assert_series_equal(broadcasted, dst, check_names = False)


