"""
Tests the singledispatch functions in siuba.dply.vector. Note that these functions
also include doctests, so we want to mostly test their implementations return similar
results.
"""
import pytest

from siuba import _, arrange, mutate, group_by, summarize, filter
import siuba.sql.dply
from siuba.dply import vector as v
from datetime import timedelta

from hypothesis import given, settings, example
from hypothesis.strategies import text, floats, integers
from hypothesis.extra.pandas import data_frames, column, indexes 

from .helpers import assert_equal_query, data_frame, backend_sql
from pandas.testing import assert_frame_equal

DATA_SPEC = data_frames([
        column('x', elements = floats(width = 32) | integers(), unique = True),
        column('g', dtype = str, elements = text(max_size = 1))
        ],
        index = indexes(elements = floats() | integers(), max_size = 10)
    )

OMNIBUS_VECTOR_FUNCS = [
        #cumall, cumany, cummean,
        #desc,
        ## removed below, since pandas was giving 0 and eg .1^(-16) same rank
        #v.dense_rank(_.x, na_option = "keep"),
        #v.percent_rank(_.x),
        v.min_rank(_.x, na_option = "keep"),
        v.cume_dist(_.x, na_option = "keep"),
        v.row_number(_.x),
        #ntile,
        v.between(_.x, 2, 5, default = False),
        v.coalesce(_.x, 2),
        v.lead(_.x),
        v.lag(_.x),
        v.n(_.x),
        v.na_if(_.x, 2),
        #near,
        v.nth(_.x, 2),
        v.first(_.x),
        v.last(_.x, order_by = _.idx),            # TODO: in SQL getting FROM LAST requires order by
        ]

VECTOR_AGG_FUNCS = [
        v.n(_.x),
        v.n(_),
        ]

VECTOR_FILTER_FUNCS = [
        v.dense_rank(_.x, na_option = "keep") < 2,
        v.min_rank(_.x, na_option = "keep") < 2,
        v.cume_dist(_.x, na_option = "keep") < .3,
        v.row_number(_.x) < 2,
        v.between(_.x, 2, 3),
        v.lead(_.x) > _.x,
        v.lag(_.x) > _.x,
        v.n(_.x) == 1,
        v.na_if(_.x, 1).isna()
        ]

@pytest.fixture(params = [
    data_frame(idx = [1,2,3], x = [1,2,3], g = ['a', 'a', 'b']),
    data_frame(idx = [1,2,3], x = [1.,2.,3.], g = ['a', 'a', 'b']),
    data_frame(idx = [1,2,3], x = [1.,2.,None], g = ['a', 'a', 'b']),
    ])
def simple_data(request):
    return request.param

@pytest.mark.parametrize('func', OMNIBUS_VECTOR_FUNCS)
def test_mutate_vector(backend, func, simple_data):
    df = backend.load_cached_df(simple_data)
    
    assert_equal_query(
            df,
            arrange(_.idx) >> mutate(y = func),
            simple_data.assign(y = func),
            check_dtype = False
            )

    # grouped
    assert_equal_query(
            df,
            arrange(_.idx) >> group_by(_.g) >> mutate(y = func),
            simple_data.groupby('g').apply(lambda d: d.assign(y = func)).reset_index(drop = True),
            check_dtype = False
            )


@pytest.mark.parametrize('func', VECTOR_AGG_FUNCS)
def test_agg_vector(backend, func, simple_data):
    df = backend.load_cached_df(simple_data)

    res = data_frame(y = func(simple_data))

    assert_equal_query(
            df,
            summarize(y = func),
            res 
            )

    # grouped
    assert_equal_query(
            df,
            group_by(_.g) >> summarize(y = func),
            simple_data.groupby('g').apply(func).reset_index().rename(columns = {0: 'y'})
            )


@backend_sql
@pytest.mark.parametrize('func', VECTOR_FILTER_FUNCS)
def test_filter_vector(backend, func, simple_data):
    df = backend.load_cached_df(simple_data)

    res = data_frame(y = func(simple_data))

    assert_equal_query(
            df,
            arrange(_.idx) >> filter(func),
            filter(simple_data, func),
            # ignore dtypes, since sql -> an empty data frame has object columns
            check_dtype = False
            )

    # grouped (vs slow_filter)
    assert_equal_query(
            df,
            arrange(_.idx) >> group_by(_.g) >> filter(func),
            simple_data >> group_by(_.g) >> filter(func),
            check_dtype = False
            )


#@given(DATA_SPEC)
#@settings(max_examples = 50, deadline = 1000)
#def test_hypothesis_mutate_vector_funcs(backend, data):
#
#    df = backend.load_df(data)
#    
#    for func in OMNIBUS_VECTOR_FUNCS:
#        assert_equal_query(
#                df,
#                mutate(y = func),
#                data.assign(y = func),
#                check_dtype = False
#                )



@pytest.mark.parametrize('func', [
    v.n_distinct(_.x),
    ])
@given(DATA_SPEC)
def test_pandas_only_vector_funcs(func, data):
    res = mutate(data, y = func)
    dst = data.assign(y = func)

    assert_frame_equal(res, dst)



