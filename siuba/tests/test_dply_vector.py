"""
Tests the singledispatch functions in siuba.dply.vector. Note that these functions
also include doctests, so we want to mostly test their implementations return similar
results.
"""
import pytest

from siuba import _, mutate, group_by, summarize
import siuba.sql.dply
from siuba.dply import vector as v
from datetime import timedelta

from hypothesis import given, settings, example
from hypothesis.strategies import text, floats, integers
from hypothesis.extra.pandas import data_frames, column, range_indexes 
from hypothesis.extra.numpy import integer_dtypes, floating_dtypes

from .helpers import assert_equal_query, data_frame
from pandas.testing import assert_frame_equal

DATA_SPEC = data_frames([
        column('x', elements = floats() | integers(), unique = True),
        column('g', dtype = str, elements = text(max_size = 1))
        ],
        index = range_indexes(min_size = 1, max_size = 10)
    )

OMNIBUS_VECTOR_FUNCS = [
        #cumall, cumany, cummean,
        #desc,
        v.dense_rank(_.x, na_option = "keep"),
        #v.percent_rank(_.x),
        v.min_rank(_.x, na_option = "keep"),
        v.cume_dist(_.x, na_option = "keep"),
        v.row_number(_.x),
        #ntile,
        v.between(_.x, 2, 5, default = False),
        #v.coalesce(_.x),
        v.lead(_.x),
        v.lag(_.x),
        v.n(_.x),
        v.na_if(_.x, 2),
        #near,
        #nth, first, last
        ]

VECTOR_AGG_FUNCS = [
        v.n(_.x),
        v.n(_),
        ]

@pytest.fixture(params = [
    data_frame(x = [1,2,3], g = ['a', 'a', 'b']),
    data_frame(x = [1.,2.,3.], g = ['a', 'a', 'b']),
    data_frame(x = [1.,2.,None], g = ['a', 'a', 'b']),
    ])
def simple_data(request):
    return request.param

@example(data_frame(x = [1,2,3], g = ['a', 'a', 'b']))
@example(data_frame(x = [1.,2.,3.], g = ['a', 'a', 'b']))
@example(data_frame(x = [1.,2.,None], g = ['a', 'a', 'b']))
@pytest.mark.parametrize('func', OMNIBUS_VECTOR_FUNCS)
def test_mutate_vector(backend, func, simple_data):
    if backend.name == 'sqlite':
        pytest.skip()

    df = backend.load_cached_df(simple_data)
    
    assert_equal_query(
            df,
            mutate(y = func),
            simple_data.assign(y = func),
            check_dtype = False
            )

@pytest.mark.parametrize('func', OMNIBUS_VECTOR_FUNCS)
def test_mutate_vector_grouped(backend, func, simple_data):
    if backend.name == 'sqlite':
        pytest.skip()

    df = backend.load_cached_df(simple_data)
    
    assert_equal_query(
            df,
            group_by(_.g) >> mutate(y = func),
            simple_data.groupby('g').apply(lambda d: d.assign(y = func)).reset_index(drop = True),
            check_dtype = False
            )


@pytest.mark.parametrize('func', VECTOR_AGG_FUNCS)
def test_agg_vector(backend, func, simple_data):
    from siuba.siu import strip_symbolic
    if backend.name == 'sqlite':
        pytest.skip()

    df = backend.load_cached_df(simple_data)

    res = data_frame(y = func(simple_data))

    assert_equal_query(
            df,
            summarize(y = func),
            res 
            )

@pytest.mark.parametrize('func', VECTOR_AGG_FUNCS)
def test_agg_vector_grouped(backend, func, simple_data):
    if backend.name == 'sqlite':
        pytest.skip()

    df = backend.load_cached_df(simple_data)

    assert_equal_query(
            df,
            group_by(_.g) >> summarize(y = func),
            simple_data.groupby('g').apply(func).reset_index().rename(columns = {0: 'y'})
            )


@given(DATA_SPEC)
@settings(max_examples = 50, deadline = 1000)
def test_omnibus_vector_funcs(backend, data):
    if backend.name == 'sqlite':
        pytest.skip()

    df = backend.load_df(data)
    
    for func in OMNIBUS_VECTOR_FUNCS:
        assert_equal_query(
                df,
                mutate(y = func),
                data.assign(y = func),
                check_dtype = False
                )



@pytest.mark.parametrize('func', [
    v.n_distinct(_.x),
    ])
@given(DATA_SPEC)
def test_pandas_only_vector_funcs(func, data):
    res = mutate(data, y = func)
    dst = data.assign(y = func)

    assert_frame_equal(res, dst)



