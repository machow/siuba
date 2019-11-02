from siuba.siu import Symbolic, strip_symbolic
from siuba.spec.series import spec
from .helpers import data_frame, assert_equal_query, backend_pandas, SqlBackend
import pytest
# TODO: dot, corr, cov

from siuba import filter, mutate, summarize, group_by
from pandas.testing import assert_frame_equal, assert_series_equal
import numpy as np
import pandas as pd
import pkg_resources

def filter_on_result(spec, types):
    return [k for k,v in spec.items() if v['result']['type'] in types]

SPEC_IMPLEMENTED = filter_on_result(spec, {"Agg", "Elwise", "Window"})
SPEC_NOTIMPLEMENTED = filter_on_result(spec, {"Singleton"})
SPEC_AGG = filter_on_result(spec, {"Agg"})

_ = Symbolic()

@pytest.fixture(params = tuple(SPEC_IMPLEMENTED))
def entry(request):
    # NOTE: the sole purpose of putting in a fixture is so pytest line output
    #       is very easy to read. (e.g. pytest -v --tb=line)
    key = request.param
    yield spec[key]

@pytest.fixture(params = tuple(SPEC_AGG))
def agg_entry(request):
    key = request.param
    yield spec[key]

@pytest.fixture(params = tuple(SPEC_NOTIMPLEMENTED))
def notimpl_entry(request):
    key  = request.param
    yield spec[key]

def assert_src_array_equal(src, dst):
    if isinstance(src, np.ndarray):
        assert np.array_equal(src, dst)
    elif isinstance(src, pd.DataFrame):
        assert_frame_equal(src, dst)
    elif isinstance(src, pd.Series):
        assert_series_equal(src, dst, check_names = False)
    else:
        assert src == dst
    
# Data ========================================================================
data_dt = data_frame(
    g = ['a', 'a', 'b', 'b'],
    x = pd.to_datetime(["2019-01-01 01:01:01", "2020-04-08 02:02:02", "2021-07-15 03:03:03", "2022-10-22 04:04:04"])
    )

data_str = data_frame(
    g = ['a', 'a', 'b', 'b'],
    x = ['abc', 'cde', 'fg', 'h']
    )

data_bool = data_frame(
    g = ['a', 'a', 'b', 'b'],
    x = [True, False, True, False],
    y = [True, True, False, False]
        )

data_default = data_frame(
    g = ['a', 'a', 'a', 'b', 'b', 'b'],
    x = [10, 11, 11, 13, 13, 13],
    y = [1,2,3,4,5,6]
    )

data = {
    'dt': data_dt,
    'str': data_str,
    None: data_default,
    'bool': data_bool
}

# Tests =======================================================================

# Series expr and call return same result

# Series expr and Postgres return same result

# Series agg and trivial group agg return same result (when cast dimless)

def test_series_against_call(entry):
    if entry['result']['type'] == "Window":
        pytest.skip()

    df = data[entry['accessor']]
    # TODO: once reading from yaml, no need to repr
    str_expr = str(entry['expr_series'])

    call_expr = strip_symbolic(eval(str_expr, {'_': _}))
    res = call_expr(df.x)

    dst = eval(str_expr, {'_': df.x})
    
    assert res.__class__ is dst.__class__
    assert_src_array_equal(res, dst)


def test_frame_expr(entry):
    df = data[entry['accessor']]
    # TODO: once reading from yaml, no need to repr
    str_expr = str(entry['expr_frame'])

    call_expr = strip_symbolic(eval(str_expr, {'_': _}))
    res = call_expr(df)

    dst = eval(str_expr, {'_': df})
    
    assert res.__class__ is dst.__class__
    assert_src_array_equal(res, dst)


@backend_pandas
#@pytest.mark.skip_backend('sqlite')
def test_frame_mutate(backend, entry):
    # CASE 1: Needs to be implmented
    if backend.name in entry['result'].get('xfail', []):
        pytest.xfail("TODO: impelement this translation")
    
    # CASE 2: Can't be used in a mutate (e.g. a SQL ordered set aggregate function)
    if backend.name in entry['result'].get('no_mutate', []):
        pytest.skip("Spec'd failure")

    # CASE 3: Uses an operation that can only take boolean inputs
    if isinstance(backend, SqlBackend) and entry['result'].get('op') == 'bool':
        crnt_data = data['bool']

    else:
        crnt_data = data[entry['accessor']]

    df = backend.load_df(crnt_data)

    # TODO: once reading from yaml, no need to repr
    str_expr = str(entry['expr_frame'])
    call_expr = strip_symbolic(eval(str_expr, {'_': _}))

    dst_series = eval(str_expr, {'_': crnt_data})
    dst = crnt_data.assign(result = dst_series)
    
    # CASE 4: marked as NotImplemented (meaning no plan to implement)
    if backend.name in entry['result'].get('not_impl', []):
        with pytest.raises(NotImplementedError):
            mutate(df, result = call_expr)

        # we're done
        return         

    # CASE 5: 
    if isinstance(backend, SqlBackend) and entry['result'].get('sql_type') == 'float':
        dst['result'] = dst['result'].astype('float')

    # otherwise, verify returns same result as mutate
    assert_equal_query(df, mutate(result = call_expr), dst)


def test_pandas_grouped_frame_fast_not_implemented(notimpl_entry):
    from siuba.experimental.pd_groups.dialect import fast_mutate
    gdf = data[notimpl_entry['accessor']].groupby('g')

    # TODO: once reading from yaml, no need to repr
    str_expr = str(notimpl_entry['expr_frame'])
    call_expr = strip_symbolic(eval(str_expr, {'_': _}))

    with pytest.raises(NotImplementedError):
        res = fast_mutate(gdf, result = call_expr)
    

def test_pandas_grouped_frame_fast_mutate(entry):
    from siuba.experimental.pd_groups.dialect import fast_mutate, DataFrameGroupBy
    gdf = data[entry['accessor']].groupby('g')

    # TODO: once reading from yaml, no need to repr
    str_expr = str(entry['expr_frame'])
    call_expr = strip_symbolic(eval(str_expr, {'_': _}))

    res = fast_mutate(gdf, result = call_expr)
    dst = mutate(gdf, result = call_expr)

    # fix mutate's current bad behavior of reordering rows ---
    # (fixed in issue #139)
    dst_obj_fixed = dst.obj

    # TODO: apply mark to skip failing tests, rather than downcast
    # pandas grouped aggs, when not using cython, _try_cast back to original type
    # but since mutate uses apply, it doesn't :/. Currently only affects median func.
    if str_expr == '_.x.median()':
        dst_obj_fixed['result'] = gdf._try_cast(dst_obj_fixed['result'], gdf.x.obj)

    assert isinstance(dst, DataFrameGroupBy)
    assert_frame_equal(res.obj, dst_obj_fixed)


#@pytest.mark.skip_backend('sqlite')
@backend_pandas
def test_frame_summarize_trivial(backend, agg_entry):
    crnt_data = data[agg_entry['accessor']]
    df = backend.load_df(crnt_data)

    # TODO: once reading from yaml, no need to repr
    str_expr = str(agg_entry['expr_frame'])

    call_expr = strip_symbolic(eval(str_expr, {'_': _}))
    res = summarize(df, result = call_expr)

    # Perform a trivial group agg, where the entire frame is 1 group
    dst_out = eval(str_expr, {'_': df})
    dst_series = dst_out if isinstance(dst_out, pd.Series) else pd.Series(dst_out)
    dst = pd.DataFrame({'result': dst_series})
    
    assert_frame_equal(res, dst)

# Edge Cases ==================================================================

def test_frame_set_aggregates_postgresql():
    # TODO: probably shouldn't be creating backend here
    backend = SqlBackend("postgresql")
    dfs = backend.load_df(data[None])
    
    expr = _.x.quantile(.75)
    assert_equal_query(
            dfs,
            group_by(_.g) >> summarize(result = expr),
            data_frame(g = ['a', 'b'], result = [11., 13.])
            )


