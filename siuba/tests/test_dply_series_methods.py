from siuba.siu import Symbolic, strip_symbolic
from siuba.ops.support import spec
from .helpers import data_frame, assert_equal_query, backend_pandas, SqlBackend, PandasBackend
import pytest
# TODO: dot, corr, cov

from siuba import filter, mutate, summarize, group_by, arrange
from pandas.testing import assert_frame_equal, assert_series_equal
import numpy as np
import pandas as pd
import pkg_resources

def get_action_kind(spec_entry):
    return spec_entry["kind"]

def filter_entry(spec, f):
    out = []
    for k, v in spec.items():
        raw_kind = v.get("kind")
        kind = raw_kind.title() if raw_kind is not None else "Unknown"
        status = v["backends"].get("pandas", {}).get("support", "Supported").title()

        if f(kind, status):
            out.append(k)

    return out

SPEC_IMPLEMENTED = filter_entry(spec, lambda k, s: s == "Supported")
SPEC_NOTIMPLEMENTED = filter_entry(spec, lambda k, s: s != "Supported")
SPEC_AGG = filter_entry(spec, lambda k, s: k in {"Agg"} and s == "Supported")

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
    id = [1, 2, 3, 4],
    g = ['a', 'a', 'b', 'b'],
    x = pd.to_datetime(["2019-01-01 01:01:01", "2020-04-08 02:02:02", "2021-07-15 03:03:03", "2022-10-22 04:04:04"])
    )

data_str = data_frame(
    id = [1, 2, 3, 4],
    g = ['a', 'a', 'b', 'b'],
    x = ['abc', 'cde', 'fg', 'h']
    )

data_bool = data_frame(
    id = [1, 2, 3, 4],
    g = ['a', 'a', 'b', 'b'],
    x = [True, False, True, False],
    y = [True, True, False, False]
        )

data_cat = data_frame(
    id = [1, 2, 3, 4],
    g = ['a', 'a', 'b', 'b'],
    x = pd.Categorical(['abc', 'cde', 'fg', 'h'])
    )

data_sparse = data_frame(
    id = [1, 2, 3, 4],
    g = ['a', 'a', 'b', 'b'],
    x = pd.Series([1, 2, 3, 4], dtype = "Sparse")
    )

data_default = data_frame(
    id = [1, 2, 3, 4, 5, 6],
    g = ['a', 'a', 'a', 'b', 'b', 'b'],
    x = [10, 11, 11, 13, 13, 13],
    y = [1,2,3,4,5,6]
    )

DATA = data = {
    'dt': data_dt,
    'str': data_str,
    None: data_default,
    'bool': data_bool,
    'cat': data_cat,
    'sparse': data_sparse

}

def get_spec_no_mutate(entry, backend):
    return "no_mutate" in entry['backends'].get(backend.name, {}).get('flags', [])

def get_spec_no_summarize(entry, backend):
    return "no_aggregate" in entry['backends'].get(backend.name, {}).get('flags', [])
    
def get_spec_backend_is_supported(entry, backend):
    return entry['backends'].get(backend.name, {}).get('is_supported')

def get_spec_sql_type(entry, backend):
    return entry['backends'].get(backend.name, {}).get('result_type')

def get_data(entry, data, backend = None):

    if backend:
        req_bool = entry["backends"].get(backend.name, {}).get('input_type') == 'bool'
    else:
        req_bool = False

    return data['bool'] if req_bool else data[entry['accessor']]

def skip_no_mutate(entry, backend):
    if "no_mutate" in entry['backends'].get(backend.name, {}).get('flags', []):
        pytest.skip("No support for mutate in backend: %s" %backend.name)

def skip_no_summarize(entry, backend):
    if "no_aggregate" in entry['backends'].get(backend.name, {}).get('flags', []):
        pytest.skip("No support for summarize in backend: %s" %backend.name)

def do_test_missing_implementation(entry, backend):
    # Check whether test should xfail, skip, or -------------------------------
    supported = get_spec_backend_is_supported(entry, backend)

    if not supported:
        pytest.skip("Spec'd failure")

    #if get_spec_no_mutate(entry, backend):
    #    pytest.skip("Spec'd failure")

    ## case: Needs to be implmented
    ## TODO(table): uses xfail
    #if backend_status == "todo":
    #    pytest.xfail("TODO: impelement this translation")
    #
    ## case: Can't be used in a mutate (e.g. a SQL ordered set aggregate function)
    ## TODO(table): no_mutate

    ## case: won't be implemented
    #if get_spec_backend_status(entry, backend.name) == "wontdo":
    #    pytest.skip()


def get_df_expr(entry):
    str_expr = str(entry['expr_frame'])
    #call_expr = strip_symbolic(eval(str_expr, {'_': _}))
    return str_expr, entry["expr_frame"]

    return str_expr, call_expr

def cast_result_type(entry, backend, ser):
    sql_type = get_spec_sql_type(entry, backend)
    if isinstance(backend, SqlBackend) and sql_type == 'float':
        return ser.astype('float')
    elif isinstance(backend, SqlBackend) and sql_type == 'int':
        return ser.astype('int')
    
    return ser

# Tests =======================================================================


def test_series_against_call(entry):
    # TODO: this test originally was evaluating string representations created
    # by calls. I've changed it to just executing the calls directly.
    # not sure the original intent of the test?
    if entry['kind'] == "window":
        pytest.skip()

    df = data[entry['accessor']]
    # TODO: once reading from yaml, no need to repr
    str_expr = str(entry['expr_series'])

    call_expr = entry['expr_series']#  strip_symbolic(eval(str_expr, {'_': _}))
    res = call_expr(df.x)

    dst = call_expr(df.x)# eval(str_expr, {'_': df.x})
    
    assert res.__class__ is dst.__class__
    assert_src_array_equal(res, dst)


def test_frame_expr(entry):
    # TODO: remove this test, not checking anything new
    df = data[entry['accessor']]
    # TODO: once reading from yaml, no need to repr
    str_expr = str(entry['expr_frame'])

    call_expr = entry['expr_frame']
    res = call_expr(df)

    dst = call_expr(df)
    
    assert res.__class__ is dst.__class__
    assert_src_array_equal(res, dst)


def test_pandas_grouped_frame_fast_not_implemented(notimpl_entry):
    from siuba.experimental.pd_groups.dialect import fast_mutate
    gdf = data[notimpl_entry['accessor']].groupby('g')

    # TODO: once reading from yaml, no need to repr
    call_expr = notimpl_entry['expr_frame']

    with pytest.warns(UserWarning):
        try:
            # not implemented functions are punted to apply, and
            # not guaranteed to work (e.g. many lengthen arrays, etc..)
            res = fast_mutate(gdf, result = call_expr)
        except:
            pass
    


#@backend_pandas
def test_frame_mutate(skip_backend, backend, entry):
    do_test_missing_implementation(entry, backend)
    skip_no_mutate(entry, backend)


    # Prepare input data ------------------------------------------------------
    # case: inputs must be boolean
    crnt_data = get_data(entry, DATA, backend)
    df = backend.load_cached_df(crnt_data)

    # Execute mutate ----------------------------------------------------------
    str_expr, call_expr = get_df_expr(entry)

    # Run test for equality w/ ungrouped pandas ----
    dst = crnt_data.assign(result = call_expr(crnt_data))
    dst['result'] = cast_result_type(entry, backend, dst['result'])

    result_type = get_spec_sql_type(entry, backend)
    if result_type == "variable":
        kwargs = {"check_dtype": False, "atol": 1e-03}
    else:
        kwargs = {}

    assert_equal_query(
            df,
            arrange(_.id) >> mutate(result = call_expr),
            dst,
            **kwargs
            )

    # Run test for equality w/ grouped pandas ----
    g_dst = crnt_data.groupby('g').apply(lambda d: d.assign(result = call_expr)).reset_index(drop = True)
    g_dst['result'] = cast_result_type(entry, backend, g_dst['result'])
    assert_equal_query(
            df,
            arrange(_.id) >> group_by(_.g) >> mutate(result = call_expr),
            g_dst,
            **kwargs
            )


def test_pandas_grouped_frame_fast_mutate(entry):
    from siuba.experimental.pd_groups.dialect import fast_mutate, DataFrameGroupBy
    gdf = get_data(entry, DATA).groupby('g')

    # Execute mutate ----------------------------------------------------------
    str_expr, call_expr = get_df_expr(entry)

    res = fast_mutate(gdf, result = call_expr)
    dst = mutate(gdf, result = call_expr)

    # TODO: apply mark to skip failing tests, rather than casting?
    # in pandas 1.2, grouped agg returns int, ungrouped agg returns float
    # in pandas 1.3, grouped agg returns float, same as ungrouped agg
    # (the difference is because the grouped agg in 1.2 did not use cython,
    # and tries casting back to the original column dtype)
    res_obj = res.obj
    if str_expr == '_.x.median()':
        res_obj['result'] = res_obj['result'].astype(float)

    assert isinstance(dst, DataFrameGroupBy)
    assert_frame_equal(res_obj, dst.obj)


def test_frame_summarize(skip_backend, backend, agg_entry):
    entry = agg_entry

    do_test_missing_implementation(entry, backend)
    skip_no_summarize(entry, backend)

    # Prepare input data ------------------------------------------------------
    # case: inputs must be boolean
    crnt_data = get_data(entry, DATA, backend)
    df = backend.load_cached_df(crnt_data)

    # Execute mutate ----------------------------------------------------------
    str_expr, call_expr = get_df_expr(entry)

    dst = data_frame(result = call_expr(crnt_data))

    # Process output ----------------------------------------------------------
    # case: output is of a different type than w/ pandas
    dst['result'] = cast_result_type(entry, backend, dst['result'])

    result_type = get_spec_sql_type(entry, backend)
    if result_type == "variable":
        kwargs = {"check_dtype": False, "atol": 1e-03}
    else:
        kwargs = {}

    # Run test for equality w/ pandas ----
    # otherwise, verify returns same result as mutate
    assert_equal_query(
            df,
            summarize(result = call_expr),
            dst,
            **kwargs
            )

    dst_g = crnt_data.groupby('g').apply(call_expr).reset_index().rename(columns = {0: 'result'})
    dst_g["result"] = cast_result_type(entry, backend, dst_g['result'])
    assert_equal_query(
            df,
            group_by(_.g) >> summarize(result = call_expr),
            dst_g,
            **kwargs
            )


def test_pandas_grouped_frame_fast_summarize(agg_entry):
    from siuba.experimental.pd_groups.dialect import fast_summarize, DataFrameGroupBy
    gdf = get_data(agg_entry, DATA).groupby('g')

    # Execute summarize ----------------------------------------------------------
    str_expr, call_expr = get_df_expr(agg_entry)

    res = fast_summarize(gdf, result = call_expr)
    dst = summarize(gdf, result = call_expr)

    # TODO: apply mark to skip failing tests, rather than downcast
    # pandas grouped aggs, when not using cython, _try_cast back to original type
    # but since summarize uses apply, it doesn't :/. Currently only affects median func.
    if str_expr == '_.x.median()':
        res['result'] = res['result'].astype(float)

    assert_frame_equal(res, dst)



# Edge Cases ==================================================================

@pytest.mark.postgresql
def test_frame_set_aggregates_postgresql():
    # TODO: probably shouldn't be creating backend here
    backend = SqlBackend("postgresql")
    dfs = backend.load_cached_df(data[None])
    
    expr = _.x.quantile(.75)
    assert_equal_query(
            dfs,
            group_by(_.g) >> summarize(result = expr),
            data_frame(g = ['a', 'b'], result = [11., 13.])
            )


