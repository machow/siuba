import pytest
from pandas.testing import assert_series_equal
from siuba.dply import vector as v
from siuba.siu import _, Call, Symbolic, strip_symbolic
from pandas import Series

# TODO: ideally all vector functions should dispatch, and return a like-type result for...
#         - numpy arrays
#         - pandas Series
#         - array likes (e.g. lists, where valid?)

@pytest.fixture
def x():
    return Series([0,1,2])

@pytest.mark.parametrize('v_func, res', [
    (v.cumall, [False, False, False]),
    (v.cumany, [False, True, True]),
    (v.cummean, [0, .5, 1]),
    (v.desc, [2,1,0]),
    (v.dense_rank, Series([1,2,3], dtype = float)),     # Note: current pandas impl returns float, not int?
    #(v.percent_rank, ["TODO"]),  # NotImplementedError
    (v.min_rank, Series([1,2,3], dtype = float)),
    (v.cume_dist, [1/3., 2/3., 3/3.]),
    (v.row_number, [1,2,3]),
    (v.lead, [1, 2, None]),
    (v.lag, [None, 0, 1]),
    # TODO: v.near, v.nth, v.first, v.last
    ])
def test_vector_unary_and_dispatch(x, v_func, res):
    target = Series(res) if not isinstance(res, Series) else res

    assert_series_equal(v_func(x), target)

    # symbolic call argument
    sym_call = v_func(_)
    assert isinstance(sym_call, Symbolic)
    assert_series_equal(sym_call(x), target)

    # call argument
    call = v_func(strip_symbolic(_))
    assert isinstance(call, Call)
    assert_series_equal(call(x), target)


# n needs own test, since returns 0 dim object (int)
# this allows broadcasting in cases like some_col + n(some_col)
def test_vector_n(x):
    assert v.n(x) == 3

def test_vector_n_distinct(x):
    assert v.n_distinct(x) == 3

# Non-unary vector funcs ----

def test_vector_between(x):
    out = v.between(x, 0, 1)
    assert_series_equal(out, Series([True, True, False]))

def test_vector_na_if(x):
    out = v.na_if(x, [0, 2])
    assert_series_equal(out, Series([None, 1, None]))


# Need to be implemented ----

@pytest.mark.skip("TODO: Not Implemented")
def test_vector_percent_rank(x):
    pass

@pytest.mark.skip("TODO: Not Implemented")
def test_vector_coalesce(x):
    pass

@pytest.mark.skip("TODO: Not Implemented")
def test_vector_ntile(x):
    pass

@pytest.mark.skip("TODO: Not Implemented")
def test_vector_near(x):
    pass

@pytest.mark.skip("TODO: Not Implemented")
def test_vector_nth(x):
    pass

@pytest.mark.skip("TODO: Not Implemented")
def test_vector_first(x):
    pass

@pytest.mark.skip("TODO: Not Implemented")
def test_vector_last(x):
    pass
