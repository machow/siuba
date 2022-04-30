import numpy as np
import pytest

from siuba.siu import strip_symbolic, FunctionLookupError, Symbolic, MetaArg, Call, _ as D


# Note that currently tests are split across the test_siu.py, and this module.

@pytest.fixture
def _():
    return Symbolic()

def test_siu_symbolic_np_array_ufunc_call(_):
    sym = np.add(_, 1)
    expr = strip_symbolic(sym)

    # structure:
    # █─'__call__'
    # ├─█─'__custom_func__'
    # │ └─<function array_ufunc at 0x103aa3820>
    # ├─_
    # ├─<ufunc 'add'>
    # ├─'__call__'
    # ├─_
    # └─1

    assert len(expr.args) == 6
    assert expr.args[1] is strip_symbolic(_)        # original dispatch obj
    assert expr.args[2] is np.add                   # ufunc object
    assert expr.args[3] == "__call__"               # its method to use
    assert expr.args[4] is strip_symbolic(_)        # lhs input
    assert expr.args[5] == 1                        # rhs input


def test_siu_symbolic_np_array_ufunc_inputs_lhs(_):
    lhs = np.array([1,2])
    rhs = np.array([3,4])
    res = lhs + rhs

    # symbol on lhs ----

    sym = np.add(_, rhs)
    expr = strip_symbolic(sym)

    assert np.array_equal(expr(lhs), res)


def test_siu_symbolic_np_array_ufunc_inputs_rhs(_):
    lhs = np.array([1,2])
    rhs = np.array([3,4])
    res = lhs + rhs

    # symbol on rhs ----

    sym2 = np.add(lhs, _)
    expr2 = strip_symbolic(sym2)

    assert np.array_equal(expr2(rhs), res)


@pytest.mark.xfail
def test_siu_symbolic_np_array_function(_):
    # Note that np.sum is not a ufunc, but sort of reduces on a ufunc under the
    # hood, so fails when called on a symbol
    sym = np.sum(_)
    expr = strip_symbolic(sym)

    assert expr(np.array([1,2])) == 3


@pytest.mark.parametrize("func", [
    np.absolute,   # a ufunc
    np.sum         # dispatched by __array_function__
    ])
def test_siu_symbolic_array_ufunc_sql_raises(_, func):
    from siuba.sql.utils import mock_sqlalchemy_engine
    from siuba.sql import LazyTbl
    from siuba.sql import SqlFunctionLookupError

    lazy_tbl = LazyTbl(mock_sqlalchemy_engine("postgresql"), "somedata", ["x", "y"])
    with pytest.raises(SqlFunctionLookupError) as exc_info:
        lazy_tbl.shape_call(strip_symbolic(func(_.x)))

    assert "Numpy function sql translation" in exc_info.value.args[0]
    assert "not supported" in exc_info.value.args[0]



@pytest.mark.parametrize("sym, res", [
    (np.sqrt(D), lambda ser: np.sqrt(ser)),    # ufunc
    (np.add(D, 1), lambda ser: np.add(ser, 1)),
    (np.add(1, D), lambda ser : np.add(1, ser)),
    (np.add(D, D), lambda ser : np.add(ser, ser)),
])
def test_siu_symbolic_array_ufunc_pandas(_, sym, res):
    import pandas as pd
    ser = pd.Series([1,2])

    expr = strip_symbolic(sym)

    src = expr(ser)
    dst = res(ser)

    assert isinstance(src, pd.Series)
    assert src.equals(dst)


@pytest.mark.parametrize("sym, res", [
    (np.mean(D), lambda ser : np.mean(ser)),     # __array_function__
    (np.sum(D), lambda ser: np.sum(ser)), 
    (np.sqrt(np.mean(D)), lambda ser: np.sqrt(np.mean(ser))), 
])
def test_siu_symbolic_array_function_pandas(_, sym, res):
    import pandas as pd
    ser = pd.Series([1,2])

    expr = strip_symbolic(sym)

    src = expr(ser)
    dst = res(ser)

    # note that all examples currently are aggregates
    assert src == dst
