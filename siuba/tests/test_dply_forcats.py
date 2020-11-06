from siuba.dply.forcats import fct_reorder, fct_recode, fct_collapse, fct_lump
from pandas import Categorical

try:
    # pandas < v1
    from pandas.util.testing import assert_categorical_equal
except ImportError:
    from pandas._testing import assert_categorical_equal


def assert_fct_equal(x, y):
    return assert_categorical_equal(x,  y)


# fct_reorder -----------------------------------------------------------------

def test_fct_reorder_simple():
    res = fct_reorder(['a', 'a', 'b'], [4, 3, 2])
    dst = Categorical(['a', 'a', 'b'], ['b', 'a'])

    assert_fct_equal(res, dst)
    

def test_fct_reorder_desc():
    res = fct_reorder(['a', 'a', 'b'], [4, 3, 2], desc = True)
    dst = Categorical(['a', 'a', 'b'], ['a', 'b'])

    assert_fct_equal(res, dst)

def test_fct_reorder_custom_func():
    import numpy as np
    res = fct_reorder(['x', 'x', 'y'], [4, 0, 2], np.max)
    dst = Categorical(['x', 'x', 'y'], ['y', 'x'])

    assert_fct_equal(res, dst)


# fct_recode ------------------------------------------------------------------

def test_fct_recode_simple():
    cat = ['a', 'b', 'c']
    res = fct_recode(cat, z = 'c')
    dst = Categorical(['a', 'b', 'z'], ['a', 'b', 'z'])

    assert_fct_equal(res, dst)


# fct_collapse ----------------------------------------------------------------

def test_fct_collapse_simple():
    res = fct_collapse(['a', 'b', 'c'], {'x': 'a'})
    dst = Categorical(['x', 'b', 'c'], ['x', 'b', 'c'])

    assert_fct_equal(res, dst)

def test_fct_collapse_others():
    res = fct_collapse(['a', 'b', 'c'], {'x': 'a'}, group_other = 'others')
    dst = Categorical(['x', 'others', 'others'], ['x', 'others'])

    assert_fct_equal(res, dst)

def test_fct_collapse_many_to_one():
    res = fct_collapse(['a', 'b', 'c'], {'ab': ['a', 'b']})
    dst = Categorical(['ab', 'ab', 'c'], ['ab', 'c'])

    assert_fct_equal(res, dst)


# fct_lump --------------------------------------------------------------------

def test_fct_lump_n():
    res = fct_lump(['a', 'a', 'b', 'c'], n = 1)
    dst = Categorical(['a', 'a', 'Other', 'Other'], ['a', 'Other'])

    assert_fct_equal(res, dst)

def test_fct_lump_prop():
    res = fct_lump(['a', 'a', 'b', 'b', 'c', 'd'], prop = .2)
    dst = Categorical(['a', 'a', 'b', 'b', 'Other', 'Other'], ['a', 'b', 'Other'])

    assert_fct_equal(res, dst)
