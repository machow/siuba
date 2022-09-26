import pandas as pd
import pytest

from siuba.dply.forcats import fct_reorder, fct_recode, fct_collapse, fct_lump, fct_inorder, fct_infreq
from pandas import Categorical

try:
    # pandas < v1
    from pandas.util.testing import assert_categorical_equal
except ImportError:
    from pandas._testing import assert_categorical_equal


def assert_fct_equal(x, y):
    return assert_categorical_equal(x,  y)

# fct_inorder -----------------------------------------------------------------

@pytest.mark.parametrize("x, dst_categories", [
    (["c", "a", "c", "b", "b"], ["c", "a", "b"]),
    (["c", "a", "c", "b", "b", None], ["c", "a", "b"])
])
def test_fct_inorder(x, dst_categories):
    dst = pd.Categorical(x, categories=dst_categories)

    res1 = fct_inorder(x)

    assert isinstance(res1, pd.Categorical)
    assert_categorical_equal(res1, dst)

    res2 = fct_inorder(pd.Series(x))

    assert isinstance(res2, pd.Series)
    assert_fct_equal(res2.array, dst)

    res3 = fct_inorder(pd.Categorical(x))

    assert isinstance(res3, pd.Categorical)
    assert_fct_equal(res3, dst)


@pytest.mark.parametrize("x, dst_categories", [
    (["c", "c", "b", "c", "a", "a"], ["c", "a", "b"]),              # no ties
    (["c", "c", "b", "c", "a", "a", "a"], ["c", "a", "b"]),          # ties
    (["c", "c", "b", "c", "a", "a", "a", None], ["c", "a", "b"])    # None
])
def test_fct_infreq(x, dst_categories):
    if pd.__version__.startswith("1.1.") or pd.__version__.startswith("1.2."):
        pytest.skip()

    dst = pd.Categorical(x, categories=dst_categories)

    res1 = fct_infreq(x)

    assert isinstance(res1, pd.Categorical)
    assert_categorical_equal(res1, dst)

    res2 = fct_infreq(pd.Series(x))

    assert isinstance(res2, pd.Series)
    assert_categorical_equal(res2.array, dst)

    res3 = fct_infreq(pd.Categorical(x))

    assert isinstance(res3, pd.Categorical)
    assert_categorical_equal(res3, dst)



# fct_reorder -----------------------------------------------------------------

def test_fct_reorder_simple():
    res = fct_reorder(['a', 'a', 'b'], [4, 3, 2])
    dst = Categorical(['a', 'a', 'b'], ['b', 'a'])

    assert_fct_equal(res, dst)
    
def test_fct_reorder_simple_upcast():
    res = fct_reorder(pd.Series(['a', 'a', 'b']), [4, 3, 2])
    dst = Categorical(['a', 'a', 'b'], ['b', 'a'])

    assert isinstance(res, pd.Series)
    assert_fct_equal(res.array, dst)

def test_fct_reorder_desc():
    res = fct_reorder(['a', 'a', 'b'], [4, 3, 2], desc = True)
    dst = Categorical(['a', 'a', 'b'], ['a', 'b'])

    assert_fct_equal(res, dst)

def test_fct_reorder_desc_upcast():
    res = fct_reorder(pd.Series(['a', 'a', 'b']), [4, 3, 2], desc = True)
    dst = Categorical(['a', 'a', 'b'], ['a', 'b'])

    assert isinstance(res, pd.Series)
    assert_fct_equal(res.array, dst)

def test_fct_reorder_custom_func():
    import numpy as np
    res = fct_reorder(['x', 'x', 'y'], [4, 0, 2], np.max)
    dst = Categorical(['x', 'x', 'y'], ['y', 'x'])

    assert_fct_equal(res, dst)

def test_fct_reorder_na_fct():
    import numpy as np
    res = fct_reorder([None, 'x', 'y'], [4, 3, 2], np.max)
    dst = Categorical([None, 'x', 'y'], ['y', 'x'])

    assert_fct_equal(res, dst)

# fct_recode ------------------------------------------------------------------

def test_fct_recode_simple():
    cat = ['a', 'b', 'c']
    res = fct_recode(cat, z = 'c')
    dst = Categorical(['a', 'b', 'z'], ['a', 'b', 'z'])

    assert_fct_equal(res, dst)

def test_fct_recode_simple_upcast():
    ser = pd.Series(['a', 'b', 'c'])
    res = fct_recode(ser, z = 'c')
    dst = Categorical(['a', 'b', 'z'], ['a', 'b', 'z'])

    assert isinstance(ser, pd.Series)
    assert_fct_equal(res.array, dst)


# fct_collapse ----------------------------------------------------------------

def test_fct_collapse_simple():
    res = fct_collapse(['a', 'b', 'c'], {'x': 'a'})
    dst = Categorical(['x', 'b', 'c'], ['x', 'b', 'c'])

    assert_fct_equal(res, dst)

def test_fct_collapse_simple_upcast():
    res = fct_collapse(pd.Series(['a', 'b', 'c']), {'x': 'a'})
    dst = Categorical(['x', 'b', 'c'], ['x', 'b', 'c'])

    assert isinstance(res, pd.Series)
    assert_fct_equal(res.array, dst)

def test_fct_collapse_others():
    res = fct_collapse(['a', 'b', 'c'], {'x': 'a'}, group_other = 'others')
    dst = Categorical(['x', 'others', 'others'], ['x', 'others'])

    assert_fct_equal(res, dst)

def test_fct_collapse_other_always_last():
    res = fct_collapse(['a', 'b', 'c'], {'x': 'c'}, group_other = 'others')
    dst = Categorical(['others', 'others', 'x'], ['x', 'others'])

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

def test_fct_lump_n_upcast():
    res = fct_lump(pd.Series(['a', 'a', 'b', 'c']), n = 1)
    dst = Categorical(['a', 'a', 'Other', 'Other'], ['a', 'Other'])

    assert isinstance(res, pd.Series)
    assert_fct_equal(res.array, dst)

def test_fct_lump_prop():
    res = fct_lump(['a', 'a', 'b', 'b', 'c', 'd'], prop = .2)
    dst = Categorical(['a', 'a', 'b', 'b', 'Other', 'Other'], ['a', 'b', 'Other'])

    assert_fct_equal(res, dst)
