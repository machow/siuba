import pytest
import pandas as pd
from numpy.testing import assert_equal
from pandas.testing import assert_series_equal, assert_index_equal
from siuba.dply.forcats import fct_recode, fct_collapse

@pytest.fixture
def series1():
    yield pd.Series(["pandas", "dplyr", "ggplot2", "plotnine"])

@pytest.fixture
def cat1():
    yield pd.Categorical(["pandas", "dplyr", "ggplot2", "plotnine"])

# Need to ensure all functions...
#  - 1. can take a series or array
#  - 2. handle a symbolic or call
#  - 3. handle names with spaces

# just a little shorter to write...
def factor(values, categories, ordered = False):
    return pd.Categorical(values, categories, ordered)

def assert_cat_equal(a, b):
    assert isinstance(a, pd.Categorical)
    assert isinstance(b, pd.Categorical)

    assert_index_equal(a.categories, b.categories)
    assert_equal(a.codes, b.codes)
    assert a.ordered == b.ordered


def test_forcats_fct_recode(cat1):
    out1 = fct_recode(cat1, R="dplyr")
    out2 = cat1.rename_categories({"dplyr": "R"})

    assert(out1.to_list() == out2.to_list())


def test_forcats_fct_collapse(cat1):
    mapping1 = {
        "python": ["pandas", "plotnine"],
        "r": "dplyr",
    }
    out1 = fct_collapse(cat1, mapping1)
    out2 = pd.Categorical(["python", "r", "ggplot2", "python"])

    assert_cat_equal(out1, out2)
