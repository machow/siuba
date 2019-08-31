import pytest
from siuba.dply.forcats import fct_recode, fct_collapse

import pandas as pd
from pandas.testing import assert_series_equal

@pytest.fixture(scope = "function")
def series1():
    yield pd.Series(["pandas", "dplyr", "ggplot2", "plotnine"])

@pytest.fixture(scope = "function")
def cat1():
    yield pd.Categorical(["pandas", "dplyr", "ggplot2", "plotnine"])

@pytest.fixture(scope = "function")
def df1():
    yield pd.DataFrame({
        "repo": ["pandas", "dplyr", "ggplot2", "plotnine"],
        "owner": ["pandas-dev", "tidyverse", "tidyverse", "has2k1"],
        "language": ["python", "R", "R", "python"],
        "stars": [17800, 2800, 3500, 1450],
        "x": [1,2,3,None]
        })

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
    # out2 = pd.Series(cat1.get_values()).apply(lambda x: mapping2[x]).astype("category")
    out2 = pd.Categorical(["python", "r", "ggplot2", "python"])

    assert(out1.to_list() == out2.to_list())
