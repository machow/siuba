import pytest
from siuba.dply.verbs import mutate
from siuba.siu import _

import pandas as pd
from pandas.testing import assert_frame_equal

@pytest.fixture(scope = "function")
def df1():
    yield pd.DataFrame({
        "repo": ["pandas", "dplyr", "ggplot2", "plotnine"],
        "owner": ["pandas-dev", "tidyverse", "tidyverse", "has2k1"],
        "language": ["python", "R", "R", "python"],
        "stars": [17800, 2800, 3500, 1450],
        "x": [1,2,3,None]
        })

def test_dply_mutate(df1):
    op_stars_1k = lambda d: d.stars * 1000
    out1 = mutate(df1, stars_1k = op_stars_1k)
    out2 = df1.assign(stars_1k = op_stars_1k)

    assert_frame_equal(out1, out2)

def test_dply_mutate_sym(df1):
    op_stars_1k = _.stars * 1000
    out1 = mutate(df1, stars_1k = op_stars_1k)
    out2 = df1.assign(stars_1k = op_stars_1k)

    assert_frame_equal(out1, out2)

