from siuba.sql.verbs import collect, show_query, LazyTbl
from siuba.dply.verbs import Pipeable
from .helpers import data_frame
import pandas as pd

import pytest

@pytest.fixture(scope = "module")
def df(backend):
    return backend.load_df(data_frame(x = [1,2,3]))

def test_show_query(df):
    assert isinstance(show_query(df), LazyTbl)
    assert isinstance(df >> show_query(), LazyTbl)
    assert isinstance(show_query(), Pipeable)

def test_collect(df):
    assert isinstance(collect(df), pd.DataFrame)
    assert isinstance(df >> collect(), pd.DataFrame)
    assert isinstance(collect(), Pipeable)
