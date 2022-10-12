from siuba.dply.verbs import collect, show_query, mutate
from siuba.sql import LazyTbl
from siuba.dply.verbs import Pipeable
from siuba.tests.helpers import SqlBackend, data_frame
from siuba import _

import pandas as pd

import pytest

pg_backend = SqlBackend("postgresql")

@pytest.fixture(scope = "module")
def df_tiny():
    return pg_backend.load_df(data_frame(x = [1,2]))

@pytest.fixture(scope = "module")
def df_wide():
    return pg_backend.load_df(data_frame(x = [1,2], y = [3,4], z = [5, 6]))

def rename_source(query, tbl):
    return query.replace(tbl.tbl.name, "SRC_TBL")


def test_show_query_basic(df_tiny):
    q = df_tiny >> mutate(a = _.x.mean()) >> show_query(return_table = False)

    assert rename_source(str(q), df_tiny) == """\
SELECT SRC_TBL.x, avg(SRC_TBL.x) OVER () AS a 
FROM SRC_TBL"""

def test_show_query_basic_simplify(df_tiny):
    q = df_tiny >> mutate(a = _.x.mean()) >> show_query(return_table = False, simplify=True)

    assert rename_source(str(q), df_tiny) == """\
SELECT *, avg(SRC_TBL.x) OVER () AS a 
FROM SRC_TBL"""

def test_show_query_complex_simplify(df_wide):
    q = df_wide >>  mutate(a = _.x.mean(), b = _.a.mean())
    res = q >> show_query(return_table = False, simplify=True)

    assert rename_source(str(res), df_wide) == """\
SELECT *, avg(anon_1.a) OVER () AS b 
FROM (SELECT *, avg(SRC_TBL.x) OVER () AS a 
FROM SRC_TBL) AS anon_1"""

