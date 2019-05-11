"""
Note: this test file was heavily influenced by its dbplyr counterpart.

https://github.com/tidyverse/dbplyr/blob/master/tests/testthat/test-verb-distinct.R
"""
    
from siuba.sql import LazyTbl, collect
from siuba import _, distinct
import pandas as pd
import os

import pytest
from sqlalchemy import create_engine

from .helpers import assert_equal_query, DbConRegistry

DATA = pd.DataFrame({
    "x": [1,1,1,1],
    "y": [1,1,2,2],
    "z": [1,2,1,2]
    })

@pytest.fixture(scope = "module")
def dbs(request):
    dialects = set(request.config.getoption("--dbs").split(","))
    dbs = DbConRegistry()

    if "sqlite" in dialects:
        dbs.register("sqlite", create_engine("sqlite:///:memory:"))
    if "postgresql" in dialects:
        port = os.environ.get("PGPORT", "5433")
        dbs.register("postgresql", create_engine('postgresql://postgres:@localhost:%s/postgres'%port))


    yield dbs

    # cleanup
    for engine in dbs.connections.values():
        engine.dispose()

@pytest.fixture(scope = "module")
def dfs(dbs):
    yield dbs.load_df(DATA)

def test_distinct_no_args(dfs):
    assert_equal_query(dfs, distinct(), DATA.drop_duplicates())
    assert_equal_query(dfs, distinct(), distinct(DATA))

def test_distinct_one_arg(dfs):
    assert_equal_query(
            dfs,
            distinct(_.y),
            DATA.drop_duplicates(['y'])[['y']].reset_index(drop = True)
            )

    assert_equal_query(dfs, distinct(_.y), distinct(DATA, _.y))

def test_distinct_keep_all_not_impl(dfs):
    # TODO: should just mock LazyTbl
    for tbl in dfs:
        with pytest.raises(NotImplementedError):
            distinct(tbl, _.y, _keep_all = True) >> collect()
    

@pytest.mark.xfail
def test_distinct_via_group_by(dfs):
    # NotImplemented
    assert False

def test_distinct_kwargs(dfs):
    dst = DATA.drop_duplicates(['y', 'x']) \
              .rename(columns = {'x': 'a'}) \
              .reset_index(drop = True)[['y', 'a']]

    assert_equal_query(dfs, distinct(_.y, a = _.x), dst)


    

