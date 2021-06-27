"""
Note: this test file was heavily influenced by its dbplyr counterpart.

https://github.com/tidyverse/dbplyr/blob/master/tests/testthat/test-verb-distinct.R
"""
    
from siuba.sql import LazyTbl
from siuba import _, distinct, group_by, summarize, arrange, mutate, collect
from .helpers import assert_equal_query, backend_sql
import pandas as pd
import os

import pytest
from sqlalchemy import create_engine

DATA = pd.DataFrame({
    "x": [1,2,3,4,5],
    "y": [5,4,3,2,1]
    })

@pytest.fixture(scope = "module")
def df(backend):
    yield backend.load_df(DATA)

def test_distinct_no_args(df):
    assert_equal_query(df, distinct(), DATA.drop_duplicates())
    assert_equal_query(df, distinct(), distinct(DATA))

def test_distinct_one_arg(df):
    assert_equal_query(
            df,
            distinct(_.y),
            DATA.drop_duplicates(['y'])[['y']].reset_index(drop = True)
            )

    assert_equal_query(df, distinct(_.y), distinct(DATA, _.y))

@backend_sql
def test_distinct_keep_all_not_impl(backend, df):
    # TODO: should just mock LazyTbl
    with pytest.raises(NotImplementedError):
        distinct(df, _.y, _keep_all = True) >> collect()
    

@pytest.mark.xfail
def test_distinct_via_group_by(df):
    # NotImplemented
    assert False


def test_distinct_after_summarize(df):
    query = group_by(g = _.x) >> summarize(z = (_.y - _.y).min()) >> distinct(_.z)
    
    assert_equal_query(df, query, pd.DataFrame({'z': [0]}))

def test_distinct_after_arrange(df):
    query = arrange(_.x) >> distinct(_.y)

    assert_equal_query(df, query, pd.DataFrame({'y': [5,4,3,2,1]}))


def test_distinct_of_mutate_col(df):
    query = mutate(z = _.x + 1) >>  distinct(_.z)

    assert_equal_query(df, query, pd.DataFrame({'z': [2,3,4,5,6]}))


def test_distinct_kwargs(df):
    dst = DATA.drop_duplicates(['y', 'x']) \
              .rename(columns = {'x': 'a'}) \
              .reset_index(drop = True)[['y', 'a']]

    assert_equal_query(df, distinct(_.y, a = _.x), dst)


    

