import pytest
from .helpers import assert_equal_query, data_frame

from siuba import _, head, group_by, arrange


DATA = data_frame(a = [1,2,3,4,5,6], g = ["x", "y"] * 3)


@pytest.fixture
def df(backend):
    # Note that we use arrange the data, so that cloud backends that do not
    # guarantee order without ORDER BY will return the right thing
    return backend.load_df(DATA) >> arrange(_.a)


def test_head_default(df):
    assert_equal_query(
            df,
            head(),
            DATA.iloc[:5,:]
    )


def test_head_n_arg(df):
    assert_equal_query(
            df,
            head(2),
            DATA.iloc[:2, :]
    )


def test_head_grouped(df):
    assert_equal_query(
            df,
            group_by(_.g) >> head(),
            DATA.iloc[:5, :]
    )


def test_head_grouped_n_arg(df):
    assert_equal_query(
            df,
            group_by(_.g) >> head(2),
            DATA.iloc[:2, :]
    )
