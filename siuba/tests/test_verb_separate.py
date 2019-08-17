from siuba import _, group_by, separate

import pytest
from .helpers import assert_equal_query, assert_frame_sort_equal, data_frame, backend_notimpl, backend_sql

DATA = data_frame(label = ["S1-E1", "S1-E2"])

@pytest.fixture(scope = "function")
def df():
    return DATA.copy()

def test_separate_default(df):
    assert_equal_query(
            df,
            separate("label", into = ["season", "episode"]),
            data_frame(season = ["S1", "S1"], episode = ["E1", "E2"])
            )

def test_separate_grouped(df):
    assert_equal_query(
            df.groupby(['label']),
            separate("label", into = ["season", "episode"], remove = False),
            df.assign(season = ["S1", "S1"], episode = ["E1", "E2"])
            )

def test_separate_sep_arg(df):
    assert_equal_query(
            df,
            separate("label", into = ["season", "episode"], sep = "E"),
            data_frame(season = ["S1-", "S1-"], episode = ["1", "2"]),
            )

def test_separate_convert_arg():
    data = data_frame(label = ["1-1", "2-a"])
    assert_equal_query(
            data,
            separate("label", into = ["season", "episode"], convert = True),
            data_frame(season = [1, 2], episode = ["1", "a"])
            )

def test_separate_warn_arg_warn():
    data = data_frame(label = "1-2-3-4")
    with pytest.warns(UserWarning):
        separate(data, "label", into = ["a", "b"], sep = "-")

@pytest.mark.skip("TODO")
def test_separate_warn_arg_merge():
    pass

@pytest.mark.skip("TODO")
def test_separate_fill_arg():
    pass

def test_separate_remove_arg(df):
    assert_equal_query(
            df,
            separate("label", into = ["season", "episode"], remove = False),
            df.assign(season = ["S1", "S1"], episode = ["E1", "E2"])
            )
