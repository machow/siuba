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
            df.assign(season = ["S1", "S1"], episode = ["E1", "E2"])
            )


def test_separate_sep_arg(df):
    assert_equal_query(
            df,
            separate("label", into = ["season", "episode"], sep = "E"),
            df.assign(season = ["S1-", "S1-"], episode = ["1", "2"]),
            )

def test_separate_convert_arg():
    data = data_frame(label = ["1-1", "2-a"])
    assert_equal_query(
            data,
            separate("label", into = ["season", "episode"], sep = "E", convert = True),
            data.assign(season = [1, 2], episode = ["1", "a"])
            )

def test_separate_warn_arg_warn():
    data = data_frame(label = "1-2-3-4")
    with pytest.warns(UserWarning):
        separate(data, "label", into = ["a", "b"], sep = "-")

def test_separate_warn_arg_error():
    data = data_frame(label = "1-2-3-4")
    with pytest.raises():
        separate(data, "label", into = ["a", "b"], sep = "-")




