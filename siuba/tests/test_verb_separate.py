from siuba import _, group_by, separate, extract, unite

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

def test_extract_default(df):
    assert_equal_query(
            df,
            extract("label", into = ["season"]),
            data_frame(season = ["S1", "S1"])
            )


# regexes ----
def test_separate_sep_arg(df):
    assert_equal_query(
            df,
            separate("label", into = ["season", "episode"], sep = "E"),
            data_frame(season = ["S1-", "S1-"], episode = ["1", "2"]),
            )

def test_extract_regex_arg(df):
    assert_equal_query(
            df,
            extract("label", into = ["season", "episode"], regex = "S([0-9]+)-E([0-9]+)"),
            data_frame(season = ["1", "1"], episode = ["1", "2"]),
            )

def test_extract_regex_arg_ignores_named(df):
    assert_equal_query(
            df,
            extract("label", into = ["season"], regex = "(?P<digit>[0-9]+)"),
            data_frame(season = ["1", "1"]),
            )


# conversions ----
def test_separate_convert_arg():
    data = data_frame(label = ["1-1", "2-a"])
    assert_equal_query(
            data,
            separate("label", into = ["season", "episode"], convert = True),
            data_frame(season = [1, 2], episode = ["1", "a"])
            )

def test_extract_convert_arg():
    data = data_frame(label = ["1-1", "2-a"])
    assert_equal_query(
            data,
            extract("label", into = ["season", "episode"], convert = True, regex = "(.)-(.)"),
            data_frame(season = [1, 2], episode = ["1", "a"])
            )

# misc ----
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

def test_extract_remove_arg(df):
    assert_equal_query(
            df,
            extract("label", into = ["season"], remove = False),
            df.assign(season = ["S1", "S1"])
            )

# unite =======================================================================

def test_unite_round_trip():
    data = data_frame(label = ["s1_e1", "s2_e2"])
    separated = separate(data, "label", into = ["season", "episode"])
    united = unite(separated, "label", "season", "episode")

    assert_frame_sort_equal(united, data)

def test_unite_round_trip_grouped_df():
    data = data_frame(label = ["s1_e1", "s2_e2"])
    separated = separate(data.groupby('label'), "label", into = ["season", "episode"])
    united = unite(separated, "label", "season", "episode")

    assert_frame_sort_equal(united, data)


def test_unite_missing_column_error():
    data = data_frame(a = ["a"], b = ["b"])

    with pytest.raises(ValueError):
        # TODO: should check message includes name of col
        united = unite(data, "label", "a", "c")

