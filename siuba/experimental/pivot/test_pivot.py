"""
Note: this test file was heavily influenced by its dbplyr counterpart.

https://github.com/tidyverse/tidyr/blob/master/tests/testthat/test-pivot-long.R
"""

from . import pivot_longer

import pytest
from siuba.siu import Symbolic
from siuba.tests.helpers import data_frame, assert_frame_sort_equal
from pandas.testing import assert_frame_equal

_ = Symbolic()

def test_pivot_all_cols_to_long():
    "can pivot all cols to long"

    src = data_frame(x = [1,2], y = [3,4])
    dst = data_frame(name = ["x", "y", "x", "y"], value = [1, 3, 2, 4])
    
    res = pivot_longer(src, _["x":"y"])

    assert_frame_sort_equal(res, dst)

