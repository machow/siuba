from pandas.tseries.offsets import Nano, Day, Hour, MonthEnd
from pandas import Timestamp
import pandas as pd
from pandas.core.arrays import DatetimeArray

import pandas.testing as tm
from pandas import offsets
from siuba.experimental.datetime import floor_date, ceil_date

import pytest

# TODO: currently testing only DatetimeIndex, also need to test PeriodIndex
#       and the array classes

def check_floor_ceil(src, offset, unit):
    # Test DatetimeIndex
    res_low = floor_date(src + offset, unit)
    res_hi  = ceil_date(src - offset, unit)

    tm.assert_index_equal(res_low, src)
    tm.assert_index_equal(res_hi, src)

    # Test series ----
    ser_src = pd.Series(src)
    ser_low = floor_date(ser_src + offset, unit)
    ser_hi =  ceil_date(ser_src - offset, unit)

    tm.assert_series_equal(ser_low, ser_src)
    tm.assert_series_equal(ser_hi, ser_src)

@pytest.mark.parametrize("unit", [
    "S",
    "D"
    ])
def test_tick(unit):
    src = pd.DatetimeIndex(["2020-01-02"])
    check_floor_ceil(src, Nano(), unit)


@pytest.mark.parametrize("offset", [Nano(), Day(), Hour()])
def test_nonperiodic_month(offset):
    src  = pd.DatetimeIndex(["2020-02-01"]) 
    check_floor_ceil(src, offset, "MS")


@pytest.mark.parametrize("offset", [Nano(), Day(), Hour()])
def test_floor_nonperiodic_month_end(offset):
    src  = pd.DatetimeIndex(["2020-01-31"]) 
    check_floor_ceil(src, offset, "M")


@pytest.mark.parametrize("offset", [Nano(), Day(), Hour(), MonthEnd()])
def test_floor_nonperiodic_year(offset):
    src = pd.DatetimeIndex(["2020-01-01"]) 
    check_floor_ceil(src, offset, "AS")


@pytest.mark.parametrize("offset", [Nano(), Day(), Hour(), MonthEnd()])
def test_floor_nonperiodic_year_end(offset):
    src = pd.DatetimeIndex(["2019-12-31"]) 
    check_floor_ceil(src, offset, "A")

def test_floor_anchor():
    src = pd.DatetimeIndex(["2020-02-01"])
    res = floor_date(src, "MS")

    tm.assert_index_equal(res, src)


def test_floor_idempotent():
    src = pd.DatetimeIndex(["2020-02-01"])
    res = floor_date(floor_date(src + Day(), "MS"), "MS")

    tm.assert_index_equal(res, src)


def test_ceil_idempotent():
    src = pd.DatetimeIndex(["2020-02-01"])
    res = ceil_date(ceil_date(src - Day(), "MS"), "MS")

    tm.assert_index_equal(res, src)

# Period ----

@pytest.mark.parametrize("unit", [
    "S",
    "D",
    "M"
    ])
def test_period_floor_tick(unit):
    src = pd.DatetimeIndex(["2020-01-02"])
    res = floor_date((src + Nano()).to_period("ns"), unit)
    tm.assert_index_equal(res, src.to_period(unit))


def test_period_ceil_fail():
    with pytest.raises(NotImplementedError):
        ceil_date(pd.PeriodIndex(["2020-01-01"], freq = "D"), "D")
