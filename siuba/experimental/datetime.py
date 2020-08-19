from siuba.siu import symbolic_dispatch
from pandas.arrays import DatetimeArray, PeriodArray
from pandas import DatetimeIndex, PeriodIndex, Period, Timestamp, Series

from pandas import offsets
from pandas.core.dtypes.common import is_period_dtype, is_datetime64_any_dtype

from abc import ABC

try:
    from pandas.tseries.offsets import prefix_mapping
except ImportError:
    from pandas._libs.tslibs.offsets import prefix_mapping

LUBRIDATE_OFFSETS = {
        "second": "S",
        "minute": "M",
        "hour": "H",
        "day": "D",
        "week": "W",
        "month": "M",
        "bimonth": "2M",
        "quarter": "Q",
        "season": None,
        "halfyear": None,
        "year": "Y"
        }

# There's no class that clearly identifies all the Datetime op compatible classes,
# so make an ABC and register them on it
def _make_abc(name, subclasses):
    cls = type(name, (ABC,), {})
    for child in subclasses: cls.register(child)
    return cls

DatetimeType = _make_abc("DatetimeType", [DatetimeIndex, DatetimeArray, Timestamp])
PeriodType   = _make_abc("PeriodType",   [PeriodIndex, PeriodArray, Period])

def _get_offset(unit):
    cls = prefix_mapping.get(unit)
    if cls is None:
        raise ValueError("unit {} not a valid offset".format(unit))

    return cls

def _get_series_dispatcher(f, x):
    if is_period_dtype(x):
        return f.registry[PeriodType]
    elif is_datetime64_any_dtype(x):
        return f.registry[DatetimeType]

    raise TypeError("does not seem to be a period or datetime")

DOCSTRING = """
    floor_date and ceil_date return dates rounded to nearest specified unit.

    Args:
        x: a DatetimeIndex, PeriodIndex, or their underlying arrays or elements.
        units: a date or time unit for rounding (eg. "MS" rounds down or up to the start of a month)

    Note:
        For a full list of units run the following:
        
        :: 
            from pandas.tseries.offsets import prefix_mapping
        
        Common time units include seconds (S), minute (T), hour (H), week (W). 
        Some date units are month end (M), month start (MS), year start (AS)


    The main feature of floor_date is that it works on things like Months, which is
    not supported in the floor method.

    >>> import pandas as pd
    >>> a_date = "2020-02-02 02:02:02"
    >>> dti = pd.DatetimeIndex([a_date])
    >>> dti.floor("MS")
    Traceback (most recent call last):
    ...
    ValueError: <MonthBegin> is a non-fixed frequency

    Month start will always go to the first day of a month.

    >>> floor_date(dti, "MS") 
    DatetimeIndex(['2020-02-01'], dtype='datetime64[ns]', freq=None)

    >>> ceil_date(dti, "MS")
    DatetimeIndex(['2020-03-01'], dtype='datetime64[ns]', freq=None)

    On the other hand, here is month end.
    
    >>> floor_date(dti, "M") 
    DatetimeIndex(['2020-01-31'], dtype='datetime64[ns]', freq=None)

    >>> ceil_date(dti, "M")
    DatetimeIndex(['2020-02-29'], dtype='datetime64[ns]', freq=None)

    It also works on things supported by the Series.dt.floor method, like hours.

    >>> floor_date(dti, "H")
    DatetimeIndex(['2020-02-02 02:00:00'], dtype='datetime64[ns]', freq=None)

    You can also use it on other types, like a PeriodIndex

    >>> per = pd.PeriodIndex([a_date], freq = "S")
    >>> floor_date(per, "M")
    PeriodIndex(['2020-02'], dtype='period[M]', freq='M')

"""

# Create single dispatch functions --------------------------------------------

@symbolic_dispatch
def floor_date(x, unit = "S"):
    raise TypeError("floor_date not implemented for class {}".format(type(x)))


@symbolic_dispatch
def ceil_date(x, unit = "S"):
    raise TypeError("ceil_date not implemented for class {}".format(type(x)))

floor_date.__doc__ = DOCSTRING
ceil_date.__doc__  = DOCSTRING


# Floor date ------------------------------------------------------------------

@floor_date.register(DatetimeType)
def _(x, unit = "S"):
    cls_offset = _get_offset(unit)

    if issubclass(cls_offset, offsets.Tick):
        return x.floor(unit)

    # note: x - 0*offset shifts anchor forward for some reason, so we
    # add then subtract to ensure it doesn't change anchor points
    offset = cls_offset(n = 1)
    return x.normalize() + offset - offset          


@floor_date.register(PeriodType)
def _(x, unit = "S"):
    return x.asfreq(unit, how = "start")


@floor_date.register(Series)
def _(x, *args, **kwargs):
    # dispatch to either period or datetime version
    f = _get_series_dispatcher(floor_date, x)
    return f(x.dt, *args, **kwargs)


# Ceil date -------------------------------------------------------------------
 
@ceil_date.register(DatetimeType)
def _(x, unit = "S"):
    cls_offset = _get_offset(unit)

    if issubclass(cls_offset, offsets.Tick):
        return x.ceil(unit)

    offset = cls_offset(n = 0)
    # the 0 ensures it will not rollforward an anchor point
    return x.normalize() + offset


@ceil_date.register(PeriodType)
def _(x, unit = "S"):
    raise NotImplementedError(
            "It is not possible to use ceil_date on a Period. "
            "Try converting to a DatetimeIndex."
            )


@ceil_date.register(Series)
def _(x, *args, **kwargs):
    f = _get_series_dispatcher(ceil_date, x)
    return f(x.dt, *args, **kwargs)


