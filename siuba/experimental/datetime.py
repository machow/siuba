from siuba.siu import symbolic_dispatch
from pandas.arrays import DatetimeArray, PeriodArray
from pandas import DatetimeIndex, PeriodIndex, Period, Timestamp, Series

from pandas import offsets
from pandas.core.dtypes.common import is_period_dtype, is_datetime64_any_dtype

from abc import ABC

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
    cls = offsets.prefix_mapping.get(unit)
    if cls is None:
        raise ValueError("unit {} not a valid offset".format(unit))

    return cls

def _get_series_dispatcher(f, x):
    if is_period_dtype(x):
        return f.registry[PeriodType]
    elif is_datetime64_any_dtype(x):
        return f.registry[DatetimeType]

    raise TypeError("does not seem to be a period or datetime")


# Floor date ------------------------------------------------------------------

@symbolic_dispatch
def floor_date(x, unit = "S"):
    raise TypeError("floor_date not implemented for class {}".format(type(x)))


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
 
@symbolic_dispatch
def ceil_date(x, unit = "S"):
    raise TypeError("ceil_date not implemented for class {}".format(type(x)))


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


