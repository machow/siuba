"""
Base dialect for translating column operations to SQL.

A dialect requires three pieces:

    1. Classes representing column data under normal and aggregate settings.
       (e.g. SqlColumn, SqlColumnAgg).
    2. Functions for creating the sqlalchemy clause corresponding to an 
       operation.
       (e.g. a function for taking the standard dev of a column).
    3. A module-level variable named translator. This should be a class with
       a translate method, that takes a column expression and returns a 
       sqlalchemy clause. Ideally using translate.SqlTranslator.
       (e.g. SqlTranslator.from_mapping(...))

"""

# NOTE: this file should be an example that could live outside siuba, so
# we (1) use full import paths, (2) define everything a new backend would need
# here.
from sqlalchemy import sql
from sqlalchemy import types as sa_types
from sqlalchemy.sql import func as fn
from sqlalchemy.sql.elements import ColumnClause

from siuba.sql.translate import (
        win_absent,
        win_over,
        win_cumul,
        win_agg,
        set_agg,
        sql_agg,
        sql_scalar,
        sql_colmeth,
        sql_not_impl,
        annotate,
        RankOver,
        CumlOver,
        SqlTranslator
        )


# =============================================================================
# Column data classes 
# =============================================================================

from siuba.sql.translate import SqlColumn, SqlColumnAgg

# =============================================================================
# Custom translations
# =============================================================================

# Computation -----------------------------------------------------------------

def sql_func_diff(col, periods = 1):
    if periods > 0:
        return CumlOver(col - sql.func.lag(col, periods))
    elif periods < 0:
        return CumlOver(col - sql.func.lead(col, abs(periods)))

    raise ValueError("periods argument to sql diff cannot be 0")

def sql_func_floordiv(x, y):
    return sql.cast(x / y, sa_types.Integer())

def sql_func_rank(col):
    # see https://stackoverflow.com/a/36823637/1144523
    min_rank = RankOver(sql.func.rank(), order_by = col)
    to_mean = (RankOver(sql.func.count(), partition_by = col) - 1) / 2.0

    return min_rank + to_mean


# Datetime -------------------------------------------------------------------

def sql_extract(name):
    return lambda col: sql.func.extract(name, col)

def sql_func_extract_dow_monday(col):
    # make monday = 0 rather than sunday
    monday0 = sql.cast(sql.func.extract('dow', col) + 6, sa_types.Integer) % 7
    # cast to numeric, since that's what extract('dow') returns
    return sql.cast(monday0, sa_types.Numeric)

def sql_is_first_of(name, reference):
    return lambda col: fn.date_trunc(name, col) == fn.date_trunc(reference, col)

def sql_func_last_day_in_period(col, period):
    return fn.date_trunc(period, col) + sql.text("interval '1 %s - 1 day'" % period)

def sql_func_days_in_month(col):
    return fn.extract('day', sql_func_last_day_in_period(col, 'month'))

def sql_is_last_day_of(period):
    def f(col):
        last_day = sql_func_last_day_in_period(col, period)
        return fn.date_trunc('day', col) == last_day

    return f

def sql_func_floor_date(col, unit):
    # see https://www.postgresql.org/docs/9.1/functions-datetime.html#FUNCTIONS-DATETIME-TRUNC
    # valid values: 
    #   microseconds, milliseconds, second, minute, hour,
    #   day, week, month, quarter, year, decade, century, millennium
    # TODO: implement in siuba.dply.lubridate
    return fn.date_trunc(unit, col)


# Strings ---------------------------------------------------------------------

def sql_str_strip(name):
    
    strip_func = getattr(fn, name)
    def f(col, to_strip = " \t\n\v\f\r"):
        return strip_func(col, to_strip)

    return f

def sql_func_capitalize(col):
    first_char = fn.upper(fn.left(col, 1)) 
    rest = fn.right(col, fn.length(col) - 1)
    return sql.functions.concat(first_char, rest)


# Misc implementations --------------------------------------------------------

def sql_func_astype(col, _type):
    mappings = {
            str: sa_types.Text,
            'str': sa_types.Text,
            int: sa_types.Integer,
            'int': sa_types.Integer,
            float: sa_types.Numeric,
            'float': sa_types.Numeric,
            bool: sa_types.Boolean,
            'bool': sa_types.Boolean
            }
    try:
        sa_type = mappings[_type]
    except KeyError:
        raise ValueError("sql astype currently only supports type objects: str, int, float, bool")
    return sql.cast(col, sa_type)


# Annotations -----------------------------------------------------------------

def req_bool(f):
    return annotate(f, input_type = "bool")


# =============================================================================
# Base translation mappings
# =============================================================================

base_scalar = dict(
    # infix ----
    __add__       = sql_colmeth("__add__"),
    __and__       = req_bool(sql_colmeth("__and__")),
    __div__       = sql_colmeth("__div__"),
    __eq__        = sql_colmeth("__eq__"),
    __floordiv__  = sql_func_floordiv,
    __ge__        = sql_colmeth("__ge__"),
    __gt__        = sql_colmeth("__gt__"),
    __invert__    = req_bool(sql_colmeth("__invert__")),
    __le__        = sql_colmeth("__le__"),
    __lt__        = sql_colmeth("__lt__"),
    __mod__       = sql_colmeth("__mod__"),
    __mul__       = sql_colmeth("__mul__"),
    __ne__        = sql_colmeth("__ne__"),
    __neg__       = sql_colmeth("__neg__"),
    __or__        = req_bool(sql_colmeth("__or__")),
    __pos__       = sql_not_impl(),
    __pow__       = sql_not_impl(),
    __radd__      = sql_colmeth("__radd__"),
    __rand__      = req_bool(sql_colmeth("__rand__")),
    __rdiv__      = sql_colmeth("__rdiv__"),
    __rfloordiv__ = lambda x, y: sql_func_floordiv(y, x),
    __rmod__      = sql_colmeth("__rmod__"),
    __rmul__      = sql_colmeth("__rmul__"),
    __ror__       = req_bool(sql_colmeth("__ror__")),
    __round__     = sql_scalar("round"),
    __rpow__      = sql_not_impl(),
    __rsub__      = sql_colmeth("__rsub__"),
    __rtruediv__  = sql_colmeth("__rtruediv__"),
    #__rxor__      = sql_colmeth("__rxor__"),
    __sub__       = sql_colmeth("__sub__"),
    __truediv__   = sql_colmeth("__truediv__"),
    #__xor__       = sql_colmeth("__xor__"), 


    # infix methods ----

    add           = sql_colmeth("__add__"),        
    #and          =
    div           = sql_colmeth("__div__"),        
    divide        = sql_colmeth("__div__"),        
    #divmod       = 
    eq            = sql_colmeth("__eq__"),         
    #floordiv     = sql_colmeth("__floordiv__"),         
    ge            = sql_colmeth("__ge__"),     
    gt            = sql_colmeth("__gt__"),         
    le            = sql_colmeth("__le__"),         
    lt            = sql_colmeth("__lt__"),        
    mod           = sql_colmeth("__mod__"),        
    mul           = sql_colmeth("__mul__"),         
    multiply      = sql_colmeth("__mul__"),        
    ne            = sql_colmeth("__ne__"),         
    pow           = sql_not_impl(),
    radd          = sql_colmeth("__radd__"),       
    rdiv          = sql_colmeth("__rdiv__"),       
    #rdivmod      = 
    #rfloordiv    = sql_colmeth("__pow__"),        
    rmod          = sql_colmeth("__rmod__"),       
    rmul          = sql_colmeth("__rmul__"),       
    round         = sql_scalar("round"),
    rpow          = sql_not_impl(),     
    rsub          = sql_colmeth("__rsub__"),       
    rtruediv      = sql_not_impl(),
    sub           = sql_colmeth("__sub__"),   
    subtract      = sql_colmeth("__sub__"),      
    truediv       = sql_colmeth("__truediv__"),        


    # computation ---- 

    abs                     = sql_scalar("abs"),
    between                 = sql_colmeth("between"),
    clip                    = lambda col, low, upp: fn.least(fn.greatest(col, low), upp),
    isin                    = sql_colmeth("in_"),


    # strings ----


    **{
      # TODO: check generality of trim functions, since MYSQL overrides
      "str.capitalize"    : sql_func_capitalize,
      #"str.center"        :,
      #"str.contains"      :,
      #"str.count"         :,
      #"str.encode"        :,
      "str.endswith"      : sql_colmeth("endswith"),
      #"str.find"          :,
      #"str.findall"       :,
      #"str.isalnum"       :,
      #"str.isalpha"       :,
      #"str.isdecimal"     :,
      #"str.isdigit"       :,
      "str.islower"       : lambda col: col == sql.func.lower(col),
      #"str.isnumeric"     :,
      #"str.isspace"       :,
      #"str.istitle"       :,
      #"str.isupper"       :,
      "str.len"           : lambda col: sql.func.length(col),
      #"str.ljust"         :,
      "str.lower"         : lambda col: sql.func.lower(col),
      "str.lstrip"        : sql_str_strip("ltrim"),
      #"str.match"         :,
      #"str.pad"           :,
      #"str.replace"       :,
      #"str.rfind"         :,
      #"str.rjust"         :,
      #"str.rsplit"        :,
      "str.rstrip"        : sql_str_strip("rtrim"),
      #"str.slice"         :,
      #"str.slice_replace" :,
      #"str.split"         :,
      "str.startswith"    : sql_colmeth("startswith"),
      "str.strip"         : sql_str_strip("trim"),
      #"str.swapcase"      :,
      "str.title"         : lambda col: sql.func.initcap(col),
      "str.upper"         : lambda col: sql.func.upper(col),
      #"str.wrap"          :,
    },


    # datetime ----


    **{
      #"dt.ceil"             :
      #"dt.date"             :
      "dt.day"              : sql_extract("day"),
      #"dt.day_name"         :
      "dt.dayofweek"        : sql_func_extract_dow_monday,
      "dt.dayofyear"        : sql_extract("doy"),
      #"dt.days"             :
      "dt.days_in_month"    : sql_func_days_in_month,
      "dt.daysinmonth"      : sql_func_days_in_month,
      #"dt.floor"            :
      "dt.hour"             : sql_extract("hour"),
      #"dt.is_leap_year"     :
      "dt.is_month_end"     : sql_is_last_day_of("month"),
      "dt.is_month_start"   : sql_is_first_of("day", "month"),
      #"dt.is_quarter_end"   :
      "dt.is_quarter_start" : sql_is_first_of("day", "quarter"),
      "dt.is_year_end"      : sql_is_last_day_of("year"),
      "dt.is_year_start"    : sql_is_first_of("day", "year"),
      #"dt.microsecond"      :
      #"dt.microseconds"     :
      "dt.minute"           : sql_extract("minute"),
      "dt.month"            : sql_extract("month"),
      #"dt.month_name"       :
      #"dt.nanosecond"       :
      #"dt.nanoseconds"      :
      #"dt.normalize"        :
      "dt.quarter"          : sql_extract("quarter"),
      #"dt.qyear"            :
      #dt.round            =
      "dt.second"           : sql_extract("second"),
      #"dt.seconds"          :
      #"dt.strftime"         :
      #"dt.time"             :
      #"dt.timetz"           :
      #"dt.to_period"        :
      #"dt.to_pydatetime"    :
      #"dt.to_pytimedelta"   :
      #"dt.to_timestamp"     :
      #"dt.total_seconds"    :
      #dt.tz_convert       =
      #dt.tz_localize      =
      "dt.week"             : sql_extract("week"),
      "dt.weekday"          : sql_func_extract_dow_monday,
      "dt.weekofyear"       : sql_extract("week"),
      "dt.year"             : sql_extract("year"),
      #"dt.freq" : 
      #"dt.tz"   :
      },


    # datetime methods not on accessor ----

    #asfreq            = 
    #between_time      = TODO


    # Missing values ----

    fillna      = lambda x, y: sql.functions.coalesce(x,y),
    isna        = sql_colmeth("is_", None),
    isnull      = sql_colmeth("is_", None),
    notna       = lambda col: ~col.is_(None),
    notnull     = lambda col: ~col.is_(None),

    # Misc ---
    #replace       =  # TODO
    astype = sql_func_astype,
    #where
)


base_win = dict(

    # computation ----
    #autocorr                = 
    cummax                  = win_cumul("max"),
    cummin                  = win_cumul("min"),
    #cumprod                 = 
    cumsum                  = annotate(win_cumul("sum"), result_type = "float"),
    diff                    = sql_func_diff,
    #is_monotonic            = 
    #is_monotonic_decreasing = 
    #is_monotonic_increasing = 
    #pct_change              = TODO(?)
    rank                    = sql_func_rank,

    # computation (strict aggregates)
    #all = #TODO(pg): all = sql_aggregate("BOOL_AND", "all")
    #any = #TODO(pg): any = sql_aggregate("BOOL_OR", "any"),
    #corr = # TODO(pg)
    count = win_agg("count"),
    #cov = 
    #is_unique = # TODO(low)
    #kurt = 
    #kurtosis = 
    #mad = 
    max = win_agg("max"),
    mean = win_agg("avg"),
    #median = 
    min = win_agg("min"),
    nunique = win_absent("nunique"),
    #prod = 
    #product = 
    quantile =  win_absent("quantile"),
    #sem = 
    #skew = 
    #std =  # TODO(pg)
    sum = annotate(win_agg("sum"), result_type = "float"),
    #var = # TODO(pg)



    # datetime not on accessor ----
    #asof              =
    #shift             =
    #tshift            =


    # missing values ----

    #bfill       = TODO: pg
    #ffill       = TODO: pg

    # index (strict aggregates) ----

    #equals =  #TODO(pg) combine == and all
    #idxmax =  #TODO?
    #idxmin = 


    # attributes (strict aggregates) ----

    #empty = 
    #hasnans = # TODO
    #memory_usage = 
    #nbytes = 
    size = win_absent("count"),
)


# =============================================================================
# Aggregate functions
# =============================================================================

base_agg = dict(
    # infix methods ----

    #dot = sql_not_impl(),

    # computation ----

    #all = #TODO(pg): all = sql_aggregate("BOOL_AND", "all")
    #any = #TODO(pg): any = sql_aggregate("BOOL_OR", "any"),
    #corr = # TODO(pg)
    count = lambda col: sql.func.count(),
    #cov = 
    #is_unique = # TODO(low)
    #kurt = 
    #kurtosis = 
    #mad = 
    max = sql_agg("max"),
    mean = sql_agg("avg"),
    #median = 
    min = sql_agg("min"),
    nunique = lambda col: sql.func.count(sql.func.distinct(col)),
    #prod = 
    #product = 
    quantile = set_agg("percentile_cont"), # TODO: flag no_mutate
    #sem = 
    #skew = 
    #std =  # TODO(pg)
    sum = annotate(sql_agg("sum"), result_type = "float"),
    #var = # TODO(pg)

    # index ----

    #equals =  #TODO(pg) combine == and all
    #idxmax =  #TODO?
    #idxmin = 


    # attributes ----

    #empty = 
    #hasnans = # TODO
    #memory_usage = 
    #nbytes = 
    size = sql_agg("count"),
)


# =============================================================================
# Base with no windows implemented (for e.g. sqlite)
# =============================================================================

# based on https://github.com/tidyverse/dbplyr/blob/master/R/backend-.R
base_nowin = dict(
        #row_number   = win_absent("ROW_NUMBER"),
        #min_rank     = win_absent("RANK"),
        rank         = win_absent("RANK"),
        dense_rank   = win_absent("DENSE_RANK"),
        percent_rank = win_absent("PERCENT_RANK"),
        cume_dist    = win_absent("CUME_DIST"),
        ntile        = win_absent("NTILE"),
        mean         = win_absent("AVG"),
        sd           = win_absent("SD"),
        var          = win_absent("VAR"),
        cov          = win_absent("COV"),
        cor          = win_absent("COR"),
        sum          = win_absent("SUM"),
        min          = win_absent("MIN"),
        max          = win_absent("MAX"),
        median       = win_absent("PERCENTILE_CONT"),
        quantile    = win_absent("PERCENTILE_CONT"),
        n            = win_absent("N"),
        n_distinct   = win_absent("N_DISTINCT"),
        nunique      = win_absent("nunique function"),
        cummean      = win_absent("MEAN"),
        cumsum       = win_absent("SUM"),
        cummin       = win_absent("MIN"),
        cummax       = win_absent("MAX"),
        nth          = win_absent("NTH_VALUE"),
        first        = win_absent("FIRST_VALUE"),
        last         = win_absent("LAST_VALUE"),
        lead         = win_absent("LEAD"),
        lag          = win_absent("LAG"),
        order_by     = win_absent("ORDER_BY"),
        str_flatten  = win_absent("STR_FLATTEN"),
        count        = win_absent("COUNT"),
        size         = win_absent("size function"),
        )


funcs = dict(scalar = base_scalar, aggregate = base_agg, window = base_win)

translator = SqlTranslator.from_mappings(
        base_scalar, base_win, base_agg,
        SqlColumn, SqlColumnAgg
        )
