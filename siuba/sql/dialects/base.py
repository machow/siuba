# NOTE: this file should be an example that could live outside siuba, so
# we (1) use full import paths, (2) define everything a new backend would need
# here.
from sqlalchemy import sql
from sqlalchemy.sql import func as fn

from siuba import ops
from siuba.ops.utils import register

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
        create_sql_translators
        )

# TODO: move anything using this into base.py
from siuba.sql.translate import SqlColumn, SqlColumnAgg

from siuba.sql.translate import (
        sql_extract, sql_func_days_in_month, sql_func_extract_dow_monday,
        sql_is_first_of, sql_is_last_day_of
        )
# TODO: sql_func_floor_date

from siuba.sql.translate import (
        sql_func_capitalize,
        sql_str_strip
        )

from siuba.sql.translate import (
        sql_func_diff
        )

from siuba.sql.translate import sql_func_astype

# =============================================================================
# Elementwise and window functions
# =============================================================================
# TODO:
#   * how to cast?
#       abs = sql_scalar("abs"),
#       acos = sql_scalar("acos"),
#       asin = sql_scalar("asin"),
#       atan = sql_scalar("atan"),
#       atan2 = sql_scalar("atan2"),
#       cos = sql_scalar("cos"),
#       cot = sql_scalar("cot"),
# 

base_scalar = dict(
    # infix ----
    __add__       = sql_colmeth("__add__"),
    __and__       = sql_colmeth("__and__"),
    __div__       = sql_colmeth("__div__"),
    __eq__        = sql_colmeth("__eq__"),
    __floordiv__  = sql_not_impl(),
    __ge__        = sql_colmeth("__ge__"),
    __gt__        = sql_colmeth("__gt__"),
    __invert__    = sql_colmeth("__invert__"),
    __le__        = sql_colmeth("__le__"),
    __lt__        = sql_colmeth("__lt__"),
    __mod__       = sql_colmeth("__mod__"),
    __mul__       = sql_colmeth("__mul__"),
    __ne__        = sql_colmeth("__ne__"),
    __neg__       = sql_colmeth("__neg__"),
    __or__        = sql_colmeth("__or__"),
    __pos__       = sql_colmeth("__pos__"),
    __pow__       = sql_colmeth("__pow__"),
    __radd__      = sql_colmeth("__radd__"),
    __rand__      = sql_colmeth("__rand__"),
    __rdiv__      = sql_colmeth("__pos__"),
    __rfloordiv__ = sql_colmeth("__pow__"),
    __rmod__      = sql_colmeth("__rmod__"),
    __rmul__      = sql_colmeth("__rmul__"),
    __ror__       = sql_colmeth("__ror__"),   
    __round__     = sql_scalar("round"),
    __rpow__      = sql_colmeth("__rpow__"),
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
    pow           = sql_colmeth("__pow__"),        
    radd          = sql_colmeth("__radd__"),       
    rdiv          = sql_colmeth("__rdiv__"),       
    #rdivmod      = 
    #rfloordiv    = sql_colmeth("__pow__"),        
    rmod          = sql_colmeth("__rmod__"),       
    rmul          = sql_colmeth("__rmul__"),       
    round         = sql_scalar("round"),
    rpow          = sql_colmeth("__rpow__"),     
    rsub          = sql_colmeth("__rsub__"),       
    rtruediv      = sql_colmeth("__rtruediv__"),       
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
      "str.islower"       : sql.func.lower,
      #"str.isnumeric"     :,
      #"str.isspace"       :,
      #"str.istitle"       :,
      #"str.isupper"       :,
      "str.len"           : sql.func.length,
      #"str.ljust"         :,
      "str.lower"         : sql.func.lower,
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
      "str.title"         : sql.func.initcap,
      "str.upper"         : sql.func.upper,
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

    fillna      = sql.functions.coalesce,
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
    cumsum                  = win_cumul("sum"),
    diff                    = sql_func_diff,
    #is_monotonic            = 
    #is_monotonic_decreasing = 
    #is_monotonic_increasing = 
    #pct_change              = TODO(?)
    rank                    = win_over("rank"),

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
    nunique = lambda col: sql.func.count(sql.func.distinct(col)),
    #prod = 
    #product = 
    quantile =  NotImplementedError(),
    #sem = 
    #skew = 
    #std =  # TODO(pg)
    sum = win_agg("sum"),
    #var = # TODO(pg)



    # datetime not on accessor ----
    #asof              =
    #shift             =
    #tshift            =


    # missing values ----

    #bfill       = TODO: pg
    #ffill       = TODO: pg
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
    sum = sql_agg("sum"),
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
        count        = win_absent("COUNT")
        )


funcs = dict(scalar = base_scalar, aggregate = base_agg, window = base_win)

translator = create_sql_translators(
        base_scalar, base_agg, base_win,
        SqlColumn, SqlColumnAgg
        )
