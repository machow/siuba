from siuba.siu import Symbolic, strip_symbolic
# TODO: dot, corr, cov

_ = Symbolic()

class Result:
    def __init__(self, **kwargs):
        self.options = kwargs

    def to_dict(self):
        return {'type': self.__class__.__name__, **self.options}

class Elwise(Result): pass
class Agg(Result): pass 
class Window(Result): pass
class Singleton(Result): pass
class WontImplement(Result): pass


CATEGORIES_TIME = {
        'time_series', 'datetime_properties', 'datetime_methods', 'period_properties',
        'timedelta_properties', 'timedelta_methods'
        }

CATEGORIES_STRING = {
        'string_methods'
        }


funcs = {
    ## ------------------------------------------------------------------------
    # Attributes 
    ## ------------------------------------------------------------------------
    '_special_methods': {
        '__invert__': _.__invert__()         >> Elwise(op = 'bool'),
        '__and__': _.__and__(_)              >> Elwise(op = 'bool'),
        '__or__': _.__or__(_)                >> Elwise(op = 'bool'),
        '__xor__': _.__xor__(_)              >> Elwise(op = 'bool'),
        '__neg__': _.__neg__()               >> Elwise(),
        '__pos__': _.__pos__()               >> Elwise(),
        '__rand__': _.__rand__(_)            >> Elwise(op = 'bool'),
        '__ror__': _.__ror__(_)              >> Elwise(op = 'bool'),
        '__rxor__': _.__rxor__(_)            >> Elwise(op = 'bool'),
        # copied from binary section below
        '__add__': _.__add__(_)               >> Elwise(),
        '__sub__': _.__sub__(_)               >> Elwise(),
        '__truediv__': _.__truediv__(_)       >> Elwise(),
        '__floordiv__': _.__floordiv__(_)     >> Elwise(),
        '__mul__': _.__mul__(_)               >> Elwise(),
        '__mod__': _.__mod__(_)               >> Elwise(),
        '__pow__': _.__pow__(_)               >> Elwise(),
        '__lt__': _.__lt__(_)                 >> Elwise(),
        '__gt__': _.__gt__(_)                 >> Elwise(),
        '__le__': _.__le__(_)                 >> Elwise(),
        '__ge__': _.__ge__(_)                 >> Elwise(), 
        '__ne__': _.__ne__(_)                 >> Elwise(), 
        '__eq__': _.__eq__(_)                 >> Elwise(), 
        '__div__': _.__div__(_)               >> Elwise(), 
        '__round__': _.__round__(2)           >> Elwise(), 
        '__radd__': _.__radd__(_)             >> Elwise(), 
        '__rsub__': _.__rsub__(_)             >> Elwise(), 
        '__rmul__': _.__rmul__(_)             >> Elwise(), 
        '__rdiv__': _.__rdiv__(_)             >> Elwise(), 
        '__rtruediv__': _.__rtruediv__(_)     >> Elwise(),
        '__rfloordiv__': _.__rfloordiv__(_)   >> Elwise(),
        '__rmod__': _.__rmod__(_)             >> Elwise(),
        '__rpow__': _.__rpow__(_)             >> Elwise(),
        },
    'attributes': {
        # method
        # index
        # array
        # values
        # dtype
        # ftype
        # shape
        # nbytes
        # ndim
        # size
        # strides
        # itemsize
        # base
        # T
        # memory_usage
        # hasnans
        # flags
        # empty
        # dtypes
        # ftypes
        # data
        # is_copy
        # name
        # put
        },
    ## ------------------------------------------------------------------------
    # Conversion 
    ## ------------------------------------------------------------------------
    'conversion': {
        #'astype': _.astype('str')       >> Elwise(),
        # infer_objects
        'copy': _.copy()              >> Elwise(),
        # bool
        # to_numpy
        # to_period
        # to_timestamp
        # to_list
        # get_values
        # __array__
        },
    ## ------------------------------------------------------------------------
    # Indexing, iteration 
    ## ------------------------------------------------------------------------
    'indexing': {
        # get
        # at
        # iat
        # loc
        # iloc
        # __iter__
        # items
        # iteritems
        # keys
        # pop
        # item
        # xs
        },
    ## ------------------------------------------------------------------------
    # Binary operator functions 
    ## ------------------------------------------------------------------------
    'binary': {
        'add': _.add(_)               >> Elwise(),
        'sub': _.sub(_)               >> Elwise(),
        'truediv': _.truediv(_)       >> Elwise(),
        'floordiv': _.floordiv(_)     >> Elwise(),
        'mul': _.mul(_)               >> Elwise(),
        'mod': _.mod(_)               >> Elwise(),
        'pow': _.pow(_)               >> Elwise(),
        'lt': _.lt(_)                 >> Elwise(),
        'gt': _.gt(_)                 >> Elwise(),
        'le': _.le(_)                 >> Elwise(),
        'ge': _.ge(_)                 >> Elwise(), 
        'ne': _.ne(_)                 >> Elwise(), 
        'eq': _.eq(_)                 >> Elwise(), 
        'div': _.div(_)               >> Elwise(), 
        'round': _.round(2)           >> Elwise(), 
        'radd': _.radd(_)             >> Elwise(), 
        'rsub': _.rsub(_)             >> Elwise(), 
        'rmul': _.rmul(_)             >> Elwise(), 
        'rdiv': _.rdiv(_)             >> Elwise(), 
        'rtruediv': _.rtruediv(_)     >> Elwise(),
        'rfloordiv': _.rfloordiv(_)   >> Elwise(),
        'rmod': _.rmod(_)             >> Elwise(),
        'rpow': _.rpow(_)             >> Elwise(),
        # combine
        # combine_first
        #'product': _.product()        >> Agg(),   # TODO: doesn't exist on GroupedDataFrame
        #'dot': _.dot(_)               >> Agg(),
        },
    ## ------------------------------------------------------------------------
    # Function application, groupby & window 
    ## ------------------------------------------------------------------------
    'function_application': {
        # apply
        # agg
        # aggregate
        # transform
        # map
        # groupby
        # rolling
        # expanding
        # ewm
        # pipe
        },
    ## ------------------------------------------------------------------------
    ## Computations / descriptive stats
    ## ------------------------------------------------------------------------
    'computations': {
        'abs': _.abs()                >> Elwise(),
        'all': _.all()                >> Agg(),
        'any': _.any()                >> Agg(),
        'autocorr': _.autocorr()      >> Window(),
        'between': _.between(2, 5)    >> Elwise(),
        'clip': _.clip(2, 5)          >> Elwise(),
        # clip_lower                                # TODO: deprecated
        # clip_upper                                # TODO: deprecated
        #'corr': _.corr(_)             >> Agg(),
        'count': _.count()            >> Agg(),
        #'cov': _.cov(_)               >> Agg(),
        'cummax': _.cummax()          >> Window(),
        'cummin': _.cummin()          >> Window(),
        'cumprod': _.cumprod()        >> Window(),
        'cumsum': _.cumsum()          >> Window(),
        # describe
        'diff': _.diff()              >> Window(),
        # factorize
        # 'kurt': _.kurt()              >> Agg(),  # TODO: doesn't exist on GDF
        'mad': _.mad()                >> Agg(),
        'max': _.max()                >> Agg(),
        'mean': _.mean()              >> Agg(),
        'median': _.median()          >> Agg(),
        'min': _.min()                >> Agg(),
        #'mode': _.mode()              >> Agg(),   # TODO: doesn't exist on GDF, can return > 1 result
        #'nlargest': _.nlargest()      >> Window(),
        #'nsmallest': _.nsmallest()    >> Window(),
        'pct_change': _.pct_change()  >> Window(),
        'prod': _.prod()              >> Agg(),
        'quantile': _.quantile()      >> Agg(),
        'rank': _.rank()              >> Window(),
        'sem': _.sem()                >> Agg(),
        'skew': _.skew()              >> Agg(),
        'std': _.std()                >> Agg(),
        'sum': _.sum()                >> Agg(),
        'var': _.var()                >> Agg(),
        #'kurtosis': _.kurtosis()      >> Agg(),  # TODO: doesn't exist on GDF
        # unique
        'nunique': _.nunique()        >> Agg(),
        #'is_unique': _.is_unique      >> Agg(),  # TODO: all is_... properties not on GDF
        #'is_monotonic': _.is_monotonic >> Agg(),
        #'is_monotonic_increasing': _.is_monotonic_increasing >> Agg(),
        #'is_monotonic_decreasing': _.is_monotonic_decreasing >> Agg(),
        # value_counts
        # compound
        },
    ## ------------------------------------------------------------------------
    # Reindexing / selection / label manipulation 
    ## ------------------------------------------------------------------------
    'reindexing': {
        # align
        # drop
        # droplevel
        # drop_duplicates
        # duplicated
        # equals
        #'first': _.first()            >> Window(),
        # head
        # idxmax
        # idxmin
        'isin': _.isin(tuple([1,2]))  >> Elwise(),
        #'last': _.last()              >> Window(),
        # reindex
        # reindex_like
        # rename
        # rename_axis
        # reset_index
        # sample
        # set_axis
        # take
        # tail
        # truncate
        # where
        # mask
        # add_prefix
        # add_suffix
        # filter
        },
    ## ------------------------------------------------------------------------
    # Missing data handling 
    ## ------------------------------------------------------------------------
    'missing_data': {
        'isna':  _.isna()             >> Elwise(),
        'notna': _.notna()            >> Elwise(),
        # dropna
        'fillna': _.fillna(1)         >> Elwise(),
        # interpolate
        },
    ## ------------------------------------------------------------------------
    # Reshaping, sorting 
    ## ------------------------------------------------------------------------
    'reshaping': {
        # argsort
        # argmin
        # argmax
        # reorder_levels
        # sort_values
        # sort_index
        # swaplevel
        # unstack
        # explode
        # searchsorted
        # ravel
        # repeat
        # squeeze
        # view
        },
    ## ------------------------------------------------------------------------
    # Combining / joining / merging 
    ## ------------------------------------------------------------------------
    'combining': {
        # append
        # replace
        # update
        },
    ## ------------------------------------------------------------------------
    # Time series-related 
    ## ------------------------------------------------------------------------
    'time_series': {
        # asfreq
        # asof
        # shift
        # first_valid_index
        # last_valid_index
        # resample
        # tz_convert
        # tz_localize
        # at_time
        # between_time
        # tshift
        # slice_shift
        },
    ## ------------------------------------------------------------------------
    # Datetime properties 
    ## ------------------------------------------------------------------------
    'datetime_properties': {
        'dt.date': _.dt.date                             >> Elwise(),
        'dt.time': _.dt.time                             >> Elwise(),
        'dt.timetz': _.dt.timetz                         >> Elwise(),
        'dt.year': _.dt.year                             >> Elwise(),
        'dt.month': _.dt.month                           >> Elwise(),
        'dt.day': _.dt.day                               >> Elwise(),
        'dt.hour': _.dt.hour                             >> Elwise(),
        'dt.minute': _.dt.minute                         >> Elwise(),
        'dt.second': _.dt.second                         >> Elwise(),
        'dt.microsecond': _.dt.microsecond               >> Elwise(),
        'dt.nanosecond': _.dt.nanosecond                 >> Elwise(),
        'dt.week': _.dt.week                             >> Elwise(),
        'dt.weekofyear': _.dt.weekofyear                 >> Elwise(),
        'dt.dayofweek': _.dt.dayofweek                   >> Elwise(),
        'dt.weekday': _.dt.weekday                       >> Elwise(),
        'dt.dayofyear': _.dt.dayofyear                   >> Elwise(),
        'dt.quarter': _.dt.quarter                       >> Elwise(),
        'dt.is_month_start': _.dt.is_month_start         >> Elwise(),
        'dt.is_month_end': _.dt.is_month_end             >> Elwise(),
        'dt.is_quarter_start': _.dt.is_quarter_start     >> Elwise(),
        'dt.is_quarter_end': _.dt.is_quarter_end         >> Elwise(),
        'dt.is_year_start': _.dt.is_year_start           >> Elwise(),
        'dt.is_year_end': _.dt.is_year_end               >> Elwise(),
        'dt.is_leap_year': _.dt.is_leap_year             >> Elwise(),
        'dt.daysinmonth': _.dt.daysinmonth               >> Elwise(),
        'dt.days_in_month': _.dt.days_in_month           >> Elwise(),
        'dt.tz': _.dt.tz                                 >> Singleton(),
        'dt.freq': _.dt.freq                             >> Singleton(),
        },
    ## ------------------------------------------------------------------------
    # Datetime methods 
    ## ------------------------------------------------------------------------
    'datetime_methods': {
        'dt.to_period': _.dt.to_period('D')             >> Elwise(),
        # dt.to_pydatetime                                              # TODO: datetime objects converted back to numpy?
        'dt.tz_localize': _.dt.tz_localize('UTC')       >> Elwise(),
        # dt.tz_convert                                                 # TODO: need custom test
        'dt.normalize': _.dt.normalize()                >> Elwise(),
        'dt.strftime': _.dt.strftime('%d')              >> Elwise(),
        'dt.round': _.dt.round('D')                     >> Elwise(),
        'dt.floor': _.dt.floor('D')                     >> Elwise(),
        'dt.ceil': _.dt.ceil('D')                       >> Elwise(),
        'dt.month_name': _.dt.month_name()              >> Elwise(),
        'dt.day_name': _.dt.day_name()                  >> Elwise(),
        },
    ## ------------------------------------------------------------------------
    # Period properties 
    ## ------------------------------------------------------------------------
    'period_properties': {
        # dt.qyear
        # dt.start_time
        # dt.end_time
        },
    ## ------------------------------------------------------------------------
    # Timedelta properties 
    ## ------------------------------------------------------------------------
    'timedelta_properties': {
        # dt.days
        # dt.seconds
        # dt.microseconds
        # dt.nanoseconds
        # dt.components
        },
    ## ------------------------------------------------------------------------
    # Timedelta methods 
    ## ------------------------------------------------------------------------
    'timedelta_methods': {
        # dt.to_pytimedelta
        # dt.total_seconds
        },
    ## ------------------------------------------------------------------------
    ## String methods
    ## ------------------------------------------------------------------------
    'string_methods': {
        'str.capitalize': _.str.capitalize()              >> Elwise(),
        #'str.casefold': _.str.casefold()                  >> Elwise(),   #TODO: introduced in v0.25.1
        # str.cat                                                         #TODO: can be Agg OR Elwise, others arg
        'str.center': _.str.center(3)                     >> Elwise(),
        'str.contains': _.str.contains('a')               >> Elwise(),
        'str.count': _.str.count('a')                     >> Elwise(),
        # str.decode                                                      # TODO custom testing
        'str.encode': _.str.encode('utf-8')               >> Elwise(),
        'str.endswith': _.str.endswith('a|b')             >> Elwise(),
        #'str.extract': _.str.extract('(a)(b)')                           # TODO: returns DataFrame
        # str.extractall
        'str.find': _.str.find('a|c')                     >> Elwise(),
        'str.findall': _.str.findall('a|c')               >> Elwise(),
        # str.get                                                         # TODO: custom test
        # str.index                                                       # TODO: custom test
        # str.join                                                        # TODO: custom test
        'str.len': _.str.len()                            >> Elwise(),
        'str.ljust': _.str.ljust(5)                       >> Elwise(),
        'str.lower': _.str.lower()                        >> Elwise(),
        'str.lstrip': _.str.lstrip()                      >> Elwise(),
        'str.match': _.str.match('a|c')                   >> Elwise(),
        # str.normalize
        'str.pad': _.str.pad(5)                           >> Elwise(),
        # str.partition
        # str.repeat
        'str.replace': _.str.replace('a|b', 'c')          >> Elwise(),
        'str.rfind': _.str.rfind('a')                     >> Elwise(),
        # str.rindex
        'str.rjust': _.str.rjust(5)                       >> Elwise(),
        # str.rpartition
        'str.rstrip': _.str.rstrip()                      >> Elwise(),
        'str.slice': _.str.slice(step = 2)                >> Elwise(),
        'str.slice_replace': _.str.slice_replace(2, repl = 'x')   >> Elwise(),
        'str.split': _.str.split('a|b')                   >> Elwise(),
        'str.rsplit': _.str.rsplit('a|b', n = 1)          >> Elwise(),
        'str.startswith': _.str.startswith('a|b')         >> Elwise(),
        'str.strip': _.str.strip()                        >> Elwise(),
        'str.swapcase': _.str.swapcase()                  >> Elwise(),
        'str.title': _.str.title()                        >> Elwise(),
        # str.translate
        'str.upper': _.str.upper()                        >> Elwise(),
        'str.wrap': _.str.wrap(2)                         >> Elwise(),
        # str.zfill
        'str.isalnum': _.str.isalnum()                    >> Elwise(),
        'str.isalpha': _.str.isalpha()                    >> Elwise(),
        'str.isdigit': _.str.isdigit()                    >> Elwise(),
        'str.isspace': _.str.isspace()                    >> Elwise(),
        'str.islower': _.str.islower()                    >> Elwise(),
        'str.isupper': _.str.isupper()                    >> Elwise(),
        'str.istitle': _.str.istitle()                    >> Elwise(),
        'str.isnumeric': _.str.isnumeric()                >> Elwise(),
        'str.isdecimal': _.str.isdecimal()                >> Elwise(),
        # str.get_dummies
        },
    'categories': {
        # cat.categories
        # cat.ordered
        # cat.codes
        # cat.rename_categories
        # cat.reorder_categories
        # cat.add_categories
        # cat.remove_categories
        # cat.remove_unused_categories
        # cat.set_categories
        # cat.as_ordered
        # cat.as_unordered
        },
    'sparse': {
        # sparse
        # sparse.npoints
        # sparse.density
        # sparse.fill_value
        # sparse.sp_values
        # sparse.from_coo
        # sparse.to_coo
        },
    ## ------------------------------------------------------------------------
    # Plotting 
    ## ------------------------------------------------------------------------
    # plot
    # plot.area
    # plot.bar
    # plot.barh
    # plot.box
    # plot.density
    # plot.hist
    # plot.kde
    # plot.line
    # plot.pie
    # hist
    ## ------------------------------------------------------------------------
    # Serialization / IO / conversion 
    ## ------------------------------------------------------------------------
    # to_pickle
    # to_csv
    # to_dict
    # to_excel
    # to_frame
    # to_xarray
    # to_hdf
    # to_sql
    # to_msgpack
    # to_json
    # to_dense
    # to_string
    # to_clipboard
    # to_latex
    }

from siuba.spec.utils import get_type_info
import itertools

funcs_stripped = { 
        section_name: { k: strip_symbolic(v) for k,v in section.items()}
          for section_name, section in funcs.items()
          } 


all_funcs = dict(itertools.chain(*[x.items() for x in funcs_stripped.values()]))


# Get spec =====
nested_spec = {}
for category, call_dict in funcs_stripped.items():
    nested_spec[category] = d = {}
    for name, call in call_dict.items():
        d[name] = get_type_info(call)


spec = dict(itertools.chain(*iter(d.items() for d in nested_spec.values())))

if __name__ == "__main__":
    from siuba.spec.utils import dump_spec
    import sys
    from pathlib import Path
    path_spec = Path(sys.argv[0]).parent / 'series.yml'
    
    with open(str(path_spec), 'w') as f:
        dump_spec(nested_spec, f)
