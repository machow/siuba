from siuba.siu import Symbolic, strip_symbolic
# TODO: dot, corr, cov
# ordered set aggregate. e.g. mode()
# hypothetical-set aggregate (e.g. rank(a) as if it were in partition(order_by b))

# kinds of windows:
#   * result len n_elements: rank()
#   * result len 1: is_monotonic (lag, diff, and any). ordered set aggregate.
#   * result len input len: percentile_cont([.1, .2]). hypo set aggregate.

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

class Wontdo(Result): pass
class Maydo(Result): pass
class Todo(Result): pass

CATEGORIES_TIME = {
        'time_series', 'datetime_properties', 'datetime_methods', 'period_properties',
        'timedelta_properties', 'timedelta_methods'
        }

CATEGORIES_STRING = {
        'string_methods'
        }


# TODO: test cases
#   * doesn't work in mutate (no_mutate = ['postgresql'])
#   * returns float rather than int (sql_type = 'float')
#   * requires boolean or float input (op = 'bool')
#   * won't be implemented (in some backend) (postgresql = 'not_impl')
#   * isn't implemented now, but will be (postgresql = 'xfail')
funcs = {
    ## ------------------------------------------------------------------------
    # Attributes 
    ## ------------------------------------------------------------------------
    '_special_methods': {
        '__invert__': _.__invert__()         >> Elwise(op = 'bool'),
        '__and__': _.__and__(_)              >> Elwise(op = 'bool'),
        '__or__': _.__or__(_)                >> Elwise(op = 'bool'),
        '__xor__': _.__xor__(_)              >> Elwise(op = 'bool', postgresql = 'xfail'),
        '__neg__': _.__neg__()               >> Elwise(),
        '__pos__': _.__pos__()               >> Elwise(postgresql = 'xfail'),
        '__rand__': _.__rand__(_)            >> Elwise(op = 'bool'),
        '__ror__': _.__ror__(_)              >> Elwise(op = 'bool'),
        '__rxor__': _.__rxor__(_)            >> Elwise(op = 'bool', postgresql = 'xfail'),
        # copied from binary section below
        '__add__': _.__add__(_)               >> Elwise(),
        '__sub__': _.__sub__(_)               >> Elwise(),
        '__truediv__': _.__truediv__(_)       >> Elwise(postgresql = 'xfail'),  # TODO: pg needs cast int to float?
        '__floordiv__': _.__floordiv__(_)     >> Elwise(postgresql = 'xfail'),
        '__mul__': _.__mul__(_)               >> Elwise(),
        '__mod__': _.__mod__(_)               >> Elwise(),
        '__pow__': _.__pow__(_)               >> Elwise(postgresql = 'xfail'),
        '__lt__': _.__lt__(_)                 >> Elwise(),
        '__gt__': _.__gt__(_)                 >> Elwise(),
        '__le__': _.__le__(_)                 >> Elwise(),
        '__ge__': _.__ge__(_)                 >> Elwise(), 
        '__ne__': _.__ne__(_)                 >> Elwise(), 
        '__eq__': _.__eq__(_)                 >> Elwise(), 
        '__div__': _.__div__(_)               >> Elwise(postgresql = 'xfail'),  # TODO: deprecated in python3, not in siu
        '__round__': _.__round__(2)           >> Elwise(postgresql = 'xfail'),  # TODO: pg returns float
        '__radd__': _.__radd__(_)             >> Elwise(), 
        '__rsub__': _.__rsub__(_)             >> Elwise(), 
        '__rmul__': _.__rmul__(_)             >> Elwise(), 
        '__rdiv__': _.__rdiv__(_)             >> Elwise(postgresql = 'xfail'), 
        '__rtruediv__': _.__rtruediv__(_)     >> Elwise(postgresql = 'xfail'),
        '__rfloordiv__': _.__rfloordiv__(_)   >> Elwise(postgresql = 'xfail'),
        '__rmod__': _.__rmod__(_)             >> Elwise(),
        '__rpow__': _.__rpow__(_)             >> Elwise(postgresql = 'xfail'),
        },
    'attributes': {
        'index': _.index               >> Wontdo(),
        'array': _.array               >> Wontdo(),
        'values': _.value              >> Wontdo(),
        'dtype': _.dtype               >> Todo(),
        'ftype': _.ftype               >> Wontdo(),
        'shape': _.shape               >> Wontdo(),
        'nbytes': _.nbytes             >> Todo(),
        'ndim': _.ndim                 >> Todo(),
        'size': _.size                 >> Maydo(),                              # TODO: good to support, is a method on grouped series...'
        'strides': _.strides           >> Wontdo(),
        'itemsize': _.itemsize         >> Wontdo(),
        'base': _.base                 >> Wontdo(),
        'T': _.T                       >> Wontdo(),
        'transpose': _.transpose()     >> Wontdo(),
        'memory_usage': _.memory_usage >> Todo(),
        'hasnans': _.hasnans           >> Todo(),
        'flags': _.flags               >> Wontdo(),
        'empty': _.empty               >> Todo(),
        'dtypes': _.dtypes             >> Todo(),
        'ftypes': _.ftypes             >> Wontdo(),
        'data': _.data                 >> Wontdo(),
        'is_copy': _.is_copy           >> Todo(),
        'name': _.name                 >> Todo(),
        'put': _.put                   >> Wontdo(),
        'axes': _.axes >> Wontdo(),
        'attrs': _.attrs >> Wontdo(),
        },
    ## ------------------------------------------------------------------------
    # Conversion 
    ## ------------------------------------------------------------------------
    'conversion': {
        'astype': _.astype('str')          >> Elwise(),
        'convert_dtypes': _.convert_dtypes() >> Todo(), # >> Elwise(),
        'infer_objects': _.infer_objects() >> Todo(),
        'copy': _.copy()                   >> Elwise(postgresql = 'not_impl'),
        'bool': _.bool                     >> Todo(),
        'to_numpy': _.to_numpy()           >> Wontdo(),
        'to_period': _.to_period()         >> Todo(),
        'to_timestamp': _.to_timestamp()   >> Todo(),
        'to_list': _.to_list()             >> Wontdo(),
        'tolist': _.tolist()               >> Wontdo(),
        'get_values': _.get_values()       >> Wontdo(),
        '__array__': _.__array__()         >> Wontdo(),
        },
    ## ------------------------------------------------------------------------
    # Indexing, iteration 
    ## ------------------------------------------------------------------------
    'indexing': {
        'get': _.get(1)            >> Todo(),
        'at': _.at[1]              >> Todo(),
        'iat': _.iat[1]            >> Todo(),
        'loc': _.loc[1]            >> Todo(),
        'iloc': _.iloc[1]          >> Todo(),
        '__iter__': _.__iter__()   >> Maydo(),
        'items': _.items()         >> Wontdo(),
        'iteritems': _.iteritems() >> Wontdo(),
        'keys': _.keys()           >> Wontdo(),
        'pop': _.pop()             >> Wontdo(),
        'item': _.item()           >> Wontdo(),
        'xs': _.xs()               >> Wontdo(),
        },
    ## ------------------------------------------------------------------------
    # Binary operator functions 
    ## ------------------------------------------------------------------------
    'binary': {
        'add': _.add(_)               >> Elwise(),
        'sub': _.sub(_)               >> Elwise(),
        'subtract': _.subtract(_)     >> Elwise(postgresql = 'not_impl'),
        'truediv': _.truediv(_)       >> Elwise(postgresql = 'xfail'),
        'floordiv': _.floordiv(_)     >> Elwise(postgresql = 'xfail'),
        'mul': _.mul(_)               >> Elwise(),
        'multiply': _.multiply(_)     >> Elwise(postgresql = 'not_impl'),
        'mod': _.mod(_)               >> Elwise(),
        'pow': _.pow(_)               >> Elwise(postgresql = 'xfail'),
        'lt': _.lt(_)                 >> Elwise(),
        'gt': _.gt(_)                 >> Elwise(),
        'le': _.le(_)                 >> Elwise(),
        'ge': _.ge(_)                 >> Elwise(), 
        'ne': _.ne(_)                 >> Elwise(), 
        'eq': _.eq(_)                 >> Elwise(), 
        'div': _.div(_)               >> Elwise(postgresql = 'xfail'), 
        'divide': _.divide(_)         >> Todo(), # >> Elwise(),
        'divmod': _.divmod(_)         >> Todo(),
        'round': _.round(2)           >> Elwise(postgresql = 'xfail'), 
        'radd': _.radd(_)             >> Elwise(), 
        'rsub': _.rsub(_)             >> Elwise(), 
        'rmul': _.rmul(_)             >> Elwise(), 
        'rdiv': _.rdiv(_)             >> Elwise(postgresql = 'xfail'), 
        'rdivmod': _.rdivmod(_)       >> Todo(),
        'rtruediv': _.rtruediv(_)     >> Elwise(postgresql = 'xfail'),
        'rfloordiv': _.rfloordiv(_)   >> Elwise(postgresql = 'xfail'),
        'rmod': _.rmod(_)             >> Elwise(),
        'rpow': _.rpow(_)             >> Elwise(postgresql = 'xfail'),
        'combine': _.combine(_, "max")  >> Todo(),            # NOTE: need to migrate to yml spec first
        'combine_first': _.combine_first(_, "max")  >> Todo(),
        'product': _.product()        >> Todo(),# >> Agg(),   # TODO: doesn't exist on GroupedDataFrame
        'dot': _.dot(_)              >> Todo(), # >> Agg(),
        },
    ## ------------------------------------------------------------------------
    # Function application, groupby & window 
    ## ------------------------------------------------------------------------
    'function_application': {
        'apply': _.apply         >> Wontdo(),
        'agg': _.agg             >> Wontdo(),
        'aggregate': _.aggregate >> Wontdo(),
        'transform': _.transform >> Wontdo(),
        'map': _.map             >> Wontdo(),
        'groupby': _.groupby     >> Wontdo(),
        'rolling': _.rolling     >> Wontdo(),
        'expanding': _.expanding >> Wontdo(),
        'ewm': _.ewm             >> Wontdo(),
        'pipe': _.pipe           >> Wontdo(),
        },
    ## ------------------------------------------------------------------------
    ## Computations / descriptive stats
    ## ------------------------------------------------------------------------
    'computations': {
        'abs': _.abs()                >> Elwise(),
        'all': _.all()                >> Agg(op = 'bool'),
        'any': _.any()                >> Agg(op = 'bool'),
        'autocorr': _.autocorr()      >> Todo(), # TODO: missing on GDF
        'between': _.between(2, 5)    >> Elwise(),
        'clip': _.clip(2, 5)          >> Elwise(),
        # clip_lower                                # TODO: deprecated
        # clip_upper                                # TODO: deprecated
        'corr': _.corr(_)             >> Todo(),#>> Agg(),
        'count': _.count()            >> Agg(),
        'cov': _.cov(_)               >> Todo(), #>> Agg(),
        'cummax': _.cummax()          >> Window(postgresql = 'xfail'),
        'cummin': _.cummin()          >> Window(postgresql = 'xfail'),
        'cumprod': _.cumprod()        >> Window(postgresql = 'xfail'),
        'cumsum': _.cumsum()          >> Window(postgresql = 'xfail'),
        'describe': _.describe()      >> Wontdo(),
        'diff': _.diff()              >> Window(),
        'factorize': _.factorize()    >> Maydo(),
        'kurt': _.kurt()              >> Todo(), # >> Agg(),  # TODO: doesn't exist on GDF
        'mad': _.mad()                >> Agg(postgresql = 'xfail'),
        'max': _.max()                >> Agg(),
        'mean': _.mean()              >> Agg(),
        'median': _.median()          >> Agg(postgresql = 'xfail'),
        'min': _.min()                >> Agg(),
        'mode': _.mode()              >> Maydo(), #Agg(),   # TODO: doesn't exist on GDF, can return > 1 result
        'nlargest': _.nlargest()      >> Maydo(), #Window(),
        'nsmallest': _.nsmallest()    >> Maydo(), #Window(),
        'pct_change': _.pct_change()  >> Window(postgresql = 'xfail'),
        'prod': _.prod()              >> Agg(postgresql = 'xfail'),
        'quantile': _.quantile(.75)      >> Agg(no_mutate = ['postgresql']),
        'rank': _.rank()              >> Window(postgresql = 'xfail'),
        'sem': _.sem()                >> Agg(postgresql = 'xfail'),
        'skew': _.skew()              >> Agg(postgresql = 'xfail'),
        'std': _.std()                >> Agg(),
        'sum': _.sum()                >> Agg(postgresql = 'xfail'), # TODO: pg returns float
        'var': _.var()                >> Agg(),
        'kurtosis': _.kurtosis()      >> Todo(), #Agg(),  # TODO: doesn't exist on GDF
        'unique': _.unique()          >> Wontdo(),
        'nunique': _.nunique()        >> Agg(no_mutate = ['postgresql']),
        'is_unique': _.is_unique      >> Todo(), #Agg(),  # TODO: all is_... properties not on GDF
        'is_monotonic': _.is_monotonic >> Todo(), # Agg(),
        'is_monotonic_increasing': _.is_monotonic_increasing >> Todo(), # Agg(),
        'is_monotonic_decreasing': _.is_monotonic_decreasing >> Todo(), # Agg(),
        'value_counts': _.value_counts() >> Wontdo(),
        'compound': _.compound() >> Maydo(),
        },
    ## ------------------------------------------------------------------------
    # Reindexing / selection / label manipulation 
    ## ------------------------------------------------------------------------
    'reindexing': {
        'align': _.align                       >> Wontdo(),
        'drop': _.drop                         >> Wontdo(),
        'droplevel': _.droplevel               >> Wontdo(),
        'drop_duplicates': _.drop_duplicates() >> Todo(),
        'duplicated': _.duplicated()           >> Todo(),
        'equals': _.equals(_)                  >> Todo(),
        'first': _.first()                     >> Maydo(), #Window
        'head': _.head()                       >> Todo(),
        'idxmax': _.idxmax()                   >> Todo(),
        'idxmin': _.idxmin()                   >> Todo(),
        'isin': _.isin(tuple([1,2]))           >> Elwise(),
        'last': _.last()                       >> Wontdo(),
        'reindex': _.reindex                   >> Wontdo(),
        'reindex_like': _.reindex_like         >> Wontdo(),
        'rename': _.rename('new_name')         >> Todo(),
        'rename_axis': _.rename_axis           >> Wontdo(),
        'reset_index': _.reset_index()         >> Wontdo(),
        'sample': _.sample()                   >> Todo(),
        'set_axis': _.set_axis                 >> Wontdo(),
        'take': _.take                         >> Wontdo(),
        'tail': _.tail()                       >> Todo(),
        'truncate': _.truncate                 >> Wontdo(),
        'where': _.where(_)                    >> Todo(),
        'mask': _.mask()                       >> Todo(),
        'add_prefix': _.add_prefix()           >> Wontdo(),
        'add_suffix': _.add_suffix()           >> Wontdo(),
        'filter': _.filter()                   >> Wontdo(),
        },
    ## ------------------------------------------------------------------------
    # Missing data handling 
    ## ------------------------------------------------------------------------
    'missing_data': {
        'isna':  _.isna()              >> Elwise(),
        'isnull':  _.isnull()          >> Elwise(postgresql = "not_impl"),
        'notna': _.notna()             >> Elwise(),
        'notnull': _.notnull()         >> Elwise(postgresql = "not_impl"),
        'dropna': _.dropna()           >> Maydo(),
        'fillna': _.fillna(1)          >> Elwise(),
        'ffill': _.ffill(1)            >> Todo(),
        'bfill': _.bfill(1)            >> Todo(),
        'interpolate': _.interpolate() >> Maydo(),
        },
    ## ------------------------------------------------------------------------
    # Reshaping, sorting 
    ## ------------------------------------------------------------------------
    'reshaping': {
        'argsort': _.argsort               >> Wontdo(),
        'argmin': _.argmin                 >> Wontdo(),
        'argmax': _.argmax                 >> Wontdo(),
        'reorder_levels': _.reorder_levels >> Wontdo(),
        'sort_values': _.sort_values()     >> Todo(),
        'sort_index': _.sort_index()       >> Wontdo(),
        'swaplevel': _.swaplevel           >> Wontdo(),
        'swapaxes': _.swapaxes            >> Wontdo(),
        'unstack': _.unstack               >> Wontdo(),
        'explode': _.explode()             >> Maydo(),
        'searchsorted': _.searchsorted()   >> Todo(),
        'ravel': _.ravel()                 >> Wontdo(),
        'repeat': _.repeat()               >> Maydo(),
        'squeeze': _.squeeze()             >> Wontdo(),
        'view': _.view                     >> Wontdo(),
        },
    ## ------------------------------------------------------------------------
    # Combining / joining / merging 
    ## ------------------------------------------------------------------------
    'combining': {
        'append': _.append()   >> Todo(),
        'replace': _.replace() >> Todo(),
        'update': _.update()   >> Wontdo(),
        },
    ## ------------------------------------------------------------------------
    # Time series-related 
    ## ------------------------------------------------------------------------
    'time_series': {
            'asfreq': _.asfreq("D")                    >> Todo(),
            'asof': _.asof()                           >> Todo(),
            'shift': _.shift()                         >> Todo(),
            'first_valid_index': _.first_valid_index() >> Todo(),
            'last_valid_index': _.last_valid_index()   >> Todo(),
            'resample': _.resample()                   >> Todo(),
            'tz_convert': _.tz_convert()               >> Todo(),
            'tz_localize': _.tz_localize()             >> Todo(),
            'at_time': _.at_time()                     >> Todo(),
            'between_time': _.between_time()           >> Maydo(),
            'tshift': _.tshift()                       >> Todo(),
            'slice_shift': _.slice_shift()             >> Maydo(),
        },
    ## ------------------------------------------------------------------------
    # Datetime properties 
    ## ------------------------------------------------------------------------
    'datetime_properties': {
        'dt.date': _.dt.date                         >> Elwise(postgresql = 'not_impl'), # TODO: all 3, not pandas objects
        'dt.time': _.dt.time                             >> Elwise(postgresql = 'not_impl'),
        'dt.timetz': _.dt.timetz                         >> Elwise(postgresql = 'not_impl'),
        'dt.year': _.dt.year                             >> Elwise(sql_type = 'float'),
        'dt.month': _.dt.month                           >> Elwise(sql_type = 'float'),
        'dt.day': _.dt.day                               >> Elwise(sql_type = 'float'),
        'dt.hour': _.dt.hour                             >> Elwise(sql_type = 'float'),
        'dt.minute': _.dt.minute                         >> Elwise(sql_type = 'float'),
        'dt.second': _.dt.second                         >> Elwise(sql_type = 'float'),
        'dt.microsecond': _.dt.microsecond               >> Elwise(sql_type = 'float', postgresql = 'xfail'),
        'dt.nanosecond': _.dt.nanosecond                 >> Elwise(postgresql = 'not_impl'),
        'dt.week': _.dt.week                             >> Elwise(sql_type = 'float'),
        'dt.weekofyear': _.dt.weekofyear                 >> Elwise(sql_type = 'float'),
        'dt.dayofweek': _.dt.dayofweek                   >> Elwise(sql_type = 'float'),
        'dt.weekday': _.dt.weekday                       >> Elwise(sql_type = 'float'),
        'dt.dayofyear': _.dt.dayofyear                   >> Elwise(sql_type = 'float'),
        'dt.quarter': _.dt.quarter                       >> Elwise(sql_type = 'float'),
        'dt.is_month_start': _.dt.is_month_start         >> Elwise(),
        'dt.is_month_end': _.dt.is_month_end             >> Elwise(),
        'dt.is_quarter_start': _.dt.is_quarter_start     >> Elwise(),
        'dt.is_quarter_end': _.dt.is_quarter_end         >> Elwise(postgresql = 'xfail'),
        'dt.is_year_start': _.dt.is_year_start           >> Elwise(),
        'dt.is_year_end': _.dt.is_year_end               >> Elwise(),
        'dt.is_leap_year': _.dt.is_leap_year             >> Elwise(postgresql = 'not_impl'),
        'dt.daysinmonth': _.dt.daysinmonth               >> Elwise(sql_type = 'float'),
        'dt.days_in_month': _.dt.days_in_month           >> Elwise(sql_type = 'float'),
        'dt.tz': _.dt.tz                                 >> Singleton(),
        'dt.freq': _.dt.freq                             >> Singleton(),
        },
    ## ------------------------------------------------------------------------
    # Datetime methods 
    ## ------------------------------------------------------------------------
    'datetime_methods': {
        'dt.to_period': _.dt.to_period('D')       >> Elwise(postgresql = 'xfail'),
        'dt.to_pydatetime': _.dt.to_pydatetime()     >> Todo(),            # TODO: datetime objects converted back to numpy?
        'dt.tz_localize': _.dt.tz_localize('UTC') >> Elwise(postgresql = 'xfail'),
        'dt.tz_convert': _.dt.tz_convert("MST")   >> Todo(),            # TODO: need custom test
        'dt.normalize': _.dt.normalize()          >> Elwise(postgresql = 'xfail'),
        'dt.strftime': _.dt.strftime('%d')        >> Elwise(postgresql = 'xfail'),
        'dt.round': _.dt.round('D')               >> Elwise(postgresql = 'xfail'),
        'dt.floor': _.dt.floor('D')               >> Elwise(postgresql = 'xfail'),
        'dt.ceil': _.dt.ceil('D')                 >> Elwise(postgresql = 'xfail'),
        'dt.month_name': _.dt.month_name()        >> Elwise(postgresql = 'xfail'),
        'dt.day_name': _.dt.day_name()            >> Elwise(postgresql = 'xfail'),
        },
    ## ------------------------------------------------------------------------
    # Period properties 
    ## ------------------------------------------------------------------------
    'period_properties': {
        'dt.asfreq': _.dt.asfreq("D")          >> Todo(),
        'dt.qyear': _.dt.qyear                 >> Todo(),
        'dt.start_time': _.dt.start_time       >> Todo(),
        'dt.end_time': _.dt.end_time           >> Todo(),
        'dt.to_timestamp': _.dt.to_timestamp() >> Todo(),
        },
    ## ------------------------------------------------------------------------
    # Timedelta properties 
    ## ------------------------------------------------------------------------
    'timedelta_properties': {
        'dt.days': _.dt.days                 >> Todo(),
        'dt.seconds': _.dt.seconds           >> Todo(),
        'dt.microseconds': _.dt.microseconds >> Todo(),
        'dt.nanoseconds': _.dt.nanoseconds   >> Todo(),
        'dt.components': _.dt.components     >> Maydo(),
        },
    ## ------------------------------------------------------------------------
    # Timedelta methods 
    ## ------------------------------------------------------------------------
    'timedelta_methods': {
        'dt.to_pytimedelta': _.dt.to_pytimedelta >> Todo(),
        'dt.total_seconds': _.dt.total_seconds >> Todo(),
        },
    ## ------------------------------------------------------------------------
    ## String methods
    ## ------------------------------------------------------------------------
    'string_methods': {
        'str.capitalize': _.str.capitalize()              >> Elwise(),
        'str.casefold': _.str.casefold()                  >> Todo(), #Elwise(),   #TODO: introduced in v0.25.1
        'str.cat': _.str.cat(_)                           >> Maydo(),                   #TODO: can be Agg OR Elwise, others arg
        'str.center': _.str.center(3)                     >> Elwise(postgresql = 'not_impl'),
        'str.contains': _.str.contains('a')               >> Elwise(),
        'str.count': _.str.count('a')                     >> Elwise(postgresql = 'xfail'),
        'str.decode': _.str.decode()                      >> Todo(),                  # TODO custom testing
        'str.encode': _.str.encode('utf-8')               >> Elwise(postgresql = 'xfail'),
        'str.endswith': _.str.endswith('a|b')             >> Elwise(postgresql = 'xfail'),
        'str.extract': _.str.extract('(a)(b)')            >> Maydo(),    # TODO: returns DataFrame
        'str.extractall': _.str.extractall('a')           >> Maydo(),
        'str.find': _.str.find('a|c')                     >> Elwise(postgresql = 'xfail'),
        'str.findall': _.str.findall('a|c')               >> Elwise(postgresql = 'xfail'),
        'str.get': _.str.split('a').get(0)                >> Todo(),       # TODO: custom test
        'str.index': _.str.index('a')                     >> Todo(),      # TODO: custom test
        'str.join': _.str.split('a').str.join(',')        >> Todo(),      # TODO: custom test
        'str.len': _.str.len()                            >> Elwise(),
        'str.ljust': _.str.ljust(5)                       >> Elwise(postgresql = 'xfail'), # pg formatstr function
        'str.lower': _.str.lower()                        >> Elwise(),
        'str.lstrip': _.str.lstrip()                      >> Elwise(),
        'str.match': _.str.match('a|c')                   >> Elwise(postgresql = 'xfail'),
        'str.normalize': _.str.normalize()                >> Todo(),
        'str.pad': _.str.pad(5)                           >> Elwise(postgresql = 'xfail'),
        'str.partition': _.str.partition()                >> Maydo(),
        'str.repeat': _.str.repeat(3)                     >> Todo(),
        'str.replace': _.str.replace('a|b', 'c')          >> Elwise(postgresql = 'xfail'),
        'str.rfind': _.str.rfind('a')                     >> Elwise(postgresql = 'xfail'),
        'str.rindex': _.str.rindex('a')                   >> Todo(),
        'str.rjust': _.str.rjust(5)                       >> Elwise(postgresql = 'xfail'),
        'str.rpartition': _.str.rpartition()              >> Todo(),
        'str.rstrip': _.str.rstrip()                      >> Elwise(),
        'str.slice': _.str.slice(step = 2)                >> Elwise(postgresql = 'xfail'),
        'str.slice_replace': _.str.slice_replace(2, repl = 'x')   >> Elwise(postgresql = 'xfail'),
        'str.split': _.str.split('a|b')                   >> Elwise(postgresql = 'xfail'),
        'str.rsplit': _.str.rsplit('a|b', n = 1)          >> Elwise(postgresql = 'xfail'),
        'str.startswith': _.str.startswith('a|b')         >> Elwise(),
        'str.strip': _.str.strip()                        >> Elwise(),
        'str.swapcase': _.str.swapcase()                  >> Elwise(postgresql = 'xfail'),
        'str.title': _.str.title()                        >> Elwise(),
        'str.translate': _.str.translate()                >> Todo(),
        'str.upper': _.str.upper()                        >> Elwise(),
        'str.wrap': _.str.wrap(2)                         >> Elwise(postgresql = 'xfail'),
        'str.zfill': _.str.zfill(2)                       >> Todo(),
        'str.isalnum': _.str.isalnum()                    >> Elwise(postgresql = 'xfail'),
        'str.isalpha': _.str.isalpha()                    >> Elwise(postgresql = 'xfail'),
        'str.isdigit': _.str.isdigit()                    >> Elwise(postgresql = 'xfail'),
        'str.isspace': _.str.isspace()                    >> Elwise(postgresql = 'xfail'),
        'str.islower': _.str.islower()                    >> Elwise(postgresql = 'xfail'),
        'str.isupper': _.str.isupper()                    >> Elwise(postgresql = 'xfail'),
        'str.istitle': _.str.istitle()                    >> Elwise(postgresql = 'xfail'),
        'str.isnumeric': _.str.isnumeric()                >> Elwise(postgresql = 'xfail'),
        'str.isdecimal': _.str.isdecimal()                >> Elwise(postgresql = 'xfail'),
        'str.get_dummies': _.str.get_dummies()            >> Todo(),
        },
    'categories': {
            'cat.categories': _.cat.categories                               >> Wontdo(),
            'cat.ordered': _.cat.ordered                                     >> Todo(),
            'cat.codes': _.cat.codes                                         >> Wontdo(),
            'cat.rename_categories': _.cat.rename_categories()               >> Todo(),
            'cat.reorder_categories': _.cat.reorder_categories()             >> Todo(),
            'cat.add_categories': _.cat.add_categories()                     >> Todo(),
            'cat.remove_categories': _.cat.remove_categories()               >> Todo(),
            'cat.remove_unused_categories': _.cat.remove_unused_categories() >> Todo(),
            'cat.set_categories': _.cat.set_categories()                     >> Todo(),
            'cat.as_ordered': _.cat.as_ordered()                             >> Todo(),
            'cat.as_unordered': _.cat.as_unordered()                         >> Todo(),
        },
    'sparse': {
        'sparse.npoints': _.sparse.npoints       >> Maydo(), #Agg()
        'sparse.density': _.sparse.density       >> Maydo(), #Agg()
        'sparse.fill_value': _.sparse.fill_value >> Maydo(), #Agg()
        'sparse.sp_values': _.sparse.sp_values   >> Maydo(),
        'sparse.from_coo': _.sparse.from_coo()   >> Wontdo(),
        'sparse.to_coo': _.sparse.to_coo()       >> Wontdo(),
        'sparse.to_dense': _.sparse.to_dense()   >> Wontdo(),
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
    'io': {
        'to_pickle': _.to_pickle()       >> Wontdo(),
        'to_csv': _.to_csv()             >> Wontdo(),
        'to_dict': _.to_dict()           >> Wontdo(),
        'to_excel': _.to_excel()         >> Wontdo(),
        'to_frame': _.to_frame()         >> Wontdo(),
        'to_xarray': _.to_xarray()       >> Maydo(),
        'to_hdf': _.to_hdf()             >> Wontdo(),
        'to_sql': _.to_sql()             >> Wontdo(),
        'to_msgpack': _.to_msgpack()     >> Wontdo(),
        'to_json': _.to_json()           >> Todo(),
        'to_dense': _.to_dense()         >> Wontdo(),
        'to_string': _.to_string()       >> Todo(),
        'to_markdown': _.to_markdown()   >> Todo(),
        'to_clipboard': _.to_clipboard() >> Wontdo(),
        'to_latex': _.to_latex()         >> Wontdo(),
        }
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
        d[name]['category'] = category


spec = dict(itertools.chain(*iter(d.items() for d in nested_spec.values())))

if __name__ == "__main__":
    from siuba.spec.utils import dump_spec
    import sys
    from pathlib import Path
    path_spec = Path(sys.argv[0]).parent / 'series.yml'
    
    with open(str(path_spec), 'w') as f:
        dump_spec(nested_spec, f)
