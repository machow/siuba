from .utils import operation, Namespace

# Infix operators -------------------------------------------------------------

ops_infix = Namespace(
    __add__ = operation('__add__', 'elwise', 2),
    __and__ = operation('__and__', 'elwise', 2),
    __div__ = operation('__div__', 'elwise', 2),
    __eq__ = operation('__eq__', 'elwise', 2),
    __floordiv__ = operation('__floordiv__', 'elwise', 2),
    __ge__ = operation('__ge__', 'elwise', 2),
    __gt__ = operation('__gt__', 'elwise', 2),
    __invert__ = operation('__invert__', 'elwise', 1),
    __le__ = operation('__le__', 'elwise', 2),
    __lt__ = operation('__lt__', 'elwise', 2),
    __mod__ = operation('__mod__', 'elwise', 2),
    __mul__ = operation('__mul__', 'elwise', 2),
    __ne__ = operation('__ne__', 'elwise', 2),
    __neg__ = operation('__neg__', 'elwise', 1),
    __or__ = operation('__or__', 'elwise', 2),
    __pos__ = operation('__pos__', 'elwise', 1),
    __pow__ = operation('__pow__', 'elwise', 2),
    __radd__ = operation('__radd__', 'elwise', 2),
    __rand__ = operation('__rand__', 'elwise', 2),
    __rdiv__ = operation('__rdiv__', 'elwise', 2),
    __rfloordiv__ = operation('__rfloordiv__', 'elwise', 2),
    __rmod__ = operation('__rmod__', 'elwise', 2),
    __rmul__ = operation('__rmul__', 'elwise', 2),
    __ror__ = operation('__ror__', 'elwise', 2),
    __round__ = operation('__round__', 'elwise', 1),
    __rpow__ = operation('__rpow__', 'elwise', 2),
    __rsub__ = operation('__rsub__', 'elwise', 2),
    __rtruediv__ = operation('__rtruediv__', 'elwise', 2),
    __rxor__ = operation('__rxor__', 'elwise', 2),
    __sub__ = operation('__sub__', 'elwise', 2),
    __truediv__ = operation('__truediv__', 'elwise', 2),
    __xor__ = operation('__xor__', 'elwise', 2),
    )

ops_infix_methods = Namespace(
    add           = operation('add', 'elwise', 2),
    div           = operation('div', 'elwise', 2),
    divide        = operation('divide', 'elwise', 2),
    divmod        = operation('divmod', None, 2),
    dot           = operation('dot', 'agg', 2),
    eq            = operation('eq', 'elwise', 2),
    floordiv      = operation('floordiv', 'elwise', 2),
    ge            = operation('ge', 'elwise', 2),
    gt            = operation('gt', 'elwise', 2),
    le            = operation('le', 'elwise', 2),
    lt            = operation('lt', 'elwise', 2),
    mod           = operation('mod', 'elwise', 2),
    mul           = operation('mul', 'elwise', 2),
    multiply      = operation('multiply', 'elwise', 2),
    ne            = operation('ne', 'elwise', 2),
    pow           = operation('pow', 'elwise', 2),
    radd          = operation('radd', 'elwise', 2),
    rdiv          = operation('rdiv', 'elwise', 2),
    rdivmod       = operation('rdivmod', None, 2),
    rfloordiv     = operation('rfloordiv', 'elwise', 2),
    rmod          = operation('rmod', 'elwise', 2),
    rmul          = operation('rmul', 'elwise', 2),
    round         = operation('round', 'elwise', 1),
    rpow          = operation('rpow', 'elwise', 2),
    rsub          = operation('rsub', 'elwise', 2),
    rtruediv      = operation('rtruediv', 'elwise', 2),
    sub           = operation('sub', 'elwise', 2),
    subtract      = operation('subtract', 'elwise', 2),
    truediv       = operation('truediv', 'elwise', 2),
    )


# Computation ---,

ops_compute = Namespace(
    abs     = operation('abs', 'elwise', 1),
    between = operation('between', 'elwise', 1),
    clip    = operation('clip', 'elwise', 1),
    isin = operation('isin', 'elwise', 1),

    all       = operation('all', 'agg', 1),
    any       = operation('any', 'agg', 1),
    corr      = operation('corr', 'agg', 2),
    count     = operation('count', 'agg', 1),
    cov       = operation('cov', 'agg', 2),
    is_unique = operation('is_unique', 'agg', 1, True, None),
    kurt      = operation('kurt', 'agg', 1),
    kurtosis  = operation('kurtosis', 'agg', 1),
    mad       = operation('mad', 'agg', 1),
    max       = operation('max', 'agg', 1),
    mean      = operation('mean', 'agg', 1),
    median    = operation('median', 'agg', 1),
    min       = operation('min', 'agg', 1),
    nunique   = operation('nunique', 'agg', 1),
    prod      = operation('prod', 'agg', 1),
    product   = operation('product', 'agg', 1),
    quantile  = operation('quantile', 'agg', 1),
    sem       = operation('sem', 'agg', 1),
    skew      = operation('skew', 'agg', 1),
    std       = operation('std', 'agg', 1),
    sum       = operation('sum', 'agg', 1),
    var       = operation('var', 'agg', 1),

    autocorr     = operation('autocorr', 'window', 1),
    cummax       = operation('cummax', 'window', 1),
    cummin       = operation('cummin', 'window', 1),
    cumprod      = operation('cumprod', 'window', 1),
    cumsum       = operation('cumsum', 'window', 1),
    diff         = operation('diff', 'window', 1),
    is_monotonic = operation('is_monotonic', 'window', 1, True, None),
    is_monotonic_decreasing = operation('is_monotonic_decreasing', 'window', 1, True, None),
    is_monotonic_increasing = operation('is_monotonic_increasing', 'window', 1, True, None),
    pct_change = operation('pct_change', 'window', 1),
    rank       = operation('rank', 'window', 1),

    compound     = operation('compound', None, 1),
    describe     = operation('describe', None, 1),
    factorize    = operation('factorize', None, 1),
    mode         = operation('mode', None, 1),
    nlargest     = operation('nlargest', None, 1),
    nsmallest    = operation('nsmallest', None, 1),
    unique       = operation('unique', None, 1),
    value_counts = operation('value_counts', None, 1),
    )


# Strings ---------------------------------------------------------------------

ops_str = Namespace(
    capitalize    = operation('capitalize', 'elwise', 1, False, 'str'),
    center        = operation('center', 'elwise', 1, False, 'str'),
    contains      = operation('contains', 'elwise', 1, False, 'str'),
    count         = operation('count', 'elwise', 1, False, 'str'),
    encode        = operation('encode', 'elwise', 1, False, 'str'),
    endswith      = operation('endswith', 'elwise', 1, False, 'str'),
    find          = operation('find', 'elwise', 1, False, 'str'),
    findall       = operation('findall', 'elwise', 1, False, 'str'),
    isalnum       = operation('isalnum', 'elwise', 1, False, 'str'),
    isalpha       = operation('isalpha', 'elwise', 1, False, 'str'),
    isdecimal     = operation('isdecimal', 'elwise', 1, False, 'str'),
    isdigit       = operation('isdigit', 'elwise', 1, False, 'str'),
    islower       = operation('islower', 'elwise', 1, False, 'str'),
    isnumeric     = operation('isnumeric', 'elwise', 1, False, 'str'),
    isspace       = operation('isspace', 'elwise', 1, False, 'str'),
    istitle       = operation('istitle', 'elwise', 1, False, 'str'),
    isupper       = operation('isupper', 'elwise', 1, False, 'str'),
    len           = operation('len', 'elwise', 1, False, 'str'),
    ljust         = operation('ljust', 'elwise', 1, False, 'str'),
    lower         = operation('lower', 'elwise', 1, False, 'str'),
    lstrip        = operation('lstrip', 'elwise', 1, False, 'str'),
    match         = operation('match', 'elwise', 1, False, 'str'),
    pad           = operation('pad', 'elwise', 1, False, 'str'),
    replace       = operation('replace', 'elwise', 1, False, 'str'),
    rfind         = operation('rfind', 'elwise', 1, False, 'str'),
    rjust         = operation('rjust', 'elwise', 1, False, 'str'),
    rsplit        = operation('rsplit', 'elwise', 1, False, 'str'),
    rstrip        = operation('rstrip', 'elwise', 1, False, 'str'),
    slice         = operation('slice', 'elwise', 1, False, 'str'),
    slice_replace = operation('slice_replace', 'elwise', 1, False, 'str'),
    split         = operation('split', 'elwise', 1, False, 'str'),
    startswith    = operation('startswith', 'elwise', 1, False, 'str'),
    strip         = operation('strip', 'elwise', 1, False, 'str'),
    swapcase      = operation('swapcase', 'elwise', 1, False, 'str'),
    title         = operation('title', 'elwise', 1, False, 'str'),
    upper         = operation('upper', 'elwise', 1, False, 'str'),
    wrap          = operation('wrap', 'elwise', 1, False, 'str'),

    casefold      = operation('casefold', None, 1, False, 'str'),
    cat           = operation('cat', None, 2, False, 'str'),
    decode        = operation('decode', None, 1, False, 'str'),
    extract       = operation('extract', None, 1, False, 'str'),
    extractall    = operation('extractall', None, 1, False, 'str'),
    get           = operation('get', None, 1, False, 'str'),
    get_dummies   = operation('get_dummies', None, 1, False, 'str'),
    index         = operation('index', None, 1, False, 'str'),
    join          = operation('join', None, 1, False, 'str'),
    normalize     = operation('normalize', None, 1, False, 'str'),
    partition     = operation('partition', None, 1, False, 'str'),
    repeat        = operation('repeat', None, 1, False, 'str'),
    rindex        = operation('rindex', None, 1, False, 'str'),
    rpartition    = operation('rpartition', None, 1, False, 'str'),
    translate     = operation('translate', None, 1, False, 'str'),
    zfill         = operation('zfill', None, 1, False, 'str'),
)


# Datetime --------------------------------------------------------------------

ops_dt = Namespace(
    ceil             = operation('ceil', 'elwise', 1, False, 'dt'),
    date             = operation('date', 'elwise', 1, True, 'dt'),
    day              = operation('day', 'elwise', 1, True, 'dt'),
    day_name         = operation('day_name', 'elwise', 1, False, 'dt'),
    dayofweek        = operation('dayofweek', 'elwise', 1, True, 'dt'),
    dayofyear        = operation('dayofyear', 'elwise', 1, True, 'dt'),
    days_in_month    = operation('days_in_month', 'elwise', 1, True, 'dt'),
    daysinmonth      = operation('daysinmonth', 'elwise', 1, True, 'dt'),
    floor            = operation('floor', 'elwise', 1, False, 'dt'),
    hour             = operation('hour', 'elwise', 1, True, 'dt'),
    is_leap_year     = operation('is_leap_year', 'elwise', 1, True, 'dt'),
    is_month_end     = operation('is_month_end', 'elwise', 1, True, 'dt'),
    is_month_start   = operation('is_month_start', 'elwise', 1, True, 'dt'),
    is_quarter_end   = operation('is_quarter_end', 'elwise', 1, True, 'dt'),
    is_quarter_start = operation('is_quarter_start', 'elwise', 1, True, 'dt'),
    is_year_end      = operation('is_year_end', 'elwise', 1, True, 'dt'),
    is_year_start    = operation('is_year_start', 'elwise', 1, True, 'dt'),
    microsecond      = operation('microsecond', 'elwise', 1, True, 'dt'),
    minute           = operation('minute', 'elwise', 1, True, 'dt'),
    month            = operation('month', 'elwise', 1, True, 'dt'),
    month_name       = operation('month_name', 'elwise', 1, False, 'dt'),
    nanosecond       = operation('nanosecond', 'elwise', 1, True, 'dt'),
    normalize        = operation('normalize', 'elwise', 1, False, 'dt'),
    quarter          = operation('quarter', 'elwise', 1, True, 'dt'),
    qyear            = operation('qyear', 'elwise', 1, True, 'dt'),
    round            = operation('round', 'elwise', 1, False, 'dt'),
    second           = operation('second', 'elwise', 1, True, 'dt'),
    strftime         = operation('strftime', 'elwise', 1, False, 'dt'),
    time             = operation('time', 'elwise', 1, True, 'dt'),
    timetz           = operation('timetz', 'elwise', 1, True, 'dt'),
    to_period        = operation('to_period', 'elwise', 1, False, 'dt'),
    to_pydatetime    = operation('to_pydatetime', 'elwise', 1, False, 'dt'),
    to_pytimedelta   = operation('to_pytimedelta', 'elwise', 1, True, 'dt'),
    to_timestamp     = operation('to_timestamp', 'elwise', 1, False, 'dt'),
    total_seconds    = operation('total_seconds', 'elwise', 1, True, 'dt'),
    tz_convert       = operation('tz_convert', 'elwise', 1, False, 'dt'),
    tz_localize      = operation('tz_localize', 'elwise', 1, False, 'dt'),
    week             = operation('week', 'elwise', 1, True, 'dt'),
    weekday          = operation('weekday', 'elwise', 1, True, 'dt'),
    weekofyear       = operation('weekofyear', 'elwise', 1, True, 'dt'),
    year             = operation('year', 'elwise', 1, True, 'dt'),

    freq = operation('freq', 'singleton', 1, True, 'dt'),
    tz   = operation('tz', 'singleton', 1, True, 'dt'),

    components = operation('components', None, 1, True, 'dt'),
    end_time   = operation('end_time', None, 1, True, 'dt'),
    start_time = operation('start_time', None, 1, True, 'dt'),
    )

# TODO: make aliases
#ops_dt_aliases = "days", "microseconds", "nanoseconds", "seconds"] 

ops_dt_plain = Namespace(
    # Note: only between_time is useful to siuba!
    asfreq            = operation('asfreq', 'elwise', 1),
    between_time      = operation('between_time', None, 1),

    asof              = operation('asof', 'window', 1),
    shift             = operation('shift', 'window', 1),
    tshift            = operation('tshift', None, 1),

    at_time           = operation('at_time', None, 1),
    first_valid_index = operation('first_valid_index', None, 1),
    last_valid_index  = operation('last_valid_index', None, 1),
    resample          = operation('resample', None, 1),
    slice_shift       = operation('slice_shift', None, 1),
    )

ops_missing = Namespace(
    fillna      = operation('fillna', 'elwise', 1),
    isna        = operation('isna', 'elwise', 1),
    isnull      = operation('isnull', 'elwise', 1),
    notna       = operation('notna', 'elwise', 1),
    notnull     = operation('notnull', 'elwise', 1),

    bfill       = operation('bfill', 'window', 1),
    ffill       = operation('ffill', 'window', 1),

    dropna      = operation('dropna', None, 1),
    interpolate = operation('interpolate', None, 1),
    )

# =============================================================================
# Not broadly supported 
# =============================================================================

# Function application --------------------------------------------------------

ops_apply = Namespace(
    agg       = operation('agg', None, 1, True, None),
    aggregate = operation('aggregate', None, 1, True, None),
    apply     = operation('apply', None, 1, True, None),
    ewm       = operation('ewm', None, 1, True, None),
    expanding = operation('expanding', None, 1, True, None),
    groupby   = operation('groupby', None, 1, True, None),
    map       = operation('map', None, 1, True, None),
    pipe      = operation('pipe', None, 1, True, None),
    rolling   = operation('rolling', None, 1, True, None),
    transform = operation('transform', None, 1, True, None),
    )

# Attributes ------------------------------------------------------------------

ops_attrs = Namespace(
    T            = operation('T', None, 1, True, None),
    array        = operation('array', None, 1, True, None),
    attrs        = operation('attrs', None, 1, True, None),
    axes         = operation('axes', None, 1, True, None),
    base         = operation('base', None, 1, True, None),
    data         = operation('data', None, 1, True, None),
    dtype        = operation('dtype', 'singleton', 1, True, None),
    dtypes       = operation('dtypes', None, 1, True, None),
    empty        = operation('empty', 'agg', 1, True, None),
    flags        = operation('flags', None, 1, True, None),
    ftype        = operation('ftype', None, 1, True, None),
    ftypes       = operation('ftypes', None, 1, True, None),
    hasnans      = operation('hasnans', 'agg', 1, True, None),
    index        = operation('index', None, 1, True, None),
    itemsize     = operation('itemsize', None, 1, True, None),
    memory_usage = operation('memory_usage', 'agg', 1, True, None),
    name         = operation('name', 'singleton', 1, True, None),
    nbytes       = operation('nbytes', 'agg', 1, True, None),
    ndim         = operation('ndim', 'singleton', 1, True, None),
    put          = operation('put', None, 1, True, None),
    shape        = operation('shape', None, 1, True, None),
    size         = operation('size', 'agg', 1, True, None),
    strides      = operation('strides', None, 1, True, None),
    transpose    = operation('transpose', None, 1),
    values       = operation('values', None, 1, True, None),
    )


# Categoricals ----------------------------------------------------------------

ops_cat = Namespace(
    add_categories = operation('add_categories', 'elwise', 1, False, 'cat'),
    as_ordered = operation('as_ordered', 'elwise', 1, False, 'cat'),
    as_unordered = operation('as_unordered', 'elwise', 1, False, 'cat'),
    categories = operation('categories', None, 1, True, 'cat'),
    codes = operation('codes', None, 1, True, 'cat'),
    ordered = operation('ordered', None, 1, True, 'cat'),
    remove_categories = operation('remove_categories', 'elwise', 1, False, 'cat'),
    remove_unused_categories = operation('remove_unused_categories', 'window', 1, False, 'cat'),
    rename_categories = operation('rename_categories', 'elwise', 1, False, 'cat'),
    reorder_categories = operation('reorder_categories', 'elwise', 1, False, 'cat'),
    set_categories = operation('set_categories', 'elwise', 1, False, 'cat'),
    )

# Combining ---
ops_combine = Namespace(
    combine       = operation('combine', None, 2),
    combine_first = operation('combine_first', None, 2),
    append        = operation('append', None, 2),
    replace       = operation('replace', 'elwise', 1),
    update        = operation('update', None, 1),
    )

ops_convert = Namespace(
    __array__ = operation('__array__', None, 1),
    astype = operation('astype', 'elwise', 1),
    bool = operation('bool', 'agg', 1, True, None),
    convert_dtypes = operation('convert_dtypes', 'window', 1),
    copy = operation('copy', 'elwise', 1),
    get_values = operation('get_values', None, 1),
    infer_objects = operation('infer_objects', None, 1),
    to_list = operation('to_list', None, 1),
    to_numpy = operation('to_numpy', None, 1),
    to_period = operation('to_period', 'elwise', 1),
    to_timestamp = operation('to_timestamp', None, 1),
    tolist = operation('tolist', None, 1),
    )

ops_index = Namespace(
    add_prefix = operation('add_prefix', None, 1),
    add_suffix = operation('add_suffix', None, 1),
    align = operation('align', None, 1, True, None),
    at = operation('at', 'window', 1),
    drop = operation('drop', None, 1, True, None),
    drop_duplicates = operation('drop_duplicates', None, 1),
    droplevel = operation('droplevel', None, 1, True, None),
    duplicated = operation('duplicated', 'window', 1),
    equals = operation('equals', 'agg', 2),
    filter = operation('filter', None, 1),
    first = operation('first', None, 1),
    get = operation('get', 'window', 1),
    head = operation('head', 'window', 1),
    iat = operation('iat', 'window', 1),
    idxmax = operation('idxmax', 'agg', 1),
    idxmin = operation('idxmin', 'agg', 1),
    iloc = operation('iloc', 'window', 1),
    item = operation('item', None, 1),
    items = operation('items', None, 1),
    iteritems = operation('iteritems', None, 1),
    last = operation('last', None, 1),
    loc = operation('loc', None, 1),
    mask = operation('mask', 'elwise', 2),
    pop = operation('pop', None, 1),
    reindex = operation('reindex', None, 1, True, None),
    reindex_like = operation('reindex_like', None, 1, True, None),
    rename = operation('rename', None, 1),
    rename_axis = operation('rename_axis', None, 1, True, None),
    reset_index = operation('reset_index', None, 1),
    sample = operation('sample', None, 1),
    set_axis = operation('set_axis', None, 1, True, None),
    tail = operation('tail', 'window', 1),
    take = operation('take', None, 1, True, None),
    truncate = operation('truncate', None, 1, True, None),
    where = operation('where', 'elwise', 2),
    xs = operation('xs', None, 1),
    )

ops_io = Namespace(
    to_clipboard = operation('to_clipboard', None, 1),
    to_csv = operation('to_csv', None, 1),
    to_dense = operation('to_dense', None, 1),
    to_dict = operation('to_dict', None, 1),
    to_excel = operation('to_excel', None, 1),
    to_frame = operation('to_frame', None, 1),
    to_hdf = operation('to_hdf', None, 1),
    to_json = operation('to_json', None, 1),
    to_latex = operation('to_latex', None, 1),
    to_markdown = operation('to_markdown', None, 1),
    to_msgpack = operation('to_msgpack', None, 1),
    to_pickle = operation('to_pickle', None, 1),
    to_sql = operation('to_sql', None, 1),
    to_string = operation('to_string', None, 1),
    to_xarray = operation('to_xarray', None, 1),
    )

ops_reshape = Namespace(
    argmax = operation('argmax', None, 1, True, None),
    argmin = operation('argmin', None, 1, True, None),
    argsort = operation('argsort', None, 1, True, None),
    explode = operation('explode', None, 1),
    ravel = operation('ravel', None, 1),
    reorder_levels = operation('reorder_levels', None, 1, True, None),
    repeat = operation('repeat', None, 1),
    searchsorted = operation('searchsorted', None, 1),
    sort_index = operation('sort_index', None, 1),
    sort_values = operation('sort_values', None, 1),
    squeeze = operation('squeeze', None, 1),
    swapaxes = operation('swapaxes', None, 1, True, None),
    swaplevel = operation('swaplevel', None, 1, True, None),
    unstack = operation('unstack', None, 1, True, None),
    view = operation('view', None, 1, True, None),
    )

ops_sparse = Namespace(
    density = operation('density', None, 1, True, 'sparse'),
    fill_value = operation('fill_value', None, 1, True, 'sparse'),
    from_coo = operation('from_coo', None, 1, False, 'sparse'),
    npoints = operation('npoints', None, 1, True, 'sparse'),
    sp_values = operation('sp_values', None, 1, True, 'sparse'),
    to_coo = operation('to_coo', None, 1, False, 'sparse'),
    to_dense = operation('to_dense', None, 1, False, 'sparse'),
    )


# keys method is necessary for Namespace to be a mapping, so we can dict unpack.
# For now, just put the keys generic function on its own. It's not a commonly
# used method, especially in siuba (since it just gets the index...)
keys = operation('keys', None, 1)

# Put all ops into a dict, that can be used by e.g. CallTreeLocal -------------

# This is a hacky way to get all the operations on a single namespace, since
# SimpleNamepace does not allow iteration
PLAIN_OPS = dict(
        # TODO: modify CallTreeLocal to take modules for accessors
        **ops_infix,
        **ops_infix_methods,
        **ops_apply,
        **ops_attrs,
        **ops_compute,
        **ops_dt_plain,
        **ops_missing,
        **ops_combine,
        **ops_convert,
        **ops_index,
        **ops_reshape,
        keys = keys,
        )

ACCESSOR_OPS = dict(
        **{'str.' + k: v for k,v in dict(ops_str).items()},
        **{'dt.' + k: v for k,v in dict(ops_dt).items()},
        **{'cat.' + k: v for k,v in dict(ops_cat).items()},
        **{'sparse.' + k: v for k,v in dict(ops_sparse).items()},
        )

ALL_OPS = {**PLAIN_OPS, **ACCESSOR_OPS}

ALL_ACCESSORS = set()
ALL_PROPERTIES = set()

for _k, _f in ALL_OPS.items():
    if _f.operation.is_property: ALL_PROPERTIES.add(_k)
    access = _f.operation.accessor
    if access is not None: ALL_ACCESSORS.add(access)

