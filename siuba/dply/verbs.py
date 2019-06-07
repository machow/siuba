from functools import singledispatch
from pandas import DataFrame
import pandas as pd
import numpy as np

from pandas.core.groupby import DataFrameGroupBy
from siuba.siu import Symbolic, Call, strip_symbolic, MetaArg, BinaryOp, create_sym_call, Lazy

DPLY_FUNCTIONS = (
        # Dply ----
        "group_by", "ungroup", 
        "select", "rename",
        "mutate", "transmute", "filter", "summarize",
        "arrange", "distinct",
        "count", "add_count",
        "head",
        # Tidy ----
        "spread", "gather",
        "nest", "unnest",
        "expand", "complete",
        # Joins ----
        "join", "inner_join", "full_join", "left_join", "right_join", "semi_join", "anti_join",
        # TODO: move to vectors
        "if_else", "case_when",
        "collect", "show_query"
        )

__all__ = [*DPLY_FUNCTIONS, "Pipeable", "pipe"]


# General TODO ================================================================
# * expressions in group_by
# * n_distinct?
# * separate_rows
# * tally

def install_pd_siu():
    # https://github.com/coursera/pandas-ply/blob/master/pandas_ply/methods.py
    func_dict = globals()
    for func_name in DPLY_FUNCTIONS:
        f = func_dict[func_name]

        method_name = "siu_{}".format(func_name)
        setattr(pd.DataFrame, method_name, f)
        setattr(DataFrameGroupBy, method_name, f)

    DataFrameGroupBy._repr_html_ = _repr_grouped_df_html_

def _repr_grouped_df_html_(self):
    return "<div><p>(grouped data frame)</p>" + self._selected_obj._repr_html_() + "</div>"

# TODO: should be a subclass of Call?
class Pipeable:
    def __init__(self, f = None, calls = None):
        if f is not None:
            if calls is not None: raise Exception()
            self.calls = [f]
        else:
            self.calls = calls

    def __rshift__(self, x):
        if isinstance(x, Pipeable):
            return Pipeable(calls = self.calls + x.calls)
        elif callable(x):
            return Pipeable(calls = self.calls + [x])

        raise Exception()

    def __rrshift__(self, x):
        if isinstance(x, Pipeable):
            return Pipeable(calls = x.calls + self.calls)
        elif callable(x):
            return Pipeable(calls = [x] + self.calls)

        return self(x)

    def __call__(self, x):
        res = x
        for f in self.calls:
            res = f(res)
        return res

pipe = Pipeable

def _regroup(df):
    # try to regroup, when user kept index (e.g. group_keys = True)
    if len(df.index.names) > 1:
        # handle cases where...
        # 1. grouping with named indices (as_index = True)
        # 2. grouping is level 0 (as_index = False)
        grp_levels = [x for x in df.index.names if x is not None] or [0]

    return df.groupby(level = grp_levels)


def simple_varname(call):
    if isinstance(call, str):
        return call

    # check for expr like _.some_var or _["some_var"]
    if (call.func in {"__getitem__", "__getattr__"}
        and isinstance(call.args[0], MetaArg)
        and isinstance(call.args[1], str)
        ):
        # return variable name
        return call.args[1]

    return None


# Symbolic Wrapper ============================================================

from functools import wraps

class NoArgs: pass

def pipe_no_args(f, cls = NoArgs):
    @f.register(cls)
    def wrapper(__data, *args, **kwargs):
        return create_pipe_call(f, MetaArg("_"), *args, **kwargs)

    return f

def register_pipe(f, cls):
    @f.register(cls)
    def wrapper(*args, **kwargs):
        return create_pipe_call(f, MetaArg("_"), *args, **kwargs)
    return f


# option: no args, custom dispatch (e.g. register NoArgs)
# strips symbols
def singledispatch2(cls, f = None):
    """Wrap singledispatch. Making sure to keep its attributes on the wrapper.
    
    This wrapper has three jobs:
        1. strip symbols off of calls
        2. pass NoArgs instance for calls like some_func(), so dispatcher can handle
        3. return a Pipeable when the first arg of a call is a symbol
    """

    # classic way of allowing args to a decorator
    if f is None:
        return lambda f: singledispatch2(cls, f)

    # initially registers func for object, so need to change to pd.DataFrame
    dispatch_func = singledispatch(f)
    if isinstance(cls, tuple):
        for c in cls: dispatch_func.register(c, f)
    else:
        dispatch_func.register(cls, f)
    # then, set the default object dispatcher to create a pipe
    register_pipe(dispatch_func, object)

    # register dispatcher for Call, and NoArgs
    pipe_call(dispatch_func)
    pipe_no_args(dispatch_func)

    @wraps(dispatch_func)
    def wrapper(*args, **kwargs):
        strip_args = map(strip_symbolic, args)
        strip_kwargs = {k: strip_symbolic(v) for k,v in kwargs.items()}

        if not args:
            return dispatch_func(NoArgs(), **strip_kwargs)

        return dispatch_func(*strip_args, **strip_kwargs)

    return wrapper


def create_pipe_call(source, *args, **kwargs):
    first, *rest = args
    return Pipeable(Call(
            "__call__",
            strip_symbolic(source),
            strip_symbolic(first),
            *(Lazy(strip_symbolic(x)) for x in rest),
            **{k: Lazy(strip_symbolic(v)) for k,v in kwargs.items()}
            ))

def pipe_with_meta(f, *args, **kwargs):
    return create_pipe_call(f, MetaArg("_"), *args, **kwargs)


def pipe_call(f):
    @f.register(Call)
    def f_dispatch(__data, *args, **kwargs):
        call = __data
        if isinstance(call, MetaArg):
            # single _ passed as first arg to function
            # e.g. mutate(_, _.id)
            return create_pipe_call(f, call, *args, **kwargs)
        else:
            # more complex _ expr passed as first arg to function
            # e.g. mutate(_.id)
            return pipe_with_meta(f, call, *args, **kwargs)

    return f


MSG_TYPE_ERROR = "The first argument to {func} must be one of: {types}"

def raise_type_error(f):
    raise TypeError(MSG_TYPE_ERROR.format(
                func = f.__name__,
                types = ", ".join(map(str, f.registry.keys()))
                ))

# Collect and show_query =========

@pipe_no_args
@singledispatch2((DataFrame, DataFrameGroupBy))
def collect(__data, *args, **kwargs):
    # simply return DataFrame, since requires no execution
    return __data


@pipe_no_args
@singledispatch2((DataFrame, DataFrameGroupBy))
def show_query(__data, simplify = False):
    print("No query to show for a DataFrame")
    return __data

# Mutate ======================================================================

# TODO: support for unnamed args
@singledispatch2(pd.DataFrame)
def mutate(__data, **kwargs):
    """Assign new variables to a DataFrame, while keeping existing ones.

    Args:
        ___data: a DataFrame
        **kwargs: new_col_name=value pairs, where value can be a function taking
                  a single argument for the data being operated on.

    Examples
    --------

    ::
        from siuba.data import mtcars
        mtcars >> mutate(cyl2 = _.cyl * 2, cyl4 = _.cyl2 * 2)
        
    """
    
    return __data.assign(**kwargs)


@mutate.register(DataFrameGroupBy)
def _mutate(__data, **kwargs):
    df = __data.apply(lambda d: d.assign(**kwargs))
    
    return _regroup(df)



# Group By ====================================================================

@singledispatch2((pd.DataFrame, DataFrameGroupBy))
def group_by(__data, *args, **kwargs):
    tmp_df = mutate(__data, **kwargs) if kwargs else __data

    by_vars = list(map(simple_varname, args))
    for ii, name in enumerate(by_vars):
        if name is None: raise Exception("group by variable %s is not a column name" %ii)

    by_vars.extend(kwargs.keys())

    return tmp_df.groupby(by = by_vars)


@singledispatch2((pd.DataFrame, DataFrameGroupBy))
def ungroup(__data):
    # TODO: can we somehow just restore the original df used to construct
    #       the groupby?
    if isinstance(__data, pd.DataFrame):
        return __data
    if isinstance(__data, pd.Series):
        return __data.reset_index()

    return __data.obj.reset_index(drop = True)



# Filter ======================================================================

@singledispatch2(pd.DataFrame)
def filter(__data, *args):
    """Keep rows where conditions are true.

    Args:
        ___data: a DataFrame
        *args: conditions that must be met to keep a column. Multiple conditions
               are combined using ``&``.

    Examples
    --------

    ::
        from siuba.data import mtcars
        # keep rows where cyl is 4 and mpg is less than 25
        mtcars >> filter(mtcars, _.cyl ==  4, _.mpg < 25) 

    """
    crnt_indx = True
    for arg in args:
        crnt_indx &= arg(__data) if callable(arg) else arg

    # use loc or iloc to subset, depending on crnt_indx ----
    # the main issue here is that loc can't remove all rows using a slice
    # and iloc can't use a boolean series
    if isinstance(crnt_indx, bool) or isinstance(crnt_indx, np.bool_):
        # iloc can do slice, but not a bool series
        return __data.iloc[slice(None) if crnt_indx else slice(0),:]

    return __data.loc[crnt_indx,:]

@filter.register(DataFrameGroupBy)
def _filter(__data, *args):
    df_filter = filter.registry[pd.DataFrame]

    filtered = __data.apply(df_filter, *args)

    return _regroup(filtered)



# Summarize ===================================================================

@singledispatch2(DataFrame)
def summarize(__data, **kwargs):
    """Assign variables that are single number summaries of a DataFrame.


    Args:
        ___data: a DataFrame
        **kwargs: new_col_name=value pairs, where value can be a function taking
                  a single argument for the data being operated on.

    Note
    ----

    Grouped DataFrames will produce one row for each group. Ungrouped DataFrames
    will produce a single row.


    Examples
    --------

    ::
        from siuba.data import mtcars
        mtcars >> summarize(mean = _.disp.mean(), n = n(_))
        
    """
    results = {}
    for k, v in kwargs.items():
        res = v(__data) if callable(v) else v

        # TODO: validation?

        results[k] = res
        
    return DataFrame(results, index = [0])

    
@summarize.register(DataFrameGroupBy)
def _summarize(__data, **kwargs):
    df_summarize = summarize.registry[pd.DataFrame]

    df = __data.apply(df_summarize, **kwargs)
        
    group_by_lvls = list(range(df.index.nlevels - 1))
    out = df.reset_index(group_by_lvls)
    out.index = pd.RangeIndex(df.shape[0])

    return out



# Transmute ===================================================================

@singledispatch2(DataFrame)
def transmute(__data, *args, **kwargs):
    arg_vars = list(map(simple_varname, args))
    for ii, name in enumerate(arg_vars):
        if name is None: raise Exception("complex, unnamed expression at pos %s not supported"%ii)

    f_mutate = mutate.registry[pd.DataFrame]

    df = f_mutate(__data, **kwargs) 

    return df[[*arg_vars, *kwargs.keys()]]

@transmute.register(DataFrameGroupBy)
def _transmute(__data, *args, **kwargs):
    # Note: not the most efficient function. Selects columns every group.
    f_transmute = transmute.registry[pd.DataFrame]

    df = __data.apply(f_transmute, *args, **kwargs)

    return _regroup(df)



# Select ======================================================================

from collections import OrderedDict
from itertools import chain

class Var:
    def __init__(self, name, negated = False, alias = None):
        self.name = name
        self.negated = negated
        self.alias = alias

    def __neg__(self):
        return self.to_copy(negated = not self.negated)

    def __eq__(self, x):
        name = x.name if isinstance(x, Var) else x
        return self.to_copy(name = name, negated = False, alias = self.name)

    def __call__(self, *args, **kwargs):
        call = Call('__call__',
                    BinaryOp('__getattr__', MetaArg("_"), self.name),
                    *args,
                    **kwargs
                    )

        return self.to_copy(name = call)


    def __repr__(self):
        return "Var('{self.name}', negated = {self.negated}, alias = {self.alias})" \
                    .format(self = self)

    def __str__(self):
        op = "-" if self.negated else ""
        pref = self.alias + " = " if self.alias else ""
        return "{pref}{op}{self.name}".format(pref = pref, op = op, self = self)

    def to_copy(self, **kwargs):
        return self.__class__(**{**self.__dict__, **kwargs})


class VarList:
    def __getattr__(self, x):
        return Var(x)

    def __getitem__(self, x):
        return Var(x)


def var_slice(colnames, x):
    """Return indices in colnames correspnding to start and stop of slice."""
    # TODO: produces bahavior similar to df.loc[:, "V1":"V3"], but can reverse
    # TODO: make DRY
    # TODO: reverse not including end points
    if isinstance(x.start, Var):
        start_indx = (colnames == x.start.name).idxmax()
    elif isinstance(x.start, str):
        start_indx = (colnames == x.start).idxmax()
    else:
        start_indx = x.start or 0

    if isinstance(x.stop, Var):
        stop_indx = (colnames == x.stop.name).idxmax() + 1
    elif isinstance(x.stop, str):
        stop_indx = (colnames == x.stop).idxmax() + 1
    else:
        stop_indx = x.stop or len(colnames)

    if start_indx > stop_indx:
        return stop_indx, start_indx
    else:
        return start_indx, stop_indx

def var_put_cols(name, var, cols):
    if isinstance(name, list) and var.alias is not None:
        raise Exception("Cannot assign name to multiple columns")
    
    names = [name] if not isinstance(name, list) else name

    for name in names:
        if var.negated:
            if name in cols: cols.pop(name)
        #elif name in cols: cols.move_to_end(name)
        else: cols[name] = var.alias

def flatten_var(var):
    if isinstance(var, Var) and isinstance(var.name, (tuple, list)):
        return [var.to_copy(name = x) for x in var.name]
    
    return [var]
            



def var_select(colnames, *args):
    # TODO: don't erase named column if included again
    colnames = colnames if isinstance(colnames, pd.Series) else pd.Series(colnames)
    cols = OrderedDict()
    everything = None

    #flat_args = var_flatten(args)
    all_vars = chain(*map(flatten_var, args))

    # Add entries in pandas.rename style {"orig_name": "new_name"}
    for arg in all_vars:
        # strings are added directly
        if isinstance(arg, str):
            cols[arg] = None
        # integers add colname at corresponding index
        elif isinstance(arg, int):
            cols[colnames[arg]] = None
        # general var handling
        elif isinstance(arg, Var):
            # remove negated Vars, otherwise include them
            if arg.negated and everything is None:
                # first time using negation, apply an implicit everything
                everything = True
                cols.update((k, None) for k in colnames if k not in cols)

            # slicing can refer to single, or range of columns
            if isinstance(arg.name, slice):
                start, stop = var_slice(colnames, arg.name)
                for ii in range(start, stop):
                    var_put_cols(colnames[ii], arg, cols)
            # method calls like endswith()
            elif callable(arg.name):
                # TODO: not sure if this is a good idea...
                #       basically proxies to pandas str methods (they must return bool array)
                indx = arg.name(colnames.str)
                var_put_cols(colnames[indx].tolist(), arg, cols)
                #cols.update((x, None) for x in set(colnames[indx]) - set(cols))
            else:
                var_put_cols(arg.name, arg, cols)
        else:
            raise Exception("variable must be either a string or Var instance")

    return cols

def var_create(*args):
    vl = VarList()
    all_vars = []
    for arg in args:
        if callable(arg) and not isinstance(arg, Var):
            all_vars.append(arg(vl))
        else:
            all_vars.append(arg)
     
    return all_vars

@singledispatch2(DataFrame)
def select(__data, *args, **kwargs):
    if kwargs:
        raise NotImplementedError(
                "Using kwargs in select not currently supported. "
                "Use _.newname == _.oldname instead"
                )
    var_list = var_create(*args)

    od = var_select(__data.columns, *var_list)

    to_rename = {k: v for k,v in od.items() if v is not None}

    return __data[list(od)].rename(columns = to_rename)
    
@select.register(DataFrameGroupBy)
def _select(__data, *args, **kwargs):
    raise Exception("Selecting columns of grouped DataFrame currently not allowed")



# Rename ======================================================================

@singledispatch2(DataFrame)
def rename(__data, **kwargs):
    # TODO: allow names with spaces, etc..
    col_names = {simple_varname(v):k for k,v in kwargs.items()}
    if None in col_names:
        raise ValueError("Rename needs column name (e.g. 'a' or _.a), but received %s"%col_names[None])

    return __data.rename(columns  = col_names)

@rename.register(DataFrameGroupBy)
def _rename(__data, **kwargs):
    raise NotImplementedError("Selecting columns of grouped DataFrame currently not allowed")



# Arrange =====================================================================

def _call_strip_ascending(f):
    if isinstance(f, Symbolic):
        f = f.source

    if isinstance(f, Call) and f.func == "__neg__":
        return f.args[0], False

    return f, True

@singledispatch2(DataFrame)
def arrange(__data, *args):
    # TODO:
    #   - add arguments to pass to sort_values (e.g. ascending, kind)
    # 
    # basically need some (1) select behavior, (2) mutate-like behavior
    # df.sort_values is the obvious candidate, but only takes names, not expressions
    # to work around this, we make a shallow copy of data, and add sorting columns
    # then drop them at the end
    # 
    # sort order is determined by using a unary w/ Call e.g. -_.repo

    df = __data.copy(deep = False)
    n_cols = len(df.columns)
    n_args = len(args)

    #kwargs = {n_cols + ii: arg for ii,arg in enumerate(args)}

    # TODO: more careful handling of arg types (true across library :/ )..
    tmp_cols = []
    sort_cols = []
    ascending = []
    for ii, arg in enumerate(args):
        f, asc = _call_strip_ascending(arg)
        ascending.append(asc)

        col = simple_varname(f)
        if col is not None:
            sort_cols.append(col)
        else:
            # TODO: could screw up if user has columns names that are ints...
            sort_cols.append(n_cols + ii)
            tmp_cols.append(n_cols + ii)

            df[n_cols + ii] = f(df)


    return df.sort_values(by = sort_cols, kind = "mergesort", ascending = ascending) \
             .drop(tmp_cols, axis = 1)


@arrange.register(DataFrameGroupBy)
def _arrange(__data, *args):
    raise NotImplementedError("TODO: arrange with grouped DataFrame")



# Distinct ====================================================================

@singledispatch2(DataFrame)
def distinct(__data, *args, _keep_all = False, **kwargs):
    # using dict as ordered set
    cols = {simple_varname(x): True for x in args}
    if None in cols:
        raise Exception("positional arguments must be simple column, "
                        "e.g. _.colname or _['colname']"
                        )

    # mutate kwargs
    cols.update(kwargs)

    # special case: use all variables when none are specified
    if not len(cols): cols = __data.columns

    tmp_data = mutate(__data, **kwargs).drop_duplicates(list(cols)).reset_index(drop = True)

    if not _keep_all:
        return tmp_data[list(cols)]

    return tmp_data
        
@distinct.register(DataFrameGroupBy)
def _distinct(__data, *args, _keep_all = False, **kwargs):
    df = __data.apply(lambda x: distinct(x, *args, _keep_all = _keep_all, **kwargs))
    return _regroup(df)


# if_else
# TODO: move to vector.py
@singledispatch
def if_else(__data, *args, **kwargs):
    raise_type_error(__data)

@if_else.register(Call)
@if_else.register(Symbolic)
def _if_else(__data, *args, **kwargs):
    return create_sym_call(if_else, __data, *args, **kwargs)

@if_else.register(pd.Series)
def _if_else(cond, true_vals, false_vals):
    true_indx = np.where(cond)[0]
    false_indx = np.where(~cond)[0]

    result = np.repeat(None, len(cond))

    result[true_indx] =  true_vals[true_indx] if np.ndim(true_vals) else true_vals
    result[false_indx] = false_vals[false_indx] if np.ndim(false_vals) else false_vals

    # TODO: inefficient way to downcast?
    return np.array(list(result))


# case_when ----------------
# note that here, we don't use @Pipeable.add_to_dispatcher.
# because case_when takes a dictionary of cases, we need to wrap cases into
# a Call, so that it can be handled by call tree visitors, etc..
# TODO: evaluate this non-table verb approach
from siuba.siu import DictCall

@singledispatch2((pd.DataFrame,pd.Series))
def case_when(__data, cases):
    if isinstance(cases, Call):
        cases = cases(__data)
    # TODO: handle when receive list of (k,v) pairs for py < 3.5 compat?
    out = np.repeat(None, len(__data))
    for k, v in reversed(list(cases.items())):
        if callable(k):
            result = k(__data)
            indx = np.where(result)[0]
            out[indx] = v
        elif k:
            # e.g. k is just True, etc..
            out[:] = v

    return np.array(list(out))

@case_when.register(Symbolic)
@case_when.register(Call)
def _case_when(__data, cases):
    if not isinstance(cases, dict):
        raise Exception("Cases must be a dictionary")
    dict_entries = dict((strip_symbolic(k), strip_symbolic(v)) for k,v in cases.items())
    cases_arg = Lazy(DictCall("__call__", dict, dict_entries))
    return create_sym_call(case_when, __data, cases_arg)




# Count =======================================================================

def _count_group(data, *args):
    crnt_cols = set(data.columns)
    out_col = "n"
    while out_col in crnt_cols: out_col = out_col + "n"

    return 


@singledispatch2(pd.DataFrame)
def count(__data, *args, wt = None, sort = False, **kwargs):
    # TODO: if expr, works like mutate

    #group by args
    if wt is None:
        counts = group_by(__data, *args, **kwargs).size().reset_index()
    else:
        wt_col = simple_varname(wt)
        if wt_col is None:
            raise Exception("wt argument has to be simple column name")
        counts = group_by(__data, *args, **kwargs)[wt_col].sum().reset_index()


    # count col named, n. If that col already exists, add more "n"s...
    crnt_cols = set(counts.columns)
    out_col = "n"
    while out_col in crnt_cols: out_col = out_col + "n"

    # rename the tally column to correct name
    counts.rename(columns = {counts.columns[-1]: out_col}, inplace = True)

    if sort:
        return counts.sort_values(out_col, ascending = False)

    return counts


@singledispatch2(pd.DataFrame)
def add_count(__data, *args, wt = None, sort = False, **kwargs):
    counts = count(__data, *args, wt = wt, sort = sort, **kwargs)

    on = list(counts.columns)[:-1]
    return __data.merge(counts, on = on)
    


# Tally =======================================================================

# Nest ========================================================================

@singledispatch2(pd.DataFrame)
def nest(__data, *args, key = "data"):
    """Nest columns within a DataFrame.
    

    Args:
        ___data: a DataFrame
        *args: the names of columns to be nested. May use any syntax used by
               the ``select`` function.
        key: the name of the column that will hold the nested columns.

    Examples
    --------

    ::
        from siuba.data import mtcars
        mtcars >> nest(-_.cyl)
        
    """
    # TODO: copied from select function
    var_list = var_create(*args)
    od = var_select(__data.columns, *var_list)

    # unselected columns are treated similar to using groupby
    grp_keys = list(k for k in __data.columns if k not in set(od))
    nest_keys = list(od)

    # AFAICT you can't name the col created in the apply here
    # but it might be more efficient to act on the groupby obj directly
    out = __data.groupby(grp_keys).apply(lambda d: [d[nest_keys]]).reset_index()
    out.rename(columns = {out.columns[-1]: key}, inplace = True)
    # hack, can assign list of dataframes, but not in a groupby?
    # it ended up making a series of lists each with a single dataframe
    out[key] = [x[0] for x in out[key]]

    return out

@nest.register(DataFrameGroupBy)
def _nest(__data, *args, key = "data"):
    grp_keys = [x.name for x in __data.grouper.groupings]
    if None in grp_keys:
        raise NotImplementedError("All groupby variables must be named when using nest")

    return nest(__data.obj, -Var(grp_keys), *args, key = key)




# Unnest ======================================================================

@singledispatch2(pd.DataFrame)
def unnest(__data, key = "data"):
    """Unnest a column holding nested data (e.g. Series of lists or DataFrames).
    
    Args:
        ___data: a DataFrame
        key: the name of the column to be unnested.

    Examples
    --------

    ::
        import pandas as pd
        df = pd.DataFrame({'id': [1,2], 'data': [['a', 'b'], ['c', 'd']]})
        df >> unnest()
        
    """
    # TODO: currently only takes key, not expressions
    nrows_nested = __data[key].apply(len, convert_dtype = True)
    indx_nested = nrows_nested.index.repeat(nrows_nested)

    grp_keys = list(__data.columns[__data.columns != key])

    # flatten nested data
    data_entries = map(_convert_nested_entry, __data[key])
    long_data = pd.concat(data_entries, ignore_index = True)
    long_data.name = key

    # may be a better approach using a multi-index
    long_grp = __data.loc[indx_nested, grp_keys].reset_index(drop = True)
    
    return long_grp.join(long_data)

def _convert_nested_entry(x):
    if isinstance(x, (tuple, list)):
        return pd.Series(x)
    
    return x


# Joins =======================================================================
from collections.abc import Mapping
from functools import partial

# TODO: will need to use multiple dispatch
@singledispatch2(pd.DataFrame)
def join(left, right, on = None, how = None):
    if not isinstance(right, DataFrame):
        raise Exception("right hand table must be a DataFrame")
    if how is None:
        raise Exception("Must specify how argument")

    if isinstance(on, Mapping):
        left_on, right_on = zip(*on.items())
        return left.merge(right, how = how, left_on = left_on, right_on = right_on)

    return left.merge(right, how = how, on = on)


@join.register(object)
def _join(left, right, on = None, how = None):
    raise Exception("Unsupported type %s" %type(left))


@singledispatch2(pd.DataFrame)
def semi_join(left, right = None, on = None):
    if isinstance(on, Mapping):
        left_on, right_on = zip(*on.items())
        return left.merge(right[right_on], how = 'inner', left_on = left_on, right_on = right_on)

    if on is None:
        on_cols = set(left.columns).intersection(set(right.columns))
        if not len(on_cols):
            raise Exception("No joining column specified, and no shared column names")
    elif isinstance(on, str):
        on_cols = [on]
    else:
        on_cols = on

    return left.merge(right.loc[:,on_cols], how = 'inner', on = on_cols)

@singledispatch2(pd.DataFrame)
def anti_join(left, right = None, on = None):
    raise NotImplementedError("anti_join not currently implemented")

left_join = partial(join, how = "left")
right_join = partial(join, how = "right")
full_join = partial(join, how = "full")
inner_join = partial(join, how = "inner")


# Head ========================================================================

@singledispatch2(pd.DataFrame)
def head(__data, n = 5):
    return __data.head(n)

# Gather ======================================================================

@singledispatch2(pd.DataFrame)
def gather(__data, key = "key", value = "value", *args, drop_na = False, convert = False):
    # TODO: implement var selection over *args
    if convert:
        raise NotImplementedError("convert not yet implemented")

    # TODO: copied from nest and select
    var_list = var_create(*args)
    od = var_select(__data.columns, *var_list)

    value_vars = list(od) or None

    id_vars = [col for col in __data.columns if col not in od]
    long = pd.melt(__data, id_vars, value_vars, key, value)

    if drop_na:
        return long[~long[value].isna()]

    return long



# Spread ======================================================================

@singledispatch2(pd.DataFrame)
def spread(__data, key, value, fill = None, reset_index = True):
    id_cols = [col for col in __data.columns if col not in {key, value}]
    wide = __data.set_index(id_cols + [key]).unstack(level = -1)
    
    if fill is not None:
        wide.fillna(fill, inplace = True)
    
    # remove multi-index from both rows and cols
    wide.columns = wide.columns.droplevel().rename(None)
    if reset_index:
        wide.reset_index(inplace = True)
    
    return wide

# Expand/Complete ====================================================================
from pandas.core.reshape.util import cartesian_product

@singledispatch2(pd.DataFrame)
def expand(__data, *args, fill = None):
    var_names = list(map(simple_varname, args))
    cols = [__data[name] for name in var_names]
    # see https://stackoverflow.com/a/25636395/1144523
    cprod = cartesian_product(cols)
    expanded = pd.DataFrame(np.array(cprod).T)
    expanded.columns = var_names

    return expanded


@singledispatch2(pd.DataFrame)
def complete(__data, *args, fill = None):
    expanded = expand(__data, *args, fill = fill)

    # TODO: should we attempt to coerce cols back to original types?
    #       e.g. NAs will turn int -> float
    on_cols = list(expanded.columns)
    df = __data.merge(expanded, how = "right", on = on_cols)
    
    if fill is not None:
        for col_name, val in fill.items():
            df[col_name].fillna(val, inplace = True)

    return df
    

# Install Siu =================================================================

install_pd_siu()
