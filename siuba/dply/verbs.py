from functools import singledispatch
from pandas import DataFrame
import pandas as pd
import numpy as np

from pandas.core.groupby import DataFrameGroupBy
from pandas.core.dtypes.inference import is_scalar
from siuba.siu import Symbolic, Call, strip_symbolic, MetaArg, BinaryOp, _SliceOpIndex, create_sym_call, Lazy

DPLY_FUNCTIONS = (
        # Dply ----
        "group_by", "ungroup", 
        "select", "rename",
        "mutate", "transmute", "filter", "summarize",
        "arrange", "distinct",
        "count", "add_count",
        "head",
        "top_n",
        # Tidy ----
        "spread", "gather",
        "nest", "unnest",
        "expand", "complete",
        "separate", "unite", "extract",
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

def install_siu_methods(cls):
    """This function attaches siuba's table verbs on a class, to use as methods.

    """
    func_dict = globals()
    for func_name in DPLY_FUNCTIONS:
        f = func_dict[func_name]

        method_name = "siu_{}".format(func_name)
        setattr(cls, method_name, f)

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
        # symbolics like _.some_attr need to be stripped down to a call, because
        # calling _.some_attr() returns another symbolic.
        f = strip_symbolic(f)

        if f is not None:
            if calls is not None: raise Exception()
            self.calls = [f]
        else:
            self.calls = calls

    def __rshift__(self, x):
        """Handle >> syntax when pipe is on the left (lazy piping)."""
        if isinstance(x, Pipeable):
            return Pipeable(calls = self.calls + x.calls)
        elif isinstance(x, (Symbolic, Call)):
            call = strip_symbolic(x)
            return Pipeable(calls = self.calls + [call])
        elif callable(x):
            return Pipeable(calls = self.calls + [x])

        raise Exception()

    def __rrshift__(self, x):
        """Handle >> syntax when pipe is on the right (eager piping)."""
        if isinstance(x, (Symbolic, Call)):
            call = strip_symbolic(x)
            return Pipeable(calls = [call] + self.calls)
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
    # try to regroup after an apply, when user kept index (e.g. group_keys = True)
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
    if (isinstance(call, Call)
        and call.func in {"__getitem__", "__getattr__"}
        and isinstance(call.args[0], MetaArg)
        and isinstance(call.args[1], (str, _SliceOpIndex))
        ):
        # return variable name
        name = call.args[1]

        if isinstance(name, str):
            return name
        elif isinstance(name, _SliceOpIndex):
            return name.args[0]

    return None


def ordered_union(*args):
    out = {}
    for arg in args:
        out.update({el: True for el in arg})
    return tuple(out)

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
    
    orig_cols = __data.columns
    result = __data.assign(**kwargs)

    new_cols = result.columns[~result.columns.isin(orig_cols)]

    return result.loc[:, [*orig_cols, *new_cols]]



@mutate.register(DataFrameGroupBy)
def _mutate(__data, **kwargs):
    groupings = __data.grouper.groupings
    orig_index = __data.obj.index

    df = __data.apply(lambda d: d.assign(**kwargs))
    
    # will drop all but original index
    group_by_lvls = list(range(df.index.nlevels - 1))
    g_df = df.reset_index(group_by_lvls, drop = True).loc[orig_index].groupby(groupings)

    return g_df




# Group By ====================================================================

@singledispatch2((pd.DataFrame, DataFrameGroupBy))
def group_by(__data, *args, add = False, **kwargs):
    tmp_df = mutate(__data, **kwargs) if kwargs else __data

    by_vars = list(map(simple_varname, args))
    for ii, name in enumerate(by_vars):
        if name is None: raise Exception("group by variable %s is not a column name" %ii)

    by_vars.extend(kwargs.keys())

    if isinstance(tmp_df, DataFrameGroupBy) and add:
        prior_groups = [el.name for el in __data.grouper.groupings]
        all_groups = ordered_union(prior_groups, by_vars)
        return tmp_df.obj.groupby(list(all_groups))

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
        result = __data.iloc[slice(None) if crnt_indx else slice(0),:]
    else:
        result = __data.loc[crnt_indx,:]

    return result

@filter.register(DataFrameGroupBy)
def _filter(__data, *args):
    groupings = __data.grouper.groupings
    df_filter = filter.registry[pd.DataFrame]

    df = __data.apply(df_filter, *args)

    # will drop all but original index, then sort to get original order
    group_by_lvls = list(range(df.index.nlevels - 1))
    ordered = df.reset_index(group_by_lvls, drop = True).sort_index()

    group_cols = [ping.name for ping in groupings]
    return ordered.groupby(group_cols)


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

        # validate operations returned single result
        if not is_scalar(res) and len(res) > 1:
            raise ValueError("Summarize argument, %s, must return result of length 1 or a scalar." % k)

        # keep result, but use underlying array to avoid crazy index issues
        # on DataFrame construction (#138)
        results[k] = res.array if isinstance(res, pd.Series) else res
        
    # must pass index, or raises error when using all scalar values
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
    arg_vars = list(map(simple_varname, args))
    for ii, name in enumerate(arg_vars):
        if name is None: raise Exception("complex, unnamed expression at pos %s not supported"%ii)

    f_mutate = mutate.registry[DataFrameGroupBy]

    gdf = f_mutate(__data, **kwargs)
    groupings = gdf.grouper.groupings

    group_names = [x.name for x in groupings]
    if None in group_names:
        raise ValueError("Passed a grouped DataFrame to transmute, but not all "
                         "its groups are named. Groups: %s" % group_names)

    subset = ungroup(gdf)[[*group_names, *arg_vars, *kwargs.keys()]]

    return subset.groupby(groupings)



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
        f = strip_symbolic(f)

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
    for arg in args:
        f, desc = _call_strip_ascending(arg)
        if not simple_varname(f):
            raise NotImplementedError(
                    "Arrange over DataFrameGroupBy only supports simple "
                    "column names, not expressions"
                    )

    df_sorted = arrange(__data.obj, *args)

    group_cols = [ping.name for ping in __data.grouper.groupings]
    return df_sorted.groupby(group_cols)




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
    """
    Example:
        >>> ser1 = pd.Series([1,2,3,4])
        >>> if_else(ser1 > 2, np.nan, ser1)        # doctest: +SKIP
        array([ 1.,  2., nan, nan])

        >>> from siuba import _
        >>> f = if_else(_ < 3, _, 3)
        >>> f(ser1)
        array([1, 2, 3, 3])

        >>> import numpy as np
        >>> ser2 = pd.Series(['NA', 'a', 'b'])
        >>> if_else(ser2 == 'NA', np.nan, ser2)
        array([nan, 'a', 'b'], dtype=object)

    """
    raise_type_error(__data)

@if_else.register(Call)
@if_else.register(Symbolic)
def _if_else(__data, *args, **kwargs):
    return create_sym_call(if_else, __data, *args, **kwargs)

@if_else.register(pd.Series)
def _if_else(cond, true_vals, false_vals):
    result = np.where(cond.fillna(False), true_vals, false_vals)

    # TODO: should functions that take a Series, return a Series?
    #       for now, just return "O" type. Sort out once better research.
    return result


# case_when ----------------
# note that here, we don't use @Pipeable.add_to_dispatcher.
# because case_when takes a dictionary of cases, we need to wrap cases into
# a Call, so that it can be handled by call tree visitors, etc..
# TODO: evaluate this non-table verb approach
from siuba.siu import DictCall

def _val_call(call, data, n, indx = None):
    if not callable(call):
        return call

    arr = call(data)
    if arr.shape != (n,):
        raise ValueError("Expected call to return array of shape {}"
                         "but it returned shape {}".format(n, arr.shape))

    return arr[indx] if indx is not None else arr


@singledispatch2((pd.DataFrame,pd.Series))
def case_when(__data, cases):
    if isinstance(cases, Call):
        cases = cases(__data)
    # TODO: handle when receive list of (k,v) pairs for py < 3.5 compat?

    stripped_cases = {strip_symbolic(k): strip_symbolic(v) for k,v in cases.items()}
    n = len(__data)
    out = np.repeat(None, n)
    for k, v in reversed(list(stripped_cases.items())):
        if callable(k):
            result = _val_call(k, __data, n)
            indx = np.where(result)[0]

            val_res = _val_call(v, __data, n, indx)
            out[indx] = val_res
        elif k:
            # e.g. k is just True, etc..
            val_res = _val_call(v, __data, n)
            out[:] = val_res

    # by recreating an array, attempts to cast as best dtype
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


@singledispatch2((pd.DataFrame, DataFrameGroupBy))
def count(__data, *args, wt = None, sort = False, **kwargs):
    """Return the number of rows for each grouping of data.

    Args:
        __data: a DataFrame
        *args: the names of columns to be used for grouping. Passed to group_by.
        wt: the name of a column to use as a weighted for each row.
        sort: whether to sort the results in descending order.
        **kwargs: creates a new named column, and uses for grouping. Passed to group_by.

    """
    no_grouping_vars = not args and not kwargs and isinstance(__data, pd.DataFrame)

    if wt is None:
        if no_grouping_vars: 
            # no groups, just use number of rows
            counts = pd.DataFrame({'tmp': [__data.shape[0]]})
        else:
            # tally rows for each group
            counts = group_by(__data, *args, add = True, **kwargs).size().reset_index()
    else:
        wt_col = simple_varname(wt)
        if wt_col is None:
            raise Exception("wt argument has to be simple column name")

        if no_grouping_vars:
            # no groups, sum weights
            counts = pd.DataFrame({'tmp': [__data[wt_col].sum()]})
        else:
            # do weighted tally
            counts = group_by(__data, *args, add = True, **kwargs)[wt_col].sum().reset_index()


    # count col named, n. If that col already exists, add more "n"s...
    crnt_cols = set(counts.columns)
    out_col = "n"
    while out_col in crnt_cols: out_col = out_col + "n"

    # rename the tally column to correct name
    counts.rename(columns = {counts.columns[-1]: out_col}, inplace = True)

    if sort:
        return counts.sort_values(out_col, ascending = False).reset_index(drop = True)

    return counts


@singledispatch2(pd.DataFrame)
def add_count(__data, *args, wt = None, sort = False, **kwargs):
    counts = count(__data, *args, wt = wt, sort = sort, **kwargs)

    on = list(counts.columns)[:-1]
    return __data.merge(counts, on = on)
    


# Tally =======================================================================

# Nest ========================================================================

def _fast_split_df(g_df):
    """
    Note
    ----

    splitting does not scale well to many groups (e.g. 50000+). This is due
    to pandas' (1) use of indexes, (2) some hard coded actions when subsetting.
    We are currently working on a fix, so that when people aren't using indexes,
    nesting will be much faster.

    see https://github.com/machow/siuba/issues/184
    """

    # TODO (#184): speed up when user doesn't need an index
    # right now, this is essentially a copy of
    # pandas.core.groupby.ops.DataSplitter.__iter__
    from pandas._libs import lib
    splitter = g_df.grouper._get_splitter(g_df.obj)

    starts, ends = lib.generate_slices(splitter.slabels, splitter.ngroups)

    # TODO: reset index
    sdata = splitter._get_sorted_data()

    # TODO: avoid costly make_block call, and hard-coded BlockManager init actions.
    #       neither of these things is necessary when subsetting rows.
    for start, end in zip(starts, ends):
        yield splitter._chop(sdata, slice(start, end))



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

    # split into sub DataFrames, with only nest_keys as columns
    g_df = __data.groupby(grp_keys)
    splitter = g_df.grouper._get_splitter(g_df.obj[nest_keys])

    result_index = g_df.grouper.result_index
    nested_dfs = [x for ii, x in splitter]

    out = pd.DataFrame({key: nested_dfs}, index = result_index).reset_index()

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
from pandas.core.reshape.merge import _MergeOperation


# TODO: will need to use multiple dispatch
@singledispatch2(pd.DataFrame)
def join(left, right, on = None, how = None, *args, by = None, **kwargs):
    if not isinstance(right, DataFrame):
        raise Exception("right hand table must be a DataFrame")
    if how is None:
        raise Exception("Must specify how argument")

    if len(args) or len(kwargs):
        raise NotImplementedError("extra arguments to pandas join not currently supported")

    if on is None and by is not None:
        on = by

    # pandas uses outer, but dplyr uses term full
    if how == "full":
        how = "outer"

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
        # coerce colnames to list, to avoid indexing with tuples
        on_cols, right_on = map(list, zip(*on.items()))
        right = right[right_on].rename(dict(zip(right_on, on_cols)))
    elif on is None:
        on_cols = set(left.columns).intersection(set(right.columns))
        if not len(on_cols):
            raise Exception("No joining column specified, and no shared column names")
    elif isinstance(on, str):
        on_cols = [on]
    else:
        on_cols = on

    # get our semi join on ----
    if len(on_cols) == 1:
        col_name = on_cols[0]
        indx = left[col_name].isin(right[col_name])
        return left.loc[indx]

    # Not a super efficient approach. Effectively, an inner join with what would
    # be duplicate rows removed.
    merger = _MergeOperation(left, right, left_on = on_cols, right_on = on_cols)
    _, l_indx, _ = merger._get_join_info()


    range_indx = pd.RangeIndex(len(left))
    return left.loc[range_indx.isin(l_indx)]


@singledispatch2(pd.DataFrame)
def anti_join(left, right = None, on = None):
    """Return the left table with every row that would *not* be kept in an inner join.
    """
    # copied from semi_join
    if isinstance(on, Mapping):
        left_on, right_on = zip(*on.items())
    else: 
        left_on = right_on = on

    # manually perform merge, up to getting pieces need for indexing
    merger = _MergeOperation(left, right, left_on = left_on, right_on = right_on)
    _, l_indx, _ = merger._get_join_info()
    
    # use the left table's indexer to exclude those rows
    range_indx = pd.RangeIndex(len(left))
    return left.iloc[range_indx.difference(l_indx),:]

left_join = partial(join, how = "left")
right_join = partial(join, how = "right")
full_join = partial(join, how = "full")
inner_join = partial(join, how = "inner")


# Head ========================================================================

@singledispatch2(pd.DataFrame)
def head(__data, n = 5):
    return __data.head(n)


# Top N =======================================================================

# TODO: should dispatch to filter, no need to specify pd.DataFrame?
@singledispatch2((pd.DataFrame, DataFrameGroupBy))
def top_n(__data, n, wt = None):
    """Filter to keep the top or bottom entries in each group.

    Args:
        ___data: a DataFrame
        n: the number of rows to keep in each group
        wt: a column or expression that determines ordering (defaults to the last column in data)

    Examples:
        >>> from siuba import _, top_n
        >>> df = pd.DataFrame({'x': [3, 1, 2, 4], 'y': [1, 1, 0, 0]})
        >>> top_n(df, 2, _.x)
           x  y
        0  3  1
        3  4  0

        >>> top_n(df, -2, _.x)
           x  y
        1  1  1
        2  2  0

        >>> top_n(df, 2, _.x*_.y)
           x  y
        0  3  1
        1  1  1

    """
    # NOTE: using min_rank, since it can return a lazy expr for min_rank(ing)
    #       but I would rather not have it imported in verbs. will be more 
    #       reasonable if each verb were its own file? need abstract verb / vector module.
    #       vector imports experimental right now, so we need to invert deps
    # TODO: 
    #   * what if wt is a string? should remove all str -> expr in verbs like group_by etc..
    #   * verbs like filter allow lambdas, but this func breaks with that   
    from .vector import min_rank
    if wt is None:
        sym_wt = getattr(Symbolic(MetaArg("_")), __data.columns[-1])
    elif isinstance(wt, Call):
        sym_wt = Symbolic(wt)
    else:
        raise TypeError("wt must be a symbolic expression, eg. _.some_col")

    if n > 0:
        return filter(__data, min_rank(-sym_wt) <= n)
    else:
        return filter(__data, min_rank(sym_wt) <= abs(n))


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
        return long[~long[value].isna()].reset_index(drop = True)

    return long



# Spread ======================================================================

def _get_single_var_select(columns, x):
    od = var_select(columns, *var_create(x))

    if len(od) != 1:
        raise ValueError("Expected single variable, received: %s" %list(od))

    return next(iter(od))

@singledispatch2(pd.DataFrame)
def spread(__data, key, value, fill = None, reset_index = True):
    key_col = _get_single_var_select(__data.columns, key)
    val_col = _get_single_var_select(__data.columns, value)

    id_cols = [col for col in __data.columns if col not in (key_col, val_col)]
    wide = __data.set_index(id_cols + [key_col]).unstack(level = -1)
    
    if fill is not None:
        wide.fillna(fill, inplace = True)
    
    # remove multi-index from both rows and cols
    wide.columns = wide.columns.droplevel().rename(None)
    if reset_index:
        wide.reset_index(inplace = True)
    
    return wide


@spread.register(DataFrameGroupBy)
def _spread_gdf(__data, *args, **kwargs):

    groupings = __data.grouper.groupings

    df = __data.obj

    f_spread = spread.registry[pd.DataFrame]
    out = f_spread(df, *args, **kwargs)

    # regroup, using group names ----
    group_names = [x.name for x in groupings]
    if any([name is None for name in group_names]):
        raise ValueError("spread can only work on grouped DataFrame if all groupings "
                         "have names. Groups are: %s" %group_names)

    return out.groupby(group_names)

# Expand/Complete ====================================================================
from pandas.core.reshape.util import cartesian_product

@singledispatch2(pd.DataFrame)
def expand(__data, *args, fill = None):
    var_names = list(map(simple_varname, args))
    cols = [__data[name].unique() for name in var_names]
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
    
# Separate/Unit/Extract ============================================================

import warnings

@singledispatch2(pd.DataFrame)
def separate(__data, col, into, sep = r"[^a-zA-Z0-9]",
             remove = True, convert = False,
             extra = "warn", fill = "warn"
            ):
    """Split col into len(into) piece. Return DataFrame with a column added for each piece.

    Args:
        __data:  a DataFrame
        col: name of column to split (either string, or siu expression)
        into: names of resulting columns holding each entry in split
        sep: regular expression used to split col. Passed to col.str.split method.
        remove: whether to remove col from the returned DataFrame
        convert: whether to attempt to convert the split columns to numerics
        extra: what to do when more splits than into names.
               One of ("warn", "drop" or "merge").
               "warn" produces a warning; "drop" and "merge" currently not implemented.
        fill: what to do when fewer splits than into names. Currently not implemented.

    Examples
    --------

    ::
        import pandas as pd
        from siuba import separate

        df = pd.DataFrame({
            "label": ["S1-1", "S2-2"]
            })

        # split into two columns
        separate(df, "label", into = ["season", "episode"])

        # split, and try to convert columns to numerics
        separate(df, "label", into = ["season", "episode"], convert = True)

    """

    n_into = len(into)
    col_name = simple_varname(col)
    
    # splitting column ----
    all_splits = __data[col_name].str.split(sep, expand = True)
    n_split_cols = len(all_splits.columns)
    
    # handling too many or too few splits ----
    if  n_split_cols < n_into:
        # too few columns
        raise ValueError("Expected %s split cols, found %s" %(n_into, n_split_cols))
    elif n_split_cols > n_into:
        # Extra argument controls how we deal with too many splits
        if extra == "warn":
            warnings.warn("some warning about too many splits", UserWarning)
        elif extra == "drop":
            pass
        elif extra == "merge":
            raise NotImplementedError("TODO: separate extra = 'merge'")
        else:
            raise ValueError("Invalid extra argument: %s" %extra)

    # end up with only the into columns, correctly named ----
    new_names = dict(zip(range(n_into), into))
    keep_splits = all_splits.iloc[:, :n_into].rename(columns = new_names)
    
    out = pd.concat([__data, keep_splits], axis = 1)

    # attempt to convert columns to numeric ----
    if convert:
        # TODO: better strategy here? 
        for k in into:
            try:
                out[k] = pd.to_numeric(out[k])
            except ValueError:
                pass

    if remove:
        return out.drop(columns = col_name)

    return out


@separate.register(DataFrameGroupBy)
def _separate_gdf(__data, *args, **kwargs):

    groupings = __data.grouper.groupings

    df = __data.obj

    f_separate = separate.registry[pd.DataFrame]
    out = f_separate(df, *args, **kwargs)

    return out.groupby(groupings)


def _coerce_to_str(arr):
    """Return either original series, or ser.astype(str)"""
    if pd.api.types.is_string_dtype(arr):
        return arr

    return arr.astype(str)


# unite ----

from functools import reduce

@singledispatch2(pd.DataFrame)
def unite(__data, col, *args, sep = "_", remove = True):
    """Combine multiple columns into a single column. Return DataFrame that column included.

    Args:
        __data:  a DataFrame
        col: name of the to-be-created column (string).
        *args: names of each column to combine.
        sep: separater joining each column being combined.
        remove: whether to remove the combined columns from the returned DataFrame.

    """
    unite_col_names = list(map(simple_varname, args))
    out_col_name = simple_varname(col)

    # validations ----
    if None in unite_col_names:
        raise ValueError("*args must be string, or simple column name, e.g. _.col_name")

    missing_cols = set(unite_col_names) - set(__data.columns)
    if missing_cols:
        raise ValueError("columns %s not in DataFrame.columns" %missing_cols)


    unite_cols = [_coerce_to_str(__data[col_name]) for col_name in unite_col_names]

    if out_col_name in __data:
        raise ValueError("col argument %s already a column in data" % out_col_name)

    # perform unite ----
    # TODO: this is probably not very efficient. Maybe try with transform or apply?
    res = reduce(lambda x,y: x + sep + y, unite_cols)

    out_df = __data.copy()
    out_df[out_col_name] = res

    if remove:
        return out_df.drop(columns = unite_col_names)

    return out_df

@unite.register(DataFrameGroupBy)
def _unite_gdf(__data, *args, **kwargs):
    # TODO: consolidate these trivial group by dispatched funcs

    groupings = __data.grouper.groupings

    df = __data.obj

    f_unite = unite.registry[pd.DataFrame]
    out = f_unite(df, *args, **kwargs)

    return out.groupby(groupings)


# extract ----

@singledispatch2(pd.DataFrame)
def extract(
        __data, col, into, regex = r"(\w+)",
        remove = True, convert = False,
        flags = 0
        ):
    """Pull out len(into) fields from character strings. Return DataFrame with a column added for each piece.

    Args:
        __data:  a DataFrame
        col: name of column to split (either string, or siu expression).
        into: names of resulting columns holding each entry in pulled out fields.
        regex: regular expression used to extract field. Passed to col.str.extract method.
        remove: whether to remove col from the returned DataFrame.
        convert: whether to attempt to convert the split columns to numerics.
        flags: flags from the re module, passed to col.str.extract.

    """

    col_name = simple_varname(col)
    n_into = len(into)

    all_splits = __data[col_name].str.extract(regex, flags)
    n_split_cols = len(all_splits.columns)

    if n_split_cols != n_into:
        raise ValueError("Split into %s pieces, but expected %s" % (n_split_cols, n_into))

    # end up with only the into columns, correctly named ----
    new_names = dict(zip(all_splits.columns, into))
    keep_splits = all_splits.rename(columns = new_names)

    # attempt to convert columns to numeric ----
    if convert:
        # TODO: better strategy here? 
        for k in keep_splits:
            try:
                keep_splits[k] = pd.to_numeric(keep_splits[k])
            except ValueError:
                pass

    
    out = pd.concat([__data, keep_splits], axis = 1)

    if remove:
        return out.drop(columns = col_name)

    return out

@extract.register(DataFrameGroupBy)
def _extract_gdf(__data, *args, **kwargs):
    # TODO: consolidate these trivial group by dispatched funcs

    groupings = __data.grouper.groupings

    df = __data.obj

    f_extract = extract.registry[pd.DataFrame]
    out = f_extract(df, *args, **kwargs)

    return out.groupby(groupings)


# Install Siu =================================================================

install_pd_siu()
