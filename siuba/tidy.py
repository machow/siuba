from functools import singledispatch
from pandas import DataFrame
import pandas as pd
import numpy as np

from pandas.core.groupby import DataFrameGroupBy
from .siu import Symbolic, Call, strip_symbolic, MetaArg, BinaryOp, create_sym_call

# TODO: should refactor all dplyr/tidy functions into dply folder
from .dply.vector import *


# General TODO ================================================================
# * joins
# * expressions in group_by
# * distinct
# * dispatch to partial when passed a single _?
# * n_distinct?
# * separate_rows
# * compare gather/spread with melt, cast
# * tally
# * row_number
from functools import reduce

FUNCTIONS = (
        "select", "mutate", "filter", "group_by", "ungroup", "summarize",
        "transmute", "count", "distinct", "nest", "unnest", "join"
        )

def install_pd_siu():
    # https://github.com/coursera/pandas-ply/blob/master/pandas_ply/methods.py
    func_dict = globals()
    for func_name in FUNCTIONS:
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

    @classmethod
    def add_to_dispatcher(cls, f):
        @f.register(Symbolic)
        def f_dispatch(__data, *args, **kwargs):
            return cls(Call("__call__", f, __data.source, *args, **kwargs))

        return f

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





# Mutate ======================================================================

# TODO: support for unnamed args
@Pipeable.add_to_dispatcher
@singledispatch
def mutate(__data, **kwargs):
    raise Exception("no")

@mutate.register(DataFrame)
def _(__data, **kwargs):
    strip_kwargs = {k: strip_symbolic(v) for k,v in kwargs.items()}
    return __data.assign(**strip_kwargs)


@mutate.register(DataFrameGroupBy)
def _(__data, **kwargs):
    strip_kwargs = {k: strip_symbolic(v) for k,v in kwargs.items()}
    df = __data.apply(lambda d: d.assign(**strip_kwargs))
    
    return _regroup(df)



# Group By ====================================================================

@Pipeable.add_to_dispatcher
@singledispatch
def group_by(__data, *args, **kwargs):
    tmp_df = mutate(__data, **kwargs) if kwargs else __data

    by_vars = list(map(simple_varname, map(strip_symbolic, args)))
    for ii, name in enumerate(by_vars):
        if name is None: raise Exception("group by variable %s is not a column name" %ii)

    by_vars.extend(kwargs.keys())

    return tmp_df.groupby(by = by_vars)


@Pipeable.add_to_dispatcher
@singledispatch
def ungroup(__data):
    # TODO: can we somehow just restore the original df used to construct
    #       the groupby?
    if isinstance(__data, pd.DataFrame):
        return __data
    if isinstance(__data, pd.Series):
        return __data.reset_index()

    return __data.obj.reset_index(drop = True)



# Filter ======================================================================

from operator import and_

@Pipeable.add_to_dispatcher
@singledispatch
def filter(__data, *args):
    raise Exception("no")


@filter.register(DataFrame)
def _(__data, *args):
    crnt_indx = True
    for arg in args:
        crnt_indx &= arg(__data) if callable(strip_symbolic(arg)) else arg

    # use loc or iloc to subset, depending on crnt_indx ----
    # the main issue here is that loc can't remove all rows using a slice
    # and iloc can't use a boolean series
    if isinstance(crnt_indx, bool) or isinstance(crnt_indx, np.bool_):
        # iloc can do slice, but not a bool series
        return __data.iloc[slice(None) if crnt_indx else slice(0),:]

    return __data.loc[crnt_indx,:]

@filter.register(DataFrameGroupBy)
def _(__data, *args):
    df_filter = filter.registry[pd.DataFrame]

    filtered = __data.apply(df_filter, *args)

    return _regroup(filtered)



# Summarize ===================================================================

@Pipeable.add_to_dispatcher
@singledispatch
def summarize(__data, **kwargs):
    raise Exception("Summarize received unsupported first argument type: %s" %type(__data))

@summarize.register(DataFrame)
def _(__data, **kwargs):
    results = {}
    for k, v in kwargs.items():
        res = strip_symbolic(v)(__data) if callable(v) else v

        # TODO: validation?

        results[k] = res
        
    return DataFrame(results, index = [0])

    
@summarize.register(DataFrameGroupBy)
def _(__data, **kwargs):
    df_summarize = summarize.registry[pd.DataFrame]

    df = __data.apply(df_summarize, **kwargs)
        
    group_by_lvls = list(range(df.index.nlevels - 1))
    return df.reset_index(group_by_lvls)



# Transmute ===================================================================

@Pipeable.add_to_dispatcher
@singledispatch
def transmute(__data, *args, **kwargs):
    raise Exception("no")

@transmute.register(DataFrame)
def _(__data, *args, **kwargs):
    arg_vars = list(map(simple_varname, map(strip_symbolic, args)))
    for ii, name in enumerate(arg_vars):
        if name is None: raise Exception("complex, unnamed expression at pos %s not supported"%ii)

    f_mutate = mutate.registry[pd.DataFrame]

    df = f_mutate(__data, **kwargs) 

    return df[[*arg_vars, *kwargs.keys()]]

@transmute.register(DataFrameGroupBy)
def _(__data, *args, **kwargs):
    # Note: not the most efficient function. Selects columns every group.
    f_transmute = transmute.registry[pd.DataFrame]

    df = __data.apply(f_transmute, *args, **kwargs)

    return _regroup(df)



# Select ======================================================================

from collections import OrderedDict

class Var:
    def __init__(self, name, negated = False, alias = None):
        self.name = name
        self.negated = negated
        self.alias = alias

    def __neg__(self):
        return self.to_copy(negated = not self.negated)

    def __eq__(self, x):
        return self.to_copy(name = x.name, negated = False, alias = self.name)

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



def var_select(colnames, *args):
    # TODO: don't erase named column if included again
    colnames = colnames if isinstance(colnames, pd.Series) else pd.Series(colnames)
    cols = OrderedDict()
    everything = None

    # Add entries in pandas.rename style {"orig_name": "new_name"}
    for arg in args:
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
                cols.update((x, None) for x in set(colnames) - set(cols))

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


@Pipeable.add_to_dispatcher
@singledispatch
def select(__data, *args, **kwargs):
    raise Exception("no")

@select.register(DataFrame)
def _(__data, *args, **kwargs):
    vl = VarList()
    evaluated = (strip_symbolic(arg)(vl) if callable(arg) else arg for arg in args)

    od = var_select(__data.columns, *evaluated)

    to_rename = {k: v for k,v in od.items() if v is not None}

    return __data[list(od)].rename(columns = to_rename)
    
@select.register(DataFrameGroupBy)
def _(__data, *args, **kwargs):
    raise Exception("Selecting columns of grouped DataFrame currently not allowed")



# Rename ======================================================================

@Pipeable.add_to_dispatcher
@singledispatch
def rename(__data, *args, **kwargs):
    raise Exception("no")

@rename.register(DataFrame)
def _(__data, **kwargs):
    # TODO: allow names with spaces, etc..
    col_names = {v:k for k,v in kwargs.items()}

    return __data.rename(columns  = col_names)

@rename.register(DataFrameGroupBy)
def _(__data, *args, **kwargs):
    raise Exception("Selecting columns of grouped DataFrame currently not allowed")



# Arrange =====================================================================

def _call_strip_ascending(f):
    if isinstance(f, Symbolic):
        f = f.source

    if isinstance(f, Call) and f.func == "__neg__":
        return f.args[0], False

    return f, True

@Pipeable.add_to_dispatcher
@singledispatch
def arrange(__data, *args, **kwargs):
    raise Exception("no")

@arrange.register(DataFrame)
def _(__data, *args):
    # TODO:
    #   - general handling of stripping Symbolics
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
    tmp_colnames = []
    ascending = []
    for ii, arg in enumerate(args):
        if isinstance(arg, str):
            tmp_colnames.append(arg)
        else:
            # TODO: could screw up if user has columns names that are ints...
            tmp_colnames.append(n_cols + ii)

            f, asc = _call_strip_ascending(arg)
            ascending.append(asc)

            df[n_cols + ii] = f(df)

    return df.sort_values(by = tmp_colnames, kind = "mergesort", ascending = ascending) \
             .drop(tmp_colnames, axis = 1)



# Distinct ====================================================================

@Pipeable.add_to_dispatcher
@singledispatch
def distinct(__data, *args, **kwargs):
    raise Exception("no")

@distinct.register(DataFrame)
def _(__data, *args, _keep_all = False, **kwargs):
    cols = set(simple_varname(strip_symbolic(x)) for x in args)
    if None in cols:
        raise Exception("positional arguments must be simple column, "
                        "e.g. _.colname or _['colname']"
                        )

    # mutate kwargs
    cols.update(kwargs.keys())
    tmp_data = mutate(__data, **kwargs).drop_duplicates(cols)

    if not _keep_all:
        return tmp_data[cols]

    return tmp_data
        
@distinct.register(DataFrameGroupBy)
def _(__data, *args, _keep_all = False, **kwargs):
    df = __data.apply(lambda x: distinct(x, *args, _keep_all = _keep_all, **kwargs))
    return _regroup(df)


# if_else
@singledispatch
def if_else(__data, *args, **kwargs):
    raise Exception("no")

@if_else.register(Symbolic)
def _(__data, *args, **kwargs):
    return create_sym_call(if_else, __data.source, *args, **kwargs)

@if_else.register(pd.Series)
def _(cond, true_vals, false_vals):
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
import itertools
from siuba.siu import DeepCall

    

@singledispatch
def case_when(__data, cases):
    raise Exception("no")


@case_when.register(Symbolic)
def _(__data, cases):
    if not isinstance(cases, dict):
        raise Exception("Cases must be a dictionary")
    dict_entries = dict((strip_symbolic(k), strip_symbolic(v)) for k,v in cases.items())
    cases_arg = DeepCall("__call__", dict, dict_entries)
    return Symbolic(source = Call( "__call__", case_when, __data.source, cases_arg))


@case_when.register(pd.Series)
@case_when.register(pd.DataFrame)
def _(__data, cases):
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


# Count =======================================================================

def _count_group(data, *args):
    crnt_cols = set(data.columns)
    out_col = "n"
    while out_col in crnt_cols: out_col = out_col + "n"

    return 


@Pipeable.add_to_dispatcher
@singledispatch
def count(__data, *args, wt = False, sort = False, **kwargs):
    raise Exception("no")

@count.register(pd.DataFrame)
def _(__data, *args, wt = None, sort = False, **kwargs):
    # TODO: if expr, works like mutate

    #group by args
    if wt is None:
        counts = group_by(__data, *args, **kwargs).size().reset_index()
    else:
        wt_col = simple_varname(strip_symbolic(wt))
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


@singledispatch
def add_count(__data, *args, **kwargs):
    raise Exception("no")

@add_count.register(pd.DataFrame)
def _(__data, *args):
    counts = count(__data, *args)
    return __data.merge(counts, on = list(args))
    


# Tally =======================================================================

# Nest ========================================================================

@Pipeable.add_to_dispatcher
@singledispatch
def nest(__data, *args, **kwargs):
    raise Exception("no")

@nest.register(pd.DataFrame)
def _(__data, *args, key = "data"):
    # TODO: copied from select function
    vl = VarList()
    evaluated = (arg(vl) if callable(arg) else arg for arg in args)
    od = var_select(__data.columns, *evaluated)

    # unselected columns are treated similar to using groupby
    grp_keys = list(set(__data.columns) - set(od))
    nest_keys = list(od)

    # AFAICT you can't name the col created in the apply here
    # but it might be more efficient to act on the groupby obj directly
    out = __data.groupby(grp_keys).apply(lambda d: [d[nest_keys]]).reset_index()
    out.rename(columns = {out.columns[-1]: key}, inplace = True)
    # hack, can assign list of dataframes, but not in a groupby?
    # it ended up making a series of lists each with a single dataframe
    out[key] = [x[0] for x in out[key]]

    return out


# Unnest ======================================================================

@Pipeable.add_to_dispatcher
@singledispatch
def unnest(__data, *args, **kwargs):
    raise Exception("no")

@unnest.register(pd.DataFrame)
def _(__data, key):
    # TODO: currently only takes key, not expressions
    nrows_nested = __data[key].apply(len, convert_dtype = True)
    indx_nested = nrows_nested.index.repeat(nrows_nested)

    grp_keys = list(__data.columns[__data.columns != key])

    out = pd.concat(__data[key].tolist(), ignore_index = True)
    # may be a better approach using a multi-index
    long_grp = __data.loc[indx_nested, grp_keys].reset_index(drop = True)
    
    return out.join(long_grp)


# Joins =======================================================================
from collections.abc import Mapping
from functools import partial

# TODO: will need to use multiple dispatch
@Pipeable.add_to_dispatcher
@singledispatch
def join(left, right, on = None, how = None):
    raise Exception("Unsupported type %s" %type(left))

@join.register(pd.DataFrame)
def _(left, right, on = None, how = None):
    if how is None:
        raise Exception("Must specify how argument")

    if isinstance(on, Mapping):
        left_on, right_on = zip(*on.items())
        return left.merge(right, how = how, left_on = left_on, right_on = right_on)

    return left.merge(right, how = how, on = on)


left_join = partial(join, how = "left")
right_join = partial(join, how = "right")
full_join = partial(join, how = "full")
inner_join = partial(join, how = "inner")



# Head ========================================================================

@Pipeable.add_to_dispatcher
@singledispatch
def head(__data, n):
    raise Exception("no")

@head.register(pd.DataFrame)
def _(__data, n = None):
    return __data.head(n)

# Gather ======================================================================

@Pipeable.add_to_dispatcher
@singledispatch
def gather(__data, key = "key", value = "value", *args, drop_na = False, convert = False):
    # TODO: implement var selection over *args
    if convert:
        raise NotImplementedError("convert not yet implemented")

    # TODO: copied from nest and select
    vl = VarList()
    evaluated = [strip_symbolic(arg)(vl) if callable(arg) else arg for arg in args]
    od = var_select(__data.columns, *evaluated)

    value_vars = list(od) or None

    id_vars = [col for col in __data.columns if col not in od]
    long = pd.melt(__data, id_vars, value_vars, key, value)

    if drop_na:
        return long[~long[value].isna()]

    return long



# Spread ======================================================================

@Pipeable.add_to_dispatcher
@singledispatch
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

# Install Siu =================================================================

install_pd_siu()
