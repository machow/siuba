from functools import singledispatch
from pandas import DataFrame
import pandas as pd
import numpy as np

from pandas.core.groupby import DataFrameGroupBy
from .siu import Symbolic, Call, strip_symbolic


# General TODO ================================================================
# * expressions in group_by
# * distinct
# * dispatch to partial when passed a single _?
# * n_distinct?
# * separate_rows
# * compare gather/spread with melt, cast
# * tally
from functools import reduce

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


def _regroup(df):
    # try to regroup, when user kept index (e.g. group_keys = True)
    if len(df.index.names) > 1:
        # handle cases where...
        # 1. grouping with named indices (as_index = True)
        # 2. grouping is level 0 (as_index = False)
        grp_levels = [x for x in df.index.names if x is not None] or [0]

    return df.groupby(level = grp_levels)



# Mutate ======================================================================

# TODO: support for unnamed args
@Pipeable.add_to_dispatcher
@singledispatch
def mutate(__data, **kwargs):
    raise Exception("no")

@mutate.register(DataFrame)
def _(__data, **kwargs):
    return __data.assign(**kwargs)


@mutate.register(DataFrameGroupBy)
def _(__data, **kwargs):
    df = __data.apply(lambda d: d.assign(**kwargs))
    
    return _regroup(df)



# Group By ====================================================================

@Pipeable.add_to_dispatcher
@singledispatch
def group_by(__data, *args):
    return __data.groupby(by = list(args))


@singledispatch
def ungroup(__data):
    # TODO: can we somehow just restore the original df used to construct
    #       the groupby?
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
        crnt_indx &= arg(__data) if callable(arg) else arg

    return __data.loc[crnt_indx]

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
        res = v(__data) if callable(v) else v

        # TODO: validation?

        results[k] = res
        
    return DataFrame(results, index = [0])

    
@summarize.register(DataFrameGroupBy)
def _(__data, **kwargs):
    df_summarize = summarize.registry[pd.DataFrame]

    df = __data.apply(df_summarize, **kwargs)
        
    return df



# Transmute ===================================================================

@Pipeable.add_to_dispatcher
@singledispatch
def transmute(__data, *args, **kwargs):
    raise Exception("no")

@transmute.register(DataFrame)
def _(__data, *args, **kwargs):
    f_mutate = mutate.registry[pd.DataFrame]

    df = f_mutate(__data, **kwargs) 

    return df[[*args, *kwargs.keys()]]

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
        self.negated = not self.negated
        return self

    def __eq__(self, x):
        x.negated = False
        x.alias = self.name
        return x

    def __repr__(self):
        return "Var('{self.name}', negated = {self.negated}, alias = {self.alias})" \
                    .format(self = self)

    def __str__(self):
        op = "-" if self.negated else ""
        pref = self.alias + " = " if self.alias else ""
        return "{pref}{op}{self.name}".format(pref = pref, op = op, self = self)


class VarList:
    def __getattr__(self, x):
        return Var(x)

    def __getitem__(self, x):
        return x

def var_slice(colnames, x):
    NotImplementedError()


def var_select(colnames, *args):
    cols = OrderedDict()
    everything = None

    # Add entries in pandas.rename style {"orig_name": "new_name"}
    for arg in args:
        if isinstance(arg, str):
            cols[arg] = None
        elif isinstance(arg, int):
            cols[colnames[arg]] = None
        elif not isinstance(arg, Var):
            raise Exception("variable must be either a string or Var instance")
        else:
            # remove negated Vars, others include them
            if arg.negated:
                # first time using negation, apply an implicit everything
                if everything is None:
                    everything = True
                    cols.update((x, None) for x in set(colnames) - set(cols))
                cols.pop(arg.name)

            else: 
                # move to end (e.g. if all columns were added earlier)
                if arg.name in cols:
                    cols.move_to_end(arg.name)

                cols[arg.name] = arg.alias

    return cols


@Pipeable.add_to_dispatcher
@singledispatch
def select(__data, *args, **kwargs):
    raise Exception("no")

@select.register(DataFrame)
def _(__data, *args, **kwargs):
    vl = VarList()
    evaluated = (arg(vl) if callable(arg) else arg for arg in args)

    od = var_select(__data.columns, *evaluated)

    to_rename = {k: v for k,v in od.items() if v is not None}

    return __data[list(od)].rename(columns = to_rename)
    
#    df = __data[[*args, *kwargs.values()]]
#
#    # pandas uses reverse format from dplyr
#    col_names = {v: k for k,v in kwargs.items()}
#
#    return df.rename(columns = col_names)

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
def _(__data, *args, **kwargs):
    raise NotImplementedError("not yet supported; use drop_duplicates")


# if_else
@singledispatch
def if_else(__data, *args, **kwargs):
    raise Exception("no")

@if_else.register(pd.Series)
def _(cond, true_vals, false_vals):
    true_indx = np.where(cond)[0]
    false_indx = np.where(~cond)[0]

    result = np.repeat(None, len(cond))
    result[true_indx] = true_vals
    result[false_indx] = false_vals

    # TODO: inefficient way to downcast?
    return np.array(list(result))


# case_when
@singledispatch
def case_when(__data, *args, **kwargs):
    raise Exception("no")

@case_when.register(pd.Series)
@case_when.register(pd.DataFrame)
def _(__data, cases):
    # TODO: handle when receive list of (k,v) pairs for py < 3.5 compat?
    out = np.repeat(None, len(__data))
    for k,v in reversed(list(cases.items())):
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
def count(__data, *args, **kwargs):
    raise Exception("no")

@count.register(pd.DataFrame)
def _(__data, *args, sort = False):
    # if expr, works like mutate

    # count col named, n. If that col already exists, add more "n"s...
    crnt_cols = set(__data.columns)
    out_col = "n"
    while out_col in crnt_cols: out_col = out_col + "n"

    #group by args
    counts = __data.groupby(list(args)).size().reset_index()
    counts.rename(columns = {counts.columns[-1]: "n"}, inplace = True)

    # .size
    # ungroup
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



