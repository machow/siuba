from functools import singledispatch, wraps
from pandas import DataFrame

import pandas as pd
import numpy as np
import warnings


from pandas.core.groupby import DataFrameGroupBy
from pandas.core.dtypes.inference import is_scalar
from siuba.siu import (
    Symbolic, Call, strip_symbolic, create_sym_call, 
    MetaArg, BinaryOp, _SliceOpIndex, Lazy,
    singledispatch2, pipe_no_args, Pipeable, pipe
    )

from .tidyselect import var_create, var_select, Var

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
        "collect", "show_query",
        "tbl",
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
    DataFrameGroupBy.__repr__ = _repr_grouped_df_console_

def _repr_grouped_df_html_(self):
    obj_repr = self.obj._repr_html_()
    
    # user can config pandas not to return html representation, in which case
    # the ipython behavior should fall back to repr
    if obj_repr is None:
        return None

    return "<div><p>(grouped data frame)</p>" + self.obj._repr_html_() + "</div>"

def _repr_grouped_df_console_(self):
    return "(grouped data frame)\n" + repr(self.obj)


def _bounce_groupby(f):
    @wraps(f)
    def wrapper(__data: "pd.DataFrame | DataFrameGroupBy", *args, **kwargs):
        if isinstance(__data, pd.DataFrame):
            return f(__data, *args, **kwargs)

        groupings = __data.grouper.groupings
        group_cols = [ping.name for ping in groupings]

        res = f(__data.obj, *args, **kwargs)

        return res.groupby(group_cols)

    return wrapper


def _regroup(df):
    # try to regroup after an apply, when user kept index (e.g. group_keys = True)
    if len(df.index.names) > 1:
        # handle cases where...
        # 1. grouping with named indices (as_index = True)
        # 2. grouping is level 0 (as_index = False)
        grp_levels = [x for x in df.index.names if x is not None] or [0]

    return df.groupby(level = grp_levels)


def _mutate_cols(__data, args, kwargs):
    from pandas.core.common import apply_if_callable

    result_names = {}          # used as ordered set
    df_tmp = __data.copy()

    for arg in args:

        # case 1: a simple, existing name is a no-op ----
        simple_name = simple_varname(arg)
        if simple_name is not None and simple_name in df_tmp.columns:
            result_names[simple_name] = True
            continue

        # case 2: across ----
        # TODO: make robust. validate input. validate output (e.g. shape).
        res_arg = arg(df_tmp)

        if not isinstance(res_arg, pd.DataFrame):
            raise NotImplementedError("Only across() can be used as positional argument.")

        for col_name, col_ser in res_arg.items():
            # need to put on the frame so subsequent args, kwargs can use
            df_tmp[col_name] = col_ser
            result_names[col_name] = True

    for col_name, expr in kwargs.items():
        # this is exactly what DataFrame.assign does
        df_tmp[col_name] = apply_if_callable(expr, df_tmp)
        result_names[col_name] = True

    return list(result_names), df_tmp


def _make_groupby_safe(gdf):
    return gdf.obj.groupby(gdf.grouper, group_keys=False)


MSG_TYPE_ERROR = "The first argument to {func} must be one of: {types}"

def raise_type_error(f):
    raise TypeError(MSG_TYPE_ERROR.format(
                func = f.__name__,
                types = ", ".join(map(str, f.registry.keys()))
                ))


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


# Collect and show_query =========

@pipe_no_args
@singledispatch2((DataFrame, DataFrameGroupBy))
def collect(__data, *args, **kwargs):
    """Retrieve data as a local DataFrame.
    
    """
    # simply return DataFrame, since requires no execution
    return __data


@pipe_no_args
@singledispatch2((DataFrame, DataFrameGroupBy))
def show_query(__data, simplify = False):
    """Print the details of a query.

    Parameters
    ----------
    __data:
        A DataFrame of siuba.sql.LazyTbl.
    simplify:
        Whether to attempt to simplify the query.
    **kwargs:
        Additional arguments passed to specific implementations.

    """
    print("No query to show for a DataFrame")
    return __data

# Mutate ======================================================================


@singledispatch2(pd.DataFrame)
def mutate(__data, *args, **kwargs):
    """Assign new variables to a DataFrame, while keeping existing ones.

    Parameters
    ----------
    __data: pd.DataFrame
    **kwargs:
        new_col_name=value pairs, where value can be a function taking a singledispatch2
        argument for the data being operated on.

    See Also
    --------
    transmute : Returns a DataFrame with only the newly created columns.

    Examples
    --------
    >>> from siuba import _, mutate, head
    >>> from siuba.data import cars
    >>> cars >> mutate(cyl2 = _.cyl * 2, cyl4 = _.cyl2 * 2) >> head(2)
       cyl   mpg   hp  cyl2  cyl4
    0    6  21.0  110    12    24
    1    6  21.0  110    12    24
        
    """

    new_names, df_res = _mutate_cols(__data, args, kwargs)
    return df_res


@mutate.register(DataFrameGroupBy)
def _mutate(__data, *args, **kwargs):
    out = __data.obj.copy()
    groupings = {ping.name: ping for ping in __data.grouper.groupings}

    f_transmute = transmute.dispatch(pd.DataFrame)

    df = _make_groupby_safe(__data).apply(lambda d: f_transmute(d, *args, **kwargs))

    for varname, ser in df.items():
        if varname in groupings:
            groupings[varname] = varname

        out[varname] = ser

    return out.groupby(list(groupings.values()))


# Group By ====================================================================

@singledispatch2((pd.DataFrame, DataFrameGroupBy))
def group_by(__data, *args, add = False, **kwargs):
    """Return a grouped DataFrame, using columns or expressions to define groups.

    Any operations (e.g. summarize, mutate, filter) performed on grouped data
    will be performed "by group". Use `ungroup()` to remove the groupings.

    Parameters
    ----------
    __data:
        The data being grouped.
    *args:
        Lazy expressions used to select the grouping columns. Currently, each
        arg must refer to a single columns (e.g. _.cyl, _.mpg).
    add: bool
        If the data is already grouped, whether to add these groupings on top of those.
    **kwargs:
        Keyword arguments define new columns used to group the data.


    Examples
    --------

    >>> from siuba import _, group_by, summarize, filter, mutate, head
    >>> from siuba.data import cars

    >>> by_cyl = cars >> group_by(_.cyl)

    >>> by_cyl >> summarize(max_mpg = _.mpg.max(), max_hp = _.hp.max())
       cyl  max_mpg  max_hp
    0    4     33.9     113
    1    6     21.4     175
    2    8     19.2     335

    >>> by_cyl >> filter(_.mpg == _.mpg.max())
    (grouped data frame)
        cyl   mpg   hp
    3     6  21.4  110
    19    4  33.9   65
    24    8  19.2  175

    >>> cars >> group_by(cyl2 = _.cyl + 1) >> head(2)
    (grouped data frame)
       cyl   mpg   hp  cyl2
    0    6  21.0  110     7
    1    6  21.0  110     7

    Note that creating the new grouping column is always performed on ungrouped data.
    Use an explicit mutate on the grouped data perform the operation within groups.

    For example, the code below calls pd.cut on the mpg column, within each cyl group.

    >>> from siuba.siu import call
    >>> (cars
    ...     >> group_by(_.cyl)
    ...     >> mutate(mpg_bin = call(pd.cut, _.mpg, 3))
    ...     >> group_by(_.mpg_bin, add=True)
    ...     >> head(2)
    ... )
    (grouped data frame)
       cyl   mpg   hp       mpg_bin
    0    6  21.0  110  (20.2, 21.4]
    1    6  21.0  110  (20.2, 21.4]
    
    """
    
    if isinstance(__data, DataFrameGroupBy):
        tmp_df = __data.obj.copy()
    else:
        tmp_df = __data.copy()

    # TODO: super inefficient, since it makes multiple copies of data
    #       need way to get the by_vars and apply (grouped) computation
    computed = transmute(tmp_df, *args, **kwargs)
    by_vars = list(computed.columns)

    for k in by_vars:
        tmp_df[k] = computed[k]

    if isinstance(__data, DataFrameGroupBy) and add:
        groupings = {el.name: el for el in __data.grouper.groupings}

        for varname in by_vars:
            # ensures group levels are recalculated if varname was in transmute
            groupings[varname] = varname

        return tmp_df.groupby(list(groupings.values()))

    return tmp_df.groupby(by = by_vars)


@singledispatch2((pd.DataFrame, DataFrameGroupBy))
def ungroup(__data):
    """Return an ungrouped DataFrame.

    Parameters
    ----------
    __data:
        The data being ungrouped.

    Examples
    --------
    >>> from siuba import _, group_by, ungroup
    >>> from siuba.data import cars

    >>> g_cyl = cars.groupby("cyl")
    >>> res1 = ungroup(g_cyl)

    >>> res2 = cars >> group_by(_.cyl) >> ungroup()
    """
    # TODO: can we somehow just restore the original df used to construct
    #       the groupby?
    if isinstance(__data, pd.DataFrame):
        return __data
    elif isinstance(__data, DataFrameGroupBy):
        return __data.obj
    else:
        raise TypeError(f"Unsupported type {type(__data)}")



# Filter ======================================================================

@singledispatch2(pd.DataFrame)
def filter(__data, *args):
    """Keep rows where conditions are true.

    Parameters
    ----------
    __data:
        The data being filtered.
    *args:
        conditions that must be met to keep a column. 

    Examples
    --------

    >>> from siuba import _, filter
    >>> from siuba.data import cars
    
    Keep rows where cyl is 4 *and* mpg is less than 25.

    >>> cars >> filter(_.cyl ==  4, _.mpg < 22) 
        cyl   mpg   hp
    20    4  21.5   97
    31    4  21.4  109

    Use `|` to represent an OR condition. For example, the code below keeps
    rows where hp is over 250 *or* mpg is over 32.

    >>> cars >> filter((_.hp > 300) | (_.mpg > 32))
        cyl   mpg   hp
    17    4  32.4   66
    19    4  33.9   65
    30    8  15.0  335

    """
    crnt_indx = True
    for arg in args:
        res = arg(__data) if callable(arg) else arg

        if isinstance(res, pd.DataFrame):
            crnt_indx &= res.all(axis=1)
        elif isinstance(res, pd.Series):
            crnt_indx &= res
        else:
            crnt_indx &= res

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
def summarize(__data, *args, **kwargs):
    """Assign variables that are single number summaries of a DataFrame.

    Grouped DataFrames will produce one row for each group. Otherwise, summarize
    produces a DataFrame with a single row.

    Parameters
    ----------
    __data: a DataFrame
        The data being summarized.
    **kwargs:
        new_col_name=value pairs, where value can be a function taking
        a single argument for the data being operated on.


    Examples
    --------
    >>> from siuba import _, group_by, summarize
    >>> from siuba.data import cars

    >>> cars >> summarize(avg = _.mpg.mean(), n = _.shape[0])
             avg   n
    0  20.090625  32

    >>> g_cyl = cars >> group_by(_.cyl)
    >>> g_cyl >> summarize(min = _.mpg.min())
       cyl   min
    0    4  21.4
    1    6  17.8
    2    8  10.4

    >>> g_cyl >> summarize(mpg_std_err = _.mpg.std() / _.shape[0]**.5)
       cyl  mpg_std_err
    0    4     1.359764
    1    6     0.549397
    2    8     0.684202
        
    """
    results = {}
    
    for ii, expr in enumerate(args):
        if not callable(expr):
            raise TypeError(
                "Unnamed arguments to summarize must be callable, but argument number "
                f"{ii} was type: {type(expr)}"
            )

        res = expr(__data)
        if isinstance(res, DataFrame):
            if len(res) != 1:
                raise ValueError(
                    f"Summarize argument `{ii}` returned a DataFrame with {len(res)} rows."
                    " Result must only be a single row."
                )

            for col_name in res.columns:
                results[col_name] = res[col_name].array
        else:
            raise ValueError(
                "Unnamed arguments to summarize must return a DataFrame, but argument "
                f"`{ii} returned type: {type(expr)}"
            )



    for k, v in kwargs.items():
        # TODO: raise error if a named expression returns a DataFrame
        res = v(__data) if callable(v) else v

        if is_scalar(res) or len(res) == 1:
            # keep result, but use underlying array to avoid crazy index issues
            # on DataFrame construction (#138)
            results[k] = res.array if isinstance(res, pd.Series) else res

        else:
            raise ValueError(
                f"Summarize argument `{k}` must return result of length 1 or a scalar.\n\n"
                f"Result type: {type(res)}\n"
                f"Result length: {len(res)}"
            )
        
    # must pass index, or raises error when using all scalar values
    return DataFrame(results, index = [0])

    
@summarize.register(DataFrameGroupBy)
def _summarize(__data, *args, **kwargs):
    df_summarize = summarize.registry[pd.DataFrame]

    df = __data.apply(df_summarize, *args, **kwargs)
        
    group_by_lvls = list(range(df.index.nlevels - 1))
    out = df.reset_index(group_by_lvls)
    out.index = pd.RangeIndex(df.shape[0])

    return out



# Transmute ===================================================================

@singledispatch2(DataFrame)
def transmute(__data, *args, **kwargs):
    """Assign new columns to a DataFrame, while dropping previous columns.

    Parameters
    ----------
    __data:
        The input data.
    **kwargs:
        Each keyword argument is the name of a new column, and an expression.

    See Also
    --------
    mutate : Assign new columns, or modify existing ones.

    Examples
    --------

    >>> from siuba import _, transmute, mutate, head
    >>> from siuba.data import cars

    Notice that transmute results in a table with only the new column:

    >>> cars >> transmute(cyl2 = _.cyl + 1) >> head(2)
       cyl2
    0     7
    1     7

    By contrast, mutate adds the new column to the end of the table:

    >>> cars >>  mutate(cyl2 = _.cyl + 1) >> head(2)
       cyl   mpg   hp  cyl2
    0    6  21.0  110     7
    1    6  21.0  110     7
    

    """
    arg_vars = list(map(simple_varname, args))

    col_names, df_res = _mutate_cols(__data, args, kwargs)
    return df_res[col_names]


@transmute.register(DataFrameGroupBy)
def _transmute(__data, *args, **kwargs):
    groupings = {ping.name: ping for ping in __data.grouper.groupings}

    f_transmute = transmute.dispatch(pd.DataFrame)

    df = _make_groupby_safe(__data).apply(lambda d: f_transmute(d, *args, **kwargs))

    
    for varname in reversed(list(groupings)):
        if varname in df.columns:
            groupings[varname] = varname
        else:
            df.insert(0, varname, __data.obj[varname])

    return df.groupby(list(groupings.values()))



# Select ======================================================================

def _insert_missing_groups(dst, orig, missing_groups):
    if missing_groups:
        warnings.warn(f"Adding missing grouping variables: {missing_groups}")

        for ii, colname in enumerate(missing_groups):
            dst.insert(ii, colname, orig[colname])


def _select_group_renames(selection: dict, group_cols):
    """Returns a 2-tuple: groups missing in the select, new group keys."""
    renamed = {k: v for k,v in selection.items() if v is not None}

    sel_groups = [
        renamed[colname] or colname for colname in group_cols if colname in renamed
    ]
    missing_groups = [colname for colname in group_cols if colname not in selection]

    return missing_groups, (*missing_groups, *sel_groups)


@singledispatch2(DataFrame)
def select(__data, *args, **kwargs):
    """Select columns of a table to keep or drop (and optionally rename).

    Parameters
    ----------
    __data:
        The input table.
    *args: 
        An expression specifying columns to keep or drop. 
    **kwargs:
        Not implemented.

    Examples
    --------
    >>> from siuba import _, select
    >>> from siuba.data import cars

    >>> small_cars = cars.head(1)
    >>> small_cars
       cyl   mpg   hp
    0    6  21.0  110

    You can refer to columns by name or position.

    >>> small_cars >> select(_.cyl, _[2])
       cyl   hp
    0    6  110

    Use a `~` sign to exclude a column.

    >>> small_cars >> select(~_.cyl)
        mpg   hp
    0  21.0  110

    You can use any methods you'd find on the .columns.str accessor:

    >>> small_cars.columns.str.contains("p")
    array([False,  True,  True])

    >>> small_cars >> select(_.contains("p"))
        mpg   hp
    0  21.0  110

    Use a slice to select a range of columns:

    >>> small_cars >> select(_[0:2])
       cyl   mpg
    0    6  21.0

    Multiple expressions can be combined using _[a, b, c] syntax. This is useful
    for dropping a complex set of matches.

    >>> small_cars >> select(~_[_.startswith("c"), -1])
        mpg
    0  21.0

    """

    if kwargs:
        raise NotImplementedError(
                "Using kwargs in select not currently supported. "
                "Use _.newname == _.oldname instead"
                )
    var_list = var_create(*args)

    od = var_select(__data.columns, *var_list, data=__data)

    to_rename = {k: v for k,v in od.items() if v is not None}

    return __data[list(od)].rename(columns = to_rename)
    

@select.register(DataFrameGroupBy)
def _select(__data, *args, **kwargs):
    # tidyselect
    var_list = var_create(*args)
    od = var_select(__data.obj.columns, *var_list)

    group_cols = [ping.name for ping in __data.grouper.groupings]

    res = select(__data.obj, *args, **kwargs)

    missing_groups, group_keys = _select_group_renames(od, group_cols)
    _insert_missing_groups(res, __data.obj, missing_groups)

    return res.groupby(list(group_keys))


# Rename ======================================================================

@singledispatch2(DataFrame)
def rename(__data, **kwargs):
    """Rename columns of a table.

    Parameters
    ----------
    __data:
        The input table.
    **kwargs:
        Keyword arguments of the form new_name = _.old_name, or new_name = "old_name".

    Examples
    --------

    >>> import pandas as pd
    >>> from siuba import _, rename, select

    >>> df = pd.DataFrame({"zzz": [1], "b": [2]})
    >>> df >> rename(a = _.zzz)
       a  b
    0  1  2

    Note that this is equivalent to this select code:

    >>> df >> select(_.a == _.zzz, _.b)
       a  b
    0  1  2

    """
    # TODO: allow names with spaces, etc..
    col_names = {simple_varname(v):k for k,v in kwargs.items()}
    if None in col_names:
        raise ValueError("Rename needs column name (e.g. 'a' or _.a), but received %s"%col_names[None])

    return __data.rename(columns  = col_names)


@rename.register(DataFrameGroupBy)
def _rename(__data, **kwargs):
    col_names = {simple_varname(v):k for k,v in kwargs.items()}
    group_cols = [ping.name for ping in __data.grouper.groupings]

    res = rename(__data.obj, **kwargs)

    missing_groups, group_keys = _select_group_renames(col_names, group_cols)

    return res.groupby(list(group_keys))


# Arrange =====================================================================

def _call_strip_ascending(f):
    if isinstance(f, Symbolic):
        f = strip_symbolic(f)

    if isinstance(f, Call) and f.func == "__neg__":
        return f.args[0], False

    return f, True

@singledispatch2(DataFrame)
def arrange(__data, *args):
    """Re-order the rows of a DataFrame using the values of specified columns.

    Parameters
    ----------
    __data:
        The input table.
    *args:
        Columns or expressions used to sort the rows.

    Examples
    --------

    >>> import pandas as pd
    >>> from siuba import _, arrange, mutate

    >>> df = pd.DataFrame({"x": [2, 1, 1], "y": ["aa", "b", "aa"]})
    >>> df
       x   y
    0  2  aa
    1  1   b
    2  1  aa

    Arrange sorts on the first argument, then the second, etc..

    >>> df >> arrange(_.x, _.y)
       x   y
    2  1  aa
    1  1   b
    0  2  aa

    Use a minus sign (`-`) to sort is descending order.

    >>> df >> arrange(-_.x)
       x   y
    0  2  aa
    1  1   b
    2  1  aa

    Note that arrange can sort on complex expressions:

    >>> df >> arrange(-_.y.str.len())
       x   y
    0  2  aa
    2  1  aa
    1  1   b

    The case above is equivalent to running a mutate before arrange:

    >>> df >> mutate(res = -_.y.str.len()) >> arrange(_.res)
       x   y  res
    0  2  aa   -2
    2  1  aa   -2
    1  1   b   -1

    """
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

            res = f(df)

            if isinstance(res, pd.DataFrame):
                raise NotImplementedError(
                    f"`arrange()` expression {ii} of {len(args)} returned a "
                    "DataFrame, which is currently unsupported."
                )

            df[n_cols + ii] = res


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
    """Keep only distinct (unique) rows from a table.

    Parameters
    ----------
    __data:
        The input data.
    *args:
        Columns to use when determining which rows are unique.
    _keep_all:
        Whether to keep all columns of the original data, not just *args.
    **kwargs:
        If specified, arguments passed to the verb mutate(), and then being used
        in distinct().

    See Also
    --------
    count : keep distinct rows, and count their number of observations.

    Examples
    --------
    >>> from siuba import _, distinct, select
    >>> from siuba.data import penguins

    >>> penguins >> distinct(_.species, _.island)
         species     island
    0     Adelie  Torgersen
    1     Adelie     Biscoe
    2     Adelie      Dream
    3     Gentoo     Biscoe
    4  Chinstrap      Dream

    Use _keep_all=True, to keep all columns in each distinct row. This lets you
    peak at the values of the first unique row.

    >>> small_penguins = penguins >> select(_[:4])
    >>> small_penguins >> distinct(_.species, _keep_all = True)
         species     island  bill_length_mm  bill_depth_mm
    0     Adelie  Torgersen            39.1           18.7
    1     Gentoo     Biscoe            46.1           13.2
    2  Chinstrap      Dream            46.5           17.9
    """

    if not (args or kwargs):
        return __data.drop_duplicates().reset_index(drop=True)

    new_names, df_res = _mutate_cols(__data, args, kwargs)
    tmp_data = df_res.drop_duplicates(new_names).reset_index(drop=True)

    if not _keep_all:
        return tmp_data[new_names]

    return tmp_data


@distinct.register(DataFrameGroupBy)
def _distinct(__data, *args, _keep_all = False, **kwargs):

    group_names = [ping.name for ping in __data.grouper.groupings]


    f_distinct = distinct.dispatch(type(__data.obj))

    tmp_data = (__data
        .apply(f_distinct, *args, _keep_all=_keep_all, **kwargs)
    )

    index_keys = tmp_data.index.names[:-1]
    keys_to_drop = [k for k in index_keys if k in tmp_data.columns]
    keys_to_keep = [k for k in index_keys if k not in tmp_data.columns]

    final = tmp_data.reset_index(keys_to_drop, drop=True).reset_index(keys_to_keep)

    return final.groupby(group_names)


# if_else, case_when ==========================================================

# TODO: move to vector.py
@singledispatch
def if_else(condition, true, false):
    """
    Parameters
    ----------
    condition:
        Logical vector (or lazy expression).
    true:
        Values to be used when condition is True.
    false:
        Values to be used when condition is False.

    See Also
    --------
    case_when : Generalized if_else, for handling many cases.
        
    Examples
    --------
    >>> ser1 = pd.Series([1,2,3])
    >>> if_else(ser1 > 2, np.nan, ser1)
    0    1.0
    1    2.0
    2    NaN
    dtype: float64

    >>> from siuba import _
    >>> f = if_else(_ < 2, _, 2)
    >>> f(ser1)
    0    1
    1    2
    2    2
    dtype: int64

    >>> import numpy as np
    >>> ser2 = pd.Series(['NA', 'a', 'b'])
    >>> if_else(ser2 == 'NA', np.nan, ser2)
    0    NaN
    1      a
    2      b
    dtype: object

    """
    raise_type_error(condition)

@if_else.register(Call)
@if_else.register(Symbolic)
def _if_else(condition, true, false):
    return create_sym_call(if_else, condition, true, false)

@if_else.register(pd.Series)
def _if_else(condition, true, false):
    result = np.where(condition.fillna(False), true, false)

    return pd.Series(result)


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
def case_when(__data, cases: dict):
    """Generalized, vectorized if statement.

    Parameters
    ----------
    __data:
        The input data.
    cases: dict
        A mapping of condition : value.

    See Also
    --------
    if_else : Handles the special case of two conditions.
        
    Examples
    --------
    >>> import pandas as pd
    >>> from siuba import _, case_when

    >>> df = pd.DataFrame({"x": [1, 2, 3]})
    >>> case_when(df, {_.x == 1: "one", _.x == 2: "two"})
    0     one
    1     two
    2    None
    dtype: object

    >>> df >> case_when({_.x == 1: "one", _.x == 2: "two"})
    0     one
    1     two
    2    None
    dtype: object

    >>> df >> case_when({_.x == 1: "one", _.x == 2: "two", True: "other"})
    0      one
    1      two
    2    other
    dtype: object


    """
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
    return pd.Series(list(out))

@case_when.register(Symbolic)
@case_when.register(Call)
def _case_when(__data, cases):
    if not isinstance(cases, dict):
        raise Exception("Cases must be a dictionary")
    dict_entries = dict((strip_symbolic(k), strip_symbolic(v)) for k,v in cases.items())
    cases_arg = Lazy(DictCall("__call__", dict, dict_entries))
    return create_sym_call(case_when, __data, cases_arg)




# Count =======================================================================

@singledispatch2((pd.DataFrame, DataFrameGroupBy))
def count(__data, *args, wt = None, sort = False, name=None, **kwargs):
    """Summarize data with the number of rows for each grouping of data.

    Parameters
    ----------
    __data:
        A DataFrame.
    *args:
        The names of columns to be used for grouping. Passed to group_by.
    wt:
        The name of a column to use as a weighted for each row.
    sort:
        Whether to sort the results in descending order.
    **kwargs:
        Creates a new named column, and uses for grouping. Passed to group_by.

    Examples
    --------

    >>> from siuba import _, count, group_by, summarize, arrange
    >>> from siuba.data import mtcars

    >>> count(mtcars, _.cyl, high_mpg = _.mpg > 30)
       cyl  high_mpg   n
    0    4     False   7
    1    4      True   4
    2    6     False   7
    3    8     False  14

    Use sort to order results by number of observations (in descending order).

    >>> count(mtcars, _.cyl, sort=True)
       cyl   n
    0    8  14
    1    4  11
    2    6   7

    count is equivalent to doing a grouped summarize:

    >>> mtcars >> group_by(_.cyl) >> summarize(n = _.shape[0]) >> arrange(-_.n)
       cyl   n
    2    8  14
    0    4  11
    1    6   7


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
    out_col = _check_name(name, set(counts.columns))

    # rename the tally column to correct name
    counts.rename(columns = {counts.columns[-1]: out_col}, inplace = True)

    if sort:
        return counts.sort_values(out_col, ascending = False).reset_index(drop = True)

    return counts


def _check_name(name, columns):
    if name is None:
        name = "n"
        while name in columns:
            name = name + "n"

    elif name != "n" and name in columns:
        raise ValueError(
            f"Column name `{name}` specified for count name, but is already present in data."
        )
    
    elif not isinstance(name, str):
        raise TypeError("`name` must be a single string.")

    return name
        

@singledispatch2((pd.DataFrame, DataFrameGroupBy))
def add_count(__data, *args, wt = None, sort = False, name = None, **kwargs):
    """Add a column that is the number of observations for each grouping of data.

    Note that this function is similar to count(), but does not aggregate. It's
    useful combined with filter().

    Parameters
    ----------
    __data:
        A DataFrame.
    *args:
        The names of columns to be used for grouping. Passed to group_by.
    wt:
        The name of a column to use as a weighted for each row.
    sort:
        Whether to sort the results in descending order.
    **kwargs:
        Creates a new named column, and uses for grouping. Passed to group_by.

    Examples
    --------
    >>> import pandas as pd
    >>> from siuba import _, add_count, group_by, ungroup, mutate
    >>> from siuba.data import mtcars

    >>> df = pd.DataFrame({"x": ["a", "a", "b"], "y": [1, 2, 3]})
    >>> df >> add_count(_.x)
       x  y  n
    0  a  1  2
    1  a  2  2
    2  b  3  1

    This is useful if you want to see data associated with some count:

    >>> df >> add_count(_.x) >> filter(_.n == 1)
       x  y  n
    2  b  3  1

    Note that add_count is equivalent to a grouped mutate:

    >>> df >> group_by(_.x) >> mutate(n = _.shape[0]) >> ungroup()
       x  y  n
    0  a  1  2
    1  a  2  2
    2  b  3  1


    """

    no_grouping_vars = not args and not kwargs and isinstance(__data, pd.DataFrame)

    if no_grouping_vars:
        out = __data
    else:
        out = group_by(__data, *args, add=True, **kwargs)

    var_names = ungroup(out).columns
    name = _check_name(name, set(var_names))

    if wt is None:
        if no_grouping_vars: 
            # no groups, just use number of rows
            counts = __data.copy()
            counts[name] = counts.shape[0]
        else:
            # note that it's easy to transform tally using single grouped column, so
            # we arbitrarily grab the first column..
            counts = out.obj.copy()
            counts[name] = out[var_names[0]].transform("size")

    else:
        wt_col = simple_varname(wt)
        if wt_col is None:
            raise Exception("wt argument has to be simple column name")

        if no_grouping_vars:
            # no groups, sum weights
            counts = __data.copy()
            counts[name] = counts[wt_col].sum()
        else:
            # TODO: should flip topmost if/else so grouped code is together
            # do weighted tally
            counts = out.obj.copy()
            counts[name] = out[wt_col].transform("sum")

    if sort:
        return counts.sort_values(out_col, ascending = False)

    return counts

    


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
    

    Parameters
    ----------
    __data:
        A DataFrame.
    *args:
        The names of columns to be nested. May use any syntax used by the
        `select` function.
    key:
        The name of the column that will hold the nested columns.

    Examples
    --------

    >>> from siuba import _, nest
    >>> from siuba.data import cars
    >>> nested_cars = cars >> nest(-_.cyl)

    Note that pandas with nested DataFrames looks okay in juypter notebooks,
    but has a weird representation in the IPython console, so the example below
    shows that each entry in the data column is a DataFrame.

    >>> nested_cars.shape
    (3, 2)

    >>> type(nested_cars.data[0])
    <class 'pandas.core.frame.DataFrame'>
        
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

    # TODO: iterating over splitter now only produces 1 item (the dataframe)
    # check backwards compat
    def _extract_subdf_pandas_1_3(entry):
        # in pandas < 1.3, splitter.__iter__ returns tuple entries (ii, df)
        if isinstance(entry, tuple):
            return entry[1]

        # in pandas 1.3, each entry is just the dataframe
        return entry

    result_index = g_df.grouper.result_index
    nested_dfs = [_extract_subdf_pandas_1_3(x) for x in splitter]

    out = pd.DataFrame({key: nested_dfs}, index = result_index).reset_index()

    return out

@nest.register(DataFrameGroupBy)
def _nest(__data, *args, key = "data"):
    from siuba.dply.tidyselect import VarAnd

    grp_keys = [x.name for x in __data.grouper.groupings]
    if None in grp_keys:
        raise NotImplementedError("All groupby variables must be named when using nest")

    sel_vars = var_create(*grp_keys)
    return nest(__data.obj, -VarAnd(sel_vars), *args, key = key)




# Unnest ======================================================================

@singledispatch2(pd.DataFrame)
def unnest(__data, key = "data"):
    """Unnest a column holding nested data (e.g. Series of lists or DataFrames).
    
    Parameters
    ----------
    ___data:
        A DataFrame.
    key:
        The name of the column to be unnested.

    Examples
    --------

    >>> import pandas as pd
    >>> df = pd.DataFrame({'id': [1,2], 'data': [['a', 'b'], ['c']]})
    >>> df >> unnest()
       id data
    0   1    a
    1   1    b
    2   2    c
        
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
@singledispatch2((pd.DataFrame, DataFrameGroupBy))
@_bounce_groupby
def join(left, right, on = None, how = None, *args, by = None, **kwargs):
    """Join two tables together, by matching on specified columns.

    The functions inner_join, left_join, right_join, and full_join are provided
    as wrappers around join, and are used in the examples.

    Parameters
    ----------
    left :
        The left-hand table.
    right :
        The right-hand table.
    on :
        How to match them. Note that the keyword "by" can also be used for this
        parameter, in order to support compatibility with dplyr.
    how :
        The type of join to perform (inner, full, left, right).
    *args:
        Additional postition arguments. Currently not supported.
    **kwargs:
        Additional keyword arguments. Currently not supported.


    Returns
    -------
    pd.DataFrame

    Examples
    --------

    >>> from siuba import _, inner_join, left_join, full_join, right_join
    >>> from siuba.data import band_members, band_instruments, band_instruments2
    >>> band_members
       name     band
    0  Mick   Stones
    1  John  Beatles
    2  Paul  Beatles

    >>> band_instruments
        name   plays
    0   John  guitar
    1   Paul    bass
    2  Keith  guitar

    Notice that above, only John and Paul have entries for band instruments.
    This means that they will be the only two rows in the inner_join result:

    >>> band_members >> inner_join(_, band_instruments)
       name     band   plays
    0  John  Beatles  guitar
    1  Paul  Beatles    bass

    A left join ensures all original rows of the left hand data are included.

    >>> band_members >> left_join(_, band_instruments)
       name     band   plays
    0  Mick   Stones     NaN
    1  John  Beatles  guitar
    2  Paul  Beatles    bass

    A full join is similar, but ensures all rows of both data are included.

    >>> band_members >> full_join(_, band_instruments)
        name     band   plays
    0   Mick   Stones     NaN
    1   John  Beatles  guitar
    2   Paul  Beatles    bass
    3  Keith      NaN  guitar

    You can explicilty specify columns to join on using the "by" argument:

    >>> band_members >> inner_join(_, band_instruments, by = "name")
       n...

    Use a dictionary for the by argument, to match up columns with different names:

    >>> band_members >> full_join(_, band_instruments2, {"name": "artist"})
       n...
 
    Joins create a new row for each pair of matches. For example, the value 1
    is in two rows on the left, and 2 rows on the right so 4 rows will be created.

    >>> df1 = pd.DataFrame({"x": [1, 1, 3]})
    >>> df2 = pd.DataFrame({"x": [1, 1, 2], "y": ["first", "second", "third"]})
    >>> df1 >> left_join(_, df2)
       x       y
    0  1   first
    1  1  second
    2  1   first
    3  1  second
    4  3     NaN

    Missing values count as matches to eachother by default:


    >>> df3 = pd.DataFrame({"x": [1, None], "y": 2})
    >>> df4 = pd.DataFrame({"x": [1, None], "z": 3})
    >>> left_join(df3, df4)
         x  y  z
    0  1.0  2  3
    1  NaN  2  3

    """

    if isinstance(right, DataFrameGroupBy):
        right = right.obj
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


@singledispatch2((pd.DataFrame, DataFrameGroupBy))
@_bounce_groupby
def semi_join(left, right = None, on = None, *args, by = None):
    """Return the left table with every row that would be kept in an inner join.

    Parameters
    ----------
    left :
        The left-hand table.
    right :
        The right-hand table.
    on :
        How to match them. By default it uses matches all columns with the same
        name across the two tables.

    Examples
    --------
    >>> import pandas as pd
    >>> from siuba import _, semi_join, anti_join

    >>> df1 = pd.DataFrame({"id": [1, 2, 3], "x": ["a", "b", "c"]})
    >>> df2 = pd.DataFrame({"id": [2, 3, 3], "y": ["l", "m", "n"]})

    >>> df1 >> semi_join(_, df2)
       id  x
    1   2  b
    2   3  c

    >>> df1 >> anti_join(_, df2)
       id  x
    0   1  a

    Generally, it's a good idea to explicitly specify the on argument.

    >>> df1 >> anti_join(_, df2, on="id")
       id  x
    0   1  a
    """

    if on is None and by is not None:
        on = by

    if isinstance(on, Mapping):
        # coerce colnames to list, to avoid indexing with tuples
        on_cols, right_on = map(list, zip(*on.items()))
        right = right[right_on].rename(dict(zip(right_on, on_cols)))
    elif on is None:
        warnings.warn(
            "No on column passed to join. "
            "Inferring join columns instead using shared column names."
        )

        on_cols = list(set(left.columns).intersection(set(right.columns)))
        if not len(on_cols):
            raise Exception("No join column specified, and no shared column names")

        warnings.warn("Detected shared columns: %s" % on_cols)
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


@singledispatch2((pd.DataFrame, DataFrameGroupBy))
@_bounce_groupby
def anti_join(left, right = None, on = None, *args, by = None):
    """Return the left table with every row that would *not* be kept in an inner join.

    Parameters
    ----------
    left :
        The left-hand table.
    right :
        The right-hand table.
    on :
        How to match them. By default it uses matches all columns with the same
        name across the two tables.

    Examples
    --------
    >>> import pandas as pd
    >>> from siuba import _, semi_join, anti_join

    >>> df1 = pd.DataFrame({"id": [1, 2, 3], "x": ["a", "b", "c"]})
    >>> df2 = pd.DataFrame({"id": [2, 3, 3], "y": ["l", "m", "n"]})

    >>> df1 >> semi_join(_, df2)
       id  x
    1   2  b
    2   3  c

    >>> df1 >> anti_join(_, df2)
       id  x
    0   1  a

    Generally, it's a good idea to explicitly specify the on argument.

    >>> df1 >> anti_join(_, df2, on="id")
       id  x
    0   1  a
    """

    if on is None and by is not None:
        on = by

    # copied from semi_join
    if isinstance(on, Mapping):
        left_on, right_on = zip(*on.items())
    else: 
        left_on = right_on = on

    if isinstance(right, DataFrameGroupBy):
        right = right.obj

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
    """Return the first n rows of the data.

    Parameters
    ----------
    __data:
        a DataFrame.
    n:
        The number of rows of data to keep.

    Examples
    --------

    >>> from siuba import head
    >>> from siuba.data import cars

    >>> cars >> head(2)
       cyl   mpg   hp
    0    6  21.0  110
    1    6  21.0  110
    """

    return __data.head(n)


@head.register(DataFrameGroupBy)
def _head_gdf(__data, n = 5):
    groupings = __data.grouper.groupings
    group_cols = [ping.name for ping in groupings]

    df_subset = __data.obj.head(n)
    return df_subset.groupby(group_cols)


# Top N =======================================================================

# TODO: should dispatch to filter, no need to specify pd.DataFrame?
@singledispatch2((pd.DataFrame, DataFrameGroupBy))
def top_n(__data, n, wt = None):
    """Filter to keep the top or bottom entries in each group.

    Parameters
    ----------
    ___data:
        A DataFrame.
    n:
        The number of rows to keep in each group.
    wt:
        A column or expression that determines ordering (defaults to last column in data).

    Examples
    --------
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

@singledispatch2((pd.DataFrame, DataFrameGroupBy))
def gather(__data, key = "key", value = "value", *args, drop_na = False, convert = False):
    """Reshape table by gathering it in to long format.

    Parameters
    ----------
    __data:
        The input data.
    key:
        Name of the key (or measure) column, which holds the names of the columns
        that were turned into rows.
    value:
        Name of the value column, which holds the values from the columns that
        were turned into rows.
    *args:
        A selection of columns. If unspecified, all columns are selected. Any
        arguments you could pass to the select() verb are allowed.
    drop_na: bool
        Whether to remove any rows where the value column is NA.
        

    Examples
    --------

    >>> import pandas as pd
    >>> from siuba import _, gather

    >>> df = pd.DataFrame({"id": ["a", "b"], "x": [1, 2], "y": [3, None]})

    The code below gathers in all columns, except id:

    >>> gather(df, "key", "value", -_.id)
      id key  value
    0  a   x    1.0
    1  b   x    2.0
    2  a   y    3.0
    3  b   y    NaN

    >>> gather(df, "measure", "result", _.x, _.y, drop_na=True)
      id measure  result
    0  a       x     1.0
    1  b       x     2.0
    2  a       y     3.0

    """
    # TODO: implement var selection over *args
    if convert:
        raise NotImplementedError("convert not yet implemented")

    # TODO: copied from nest and select
    var_list = var_create(*(args or __data.columns))
    od = var_select(__data.columns, *var_list)

    if not od:
        return __data

    id_vars = [col for col in __data.columns if col not in od]
    long = pd.melt(__data, id_vars, list(od), key, value)

    if drop_na:
        return long[~long[value].isna()].reset_index(drop = True)

    return long


@gather.register(DataFrameGroupBy)
def _gather(__data, key = "key", value = "value", *args, **kwargs):
    group_cols = [ping.name for ping in __data.grouper.groupings]

    res = gather(__data.obj, key, value, *args, **kwargs)

    # regroup on any grouping vars we did not gather ----
    candidates = set(res.columns) - {key, value}
    regroup_cols = [name for name in group_cols if name in candidates]

    if res is __data.obj:
        # special case where nothing happened
        return __data
    elif regroup_cols:
        return res.groupby(regroup_cols)

    return res


# Spread ======================================================================

def _get_single_var_select(columns, x):
    od = var_select(columns, *var_create(x))

    if len(od) != 1:
        raise ValueError("Expected single variable, received: %s" %list(od))

    return next(iter(od))

@singledispatch2(pd.DataFrame)
def spread(__data, key, value, fill = None, reset_index = True):
    """Reshape table by spreading it out to wide format.

    Parameters
    ----------
    __data:
        The input data.
    key:
        Column whose values will be used as new column names.
    value:
        Column whose values will fill the new column entries.
    fill:
        Value to set for any missing values. By default keeps them as missing values.


    Examples
    --------
    >>> import pandas as pd                                                
    >>> from siuba import _, gather                                        

    >>> df = pd.DataFrame({"id": ["a", "b"], "x": [1, 2], "y": [3, None]}) 

    >>> long = gather(df, "key", "value", -_.id, drop_na=True)
    >>> long
      id key  value
    0  a   x    1.0
    1  b   x    2.0
    2  a   y    3.0

    >>> spread(long, "key", "value")
      id    x    y
    0  a  1.0  3.0
    1  b  2.0  NaN

    """
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


def _unique_name(prefix: str, names: "set[str]"):
    names = set(names)

    ii = 0
    while prefix in names:
        prefix = prefix + str(ii)
        
        ii += 1

    return prefix


def _expand_column(x):
    from pandas.api.types import is_categorical_dtype

    if is_categorical_dtype(x):
        if x.isna().any():
            return [*x.cat.categories, None]

        return x.cat.categories

    return x.unique()



@singledispatch2(pd.DataFrame)
def expand(__data, *args, fill = None):
    """Return table with unique crossings of specified columns.

    Parameters
    ----------
    __data:
        The input data.
    *args:
        Column names to cross and de-duplicate.

    Examples
    --------
    >>> import pandas as pd
    >>> from siuba import _, expand, count, anti_join, right_join

    >>> df = pd.DataFrame({"x": [1, 2, 2], "y": ["a", "a", "b"], "z": 1})
    >>> df
       x  y  z
    0  1  a  1
    1  2  a  1
    2  2  b  1

    >>> combos = df >> expand(_.x, _.y)
    >>> combos
       x  y
    0  1  a
    1  1  b
    2  2  a
    3  2  b

    >>> df >> right_join(_, combos)
       x  y    z
    0  1  a  1.0
    1  1  b  NaN
    2  2  a  1.0
    3  2  b  1.0

    >>> combos >> anti_join(_, df)
       x  y
    1  1  b

    Note that expand will also cross missing values: 

    >>> df2 = pd.DataFrame({"x": [1, None], "y": [3, 4]})
    >>> expand(df2, _.x, _.y)
         x  y
    0  1.0  3
    1  1.0  4
    2  NaN  3
    3  NaN  4

    It will also cross all levels of a categorical (even those not in the data):

    >>> df3 = pd.DataFrame({"x": pd.Categorical(["a"], ["a", "b"])})
    >>> expand(df3, _.x)
       x
    0  a
    1  b

    """


    var_names = list(map(simple_varname, args))
    cols = [_expand_column(__data.loc[:, name]) for name in var_names]

    if fill is not None:
        raise NotImplementedError()

    return pd.MultiIndex.from_product(cols, names=var_names).to_frame(index=False)


@singledispatch2(pd.DataFrame)
def complete(__data, *args, fill = None, explicit=True):
    """Add rows to fill in missing combinations in the data.

    This is a wrapper around expand(), right_join(), along with filling NAs.

    Parameters
    ----------
    __data:
        The input data.
    *args:
        Columns to cross and expand.
    fill:
        A dictionary specifying what to use for missing values in each column.
        If a column is not specified, missing values are left as is.
    explicit:
        Should both NAs created by the complete and pre-existing NAs be filled
        by the fill argument? Defaults to True (filling both). When set to False,
        it will only fill newly created NAs.

    Examples
    --------
    >>> import pandas as pd
    >>> from siuba import _, expand, count, anti_join, right_join

    >>> df = pd.DataFrame({"x": [1, 2, 2], "y": ["a", "a", "b"], "z": [8, 9, None]})
    >>> df
       x  y    z
    0  1  a  8.0
    1  2  a  9.0
    2  2  b  NaN

    >>> df >> complete(_.x, _.y)
       x  y    z
    0  1  a  8.0
    1  1  b  NaN
    2  2  a  9.0
    3  2  b  NaN

    Use the fill argument to replace missing values:

    >>> df >> complete(_.x, _.y, fill={"z": 999})
       x  y      z
    0  1  a    8.0
    1  1  b  999.0
    2  2  a    9.0
    3  2  b  999.0

    A common use of complete is to make zero counts explicit (e.g. for charting):

    >>> df >> count(_.x, _.y) >> complete(_.x, _.y, fill={"n": 0})
       x  y    n
    0  1  a  1.0
    1  1  b  0.0
    2  2  a  1.0
    3  2  b  1.0
    
    Use explicit=False to only fill the NaNs introduced by complete (implicit missing),
    and not those already in the original data (explicit missing):

    >>> df >> complete(_.x, _.y, fill={"z": 999}, explicit=False)
       x  y      z
    0  1  a    8.0
    1  1  b  999.0
    2  2  a    9.0
    3  2  b    NaN
    
    """

    if explicit:
        indicator = False
    else:
        indicator = _unique_name("__merge_indicator", {*__data.columns})
    

    expanded = expand(__data, *args)

    # TODO: should we attempt to coerce cols back to original types?
    #       e.g. NAs will turn int -> float
    on_cols = list(expanded.columns)
    df = expanded.merge(__data, how = "outer", on = on_cols, indicator = indicator)
    
    if fill is not None:
        if explicit:
            for col_name, val in fill.items():
                df[col_name].fillna(val, inplace = True)
        else:
            fill_cols = list(fill)
            indx = df[indicator] == "left_only"
            df.loc[indx, fill_cols] = df.loc[indx, fill_cols].fillna(fill)

    if indicator:
        return df.drop(columns=indicator)

    return df
    

# Separate/Unit/Extract ============================================================

@singledispatch2(pd.DataFrame)
def separate(__data, col, into, sep = r"[^a-zA-Z0-9]",
             remove = True, convert = False,
             extra = "warn", fill = "warn"
            ):
    """Split col into len(into) piece. Return DataFrame with a column added for each piece.

    Parameters
    ----------
    __data:
        a DataFrame.
    col:
        name of column to split (either string, or siu expression).
    into:
        names of resulting columns holding each entry in split.
    sep:
        regular expression used to split col. Passed to col.str.split method.
    remove:
        whether to remove col from the returned DataFrame.
    convert:
        whether to attempt to convert the split columns to numerics.
    extra:
        what to do when more splits than into names.  One of ("warn", "drop" or "merge").
        "warn" produces a warning; "drop" and "merge" currently not implemented.
    fill:
        what to do when fewer splits than into names. Currently not implemented.

    Examples
    --------
    >>> import pandas as pd
    >>> from siuba import separate

    >>> df = pd.DataFrame({"label": ["S1-1", "S2-2"]})

    Split into two columns:

    >>> separate(df, "label", into = ["season", "episode"])
      season episode
    0     S1       1
    1     S2       2

    Split, and try to convert columns to numerics:
    
    >>> separate(df, "label", into = ["season", "episode"], convert = True)
      season  episode
    0     S1        1
    1     S2        2

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
            df_extra_cols = all_splits.iloc[:, n_into].reset_index(drop=True)
            bad_rows = df_extra_cols.dropna(how="all")
            n_extra = bad_rows.shape[0]

            warnings.warn(
                f"Expected {n_into} pieces."
                f"Additional pieces discarded in {n_extra} rows."
                f"Row numbers: {bad_rows.index.values}",
                UserWarning
            )
        elif extra == "drop":
            pass
        elif extra == "merge":
            raise NotImplementedError("TODO: separate extra = 'merge'")
        else:
            raise ValueError("Invalid extra argument: %s" %extra)

    # create new columns in data ----
    out = __data.copy()

    for ii, name in enumerate(into):
        out[name] = all_splits.iloc[:, ii]
    
    #out = pd.concat([__data, keep_splits], axis = 1)

    # attempt to convert columns to numeric ----
    if convert:
        # TODO: better strategy here? 
        for k in into:
            try:
                out[k] = pd.to_numeric(out[k])
            except ValueError:
                pass

    if remove and col_name not in into:
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

    Parameters
    ----------
    __data:
        a DataFrame
    col:
        name of the to-be-created column (string).
    *args:
        names of each column to combine.
    sep:
        separator joining each column being combined.
    remove:
        whether to remove the combined columns from the returned DataFrame.

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
    """Pull out len(into) fields from character strings. 

    Returns a DataFrame with a column added for each piece.

    Parameters
    ----------
    __data:
        a DataFrame
    col:
        name of column to split (either string, or siu expression).
    into:
        names of resulting columns holding each entry in pulled out fields.
    regex:
        regular expression used to extract field. Passed to col.str.extract method.
    remove:
        whether to remove col from the returned DataFrame.
    convert:
        whether to attempt to convert the split columns to numerics.
    flags:
        flags from the re module, passed to col.str.extract.

    """

    col_name = simple_varname(col)
    n_into = len(into)

    all_splits = __data[col_name].str.extract(regex, flags)
    n_split_cols = len(all_splits.columns)

    if n_split_cols != n_into:
        raise ValueError("Split into %s pieces, but expected %s" % (n_split_cols, n_into))

    # attempt to convert columns to numeric ----
    if convert:
        # TODO: better strategy here? 
        for k in all_splits:
            try:
                all_splits[k] = pd.to_numeric(all_splits[k])
            except ValueError:
                pass

    out = __data.copy()
    for ii, name in enumerate(into):
        out[name] = all_splits.iloc[:, ii]
    
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


# tbl ----

from siuba.siu._databackend import SqlaEngine

@singledispatch2((pd.DataFrame, DataFrameGroupBy))
def tbl(src, *args, **kwargs):
    """Create a table from a data source.

    Parameters
    ----------
    src:
        A pandas DataFrame, SQLAlchemy Engine, or other registered object.
    *args, **kwargs:
        Additional arguments passed to the individual implementations.

    Examples
    --------
    >>> from siuba.data import cars

    A pandas DataFrame is already a table of data, so trivially returns itself.

    >>> tbl(cars) is cars
    True

    tbl() is useful for quickly connecting to a SQL database table.

    >>> from sqlalchemy import create_engine
    >>> from siuba import count, show_query, collect

    >>> engine = create_engine("sqlite:///:memory:")
    >>> cars.to_sql("cars", engine, index=False)

    >>> tbl_sql_cars = tbl(engine, "cars")
    >>> tbl_sql_cars >> count()
    # Source: lazy query
    # DB Conn: Engine(sqlite:///:memory:)
    # Preview:
        n
    0  32
    # .. may have more rows

    When using duckdb, pass a DataFrame as the third argument to operate directly on it:

    >>> engine2 = create_engine("duckdb:///:memory:")
    >>> tbl_cars_duck = tbl(engine, "cars", cars.head(2)) 
    >>> tbl_cars_duck >> count() >> collect()
        n
    0  32

    You can analyze a mock table

    >>> from sqlalchemy import create_mock_engine
    >>> from siuba import _

    >>> mock_engine = create_mock_engine("postgresql:///", lambda *args, **kwargs: None)
    >>> tbl_mock = tbl(mock_engine, "some_table", columns = ["a", "b", "c"])

    >>> q = tbl_mock >> count(_.a) >> show_query()    # doctest: +NORMALIZE_WHITESPACE
    SELECT some_table_1.a, count(*) AS n
    FROM some_table AS some_table_1 GROUP BY some_table_1.a ORDER BY n DESC
    """

    return src


@tbl.register
def _tbl_sqla(src: SqlaEngine, table_name, columns=None):
    from siuba.sql import LazyTbl

    # TODO: once we subclass LazyTbl per dialect (e.g. duckdb), we can move out
    # this dialect specific logic.
    if src.dialect.name == "duckdb" and isinstance(columns, pd.DataFrame):
        src.execute("register", (table_name, columns))
        return LazyTbl(src, table_name)
    
    return LazyTbl(src, table_name, columns=columns)


@tbl.register(object)
def _tbl(__data, *args, **kwargs):
    raise NotImplementedError(
        f"Unsupported type {type(__data)}. "
        "Note that tbl currently can be used at the start of a pipe, but not as "
        "a step in the pipe."
    )

# Install Siu =================================================================

install_pd_siu()
