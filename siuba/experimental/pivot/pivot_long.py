import pandas as pd
import numpy as np
import re

from typing import Union, Tuple, Dict, Optional, Callable
from pandas.core.groupby import DataFrameGroupBy

from siuba.dply.verbs import gather, var_create, var_select, separate, extract
from siuba.siu import singledispatch2

from .utils import vec_as_names, reconstruct_tibble, check_dict_of_functions

# Utilities ===================================================================

def spec_to_multiindex(df_spec):
    # _value will be the outer column index, and split name columns the inner.
    # this allows us to stack the inner part, while that _values stays as columns.
    internal = {".value", ".name"}

    #other_cols = [name for name in df_spec.columns if name not in internal]
    #df_spec[other_cols].apply(lambda ser: tuple(ser), axis=1)
    other_cols = [nm for nm in df_spec.columns if nm not in internal]

    # get final columns together
    indx_cols = [".value", *other_cols]
    indx_names = [None] + other_cols

    df_final = df_spec.loc[:, indx_cols]

    # ensure levels of _value in multi-index are in first-observed order
    # otherwise, their final columns come out alphabetically in pivot_longer.
    val = df_final[".value"]
    df_final[".value"] = pd.Categorical(val, val.dropna().unique())

    return pd.MultiIndex.from_frame(df_final, names=indx_names)


def _drop_cols_by_position(df, locations):
    return df.loc[:, ~np.array([x in locations for x in range(df.shape[1])])]


# Pivot longer ================================================================

@singledispatch2(pd.DataFrame)
def pivot_longer(
        __data,
        *cols,
        names_to: Union[str, Tuple[str, ...]] = "name",
        names_prefix: Optional[str] = None,
        names_sep: Optional[str] = None,
        names_pattern: Optional[str] = None,
        names_ptypes: Optional[Tuple] = None,
        names_repair: str = "check_unique",
        values_to: str = "value",
        values_drop_na: bool = False,
        values_ptypes: Optional[Union[str, Tuple[str, ...]]] = None,
        values_transform: Optional[Dict] = None,
):
    """Pivot data from wide to long format.

    This function stacks columns of data, turning them into rows.

    Parameters
    ----------
    __data:
        The input data.
    *cols: 
        Columns to pivot into longer format. This uses tidyselect
        (e.g. `_[_.some_col, _.another_col]`).
    names_to:
        A list specifying the new column or columns to create from the information
        stored in the column names of data specified by cols.
    names_prefix:
        A regular expression to strip off from the start of column selected by `*cols`.
    names_sep:
        If names_to is a list of name parts, this is a separater the name is split on.
        This is the same as the sep argument in the separate() function.
    names_pattern:
        If names_to is a list of name parts, this is a pattern to extract parts
        This is the same as the regex argument in the extract() function.
    names_ptypes, values_ptypes:
        Not implemented.
    names_transform:
        TODO
    names_repair:
        Strategy for fixing of invalid column names. "minimal" leaves them as is.
        "check_unique" raises an error if there are duplicate names. "unique"
        de-duplicates names by appending "___{position}" to them.
    values_to:
        A string specifying the name of the column created to hold the stacked
        values of the selected `*cols`. If names_to is a list with the entry ".value",
        then this argument is ignored.


    Examples
    --------
    >>> from siuba import _

    >>> df = pd.DataFrame({"id": [1, 2], "x": [5, 6], "y": [7, 8]})
    >>> pivot_longer(df, ~_.id, names_to="variable", values_to="number")
       id variable  number
    0   1        x       5
    0   1        y       7
    1   2        x       6
    1   2        y       8

    >>> weeks = pd.DataFrame({"id": [1], "year": [2020], "wk1": [5], "wk2": [6]})
    >>> pivot_longer(weeks, _.startswith("wk"), names_to="week", names_prefix="wk")
       id  year week  value
    0   1  2020    1      5
    0   1  2020    2      6

    >>> df2 = pd.DataFrame({"id": [1], "a_x1": [2], "b_x2": [3], "a_y1": [4]})
    >>> names = ["condition", "group", "number"]
    >>> pat = "(.*)_(.)(.*)"
    >>> pivot_longer(df2, _["a_x1":"a_y1"], names_to = names, names_pattern = pat)
       id condition group number  value
    0   1         a     x      1      2
    0   1         b     x      2      3
    0   1         a     y      1      4

    >>> names = ["x1", "x2", "y1", "y2"]
    >>> wide = pd.DataFrame({
    ...    "x1": [1, 11], "x2": [2, 22], "y1": [3, 33], "y2": [4, 44]
    ... })
    >>> pivot_longer(wide, _[:], names_to = [".value", "set"], names_pattern = "(.)(.)")
      set   x   y
    0   1   1   3
    0   2   2   4
    1   1  11  33
    1   2  22  44

    """

    df_spec = build_longer_spec(
        __data,
        *cols,
        names_to=names_to,
        values_to=values_to,
        names_prefix=names_prefix,
        names_sep=names_sep,
        names_pattern=names_pattern,
        names_ptypes=names_ptypes,
    )

    return pivot_longer_spec(
        __data,
        df_spec,
        names_repair,
        values_drop_na,
        values_ptypes, 
        values_transform
    )


@pivot_longer.register(DataFrameGroupBy)
def _pivot_longer_gdf(__data, *args, **kwargs):
    # TODO: consolidate all verbs that punt to DataFrame version (#118)
    prior_groups = [el.name for el in __data.grouper.groupings]

    df = __data.obj
    res = pivot_longer(df, *args, **kwargs)

    missing_groups = set(prior_groups) - set(res.columns)
    if missing_groups:
        raise ValueError(
                "When using pivot_longer on grouped data, the result must contain "
                "original grouping columns. Missing group columns: %s" %missing_groups
                )

    return res.groupby(prior_groups)


@singledispatch2((pd.DataFrame, DataFrameGroupBy))
def build_longer_spec(
    __data,
    *cols,
    names_to="name",
    values_to="value",
    names_prefix: "str | None"=None,
    names_sep=None,
    names_pattern=None,
    names_ptypes=None,
    names_transform: "dict[str, Callable] | None"=None
):
    if names_sep is not None and names_pattern is not None:
        raise ValueError("You may only use either `names_sep` or "
                         "`names_pattern`.")

    if isinstance(names_to, str):
        names_to = (names_to,)

    # select id columns and measure data --------------------------------------

    var_list = var_create(*cols)
    od = var_select(__data.columns, *var_list)

    value_vars = list(od)

    if not value_vars:
        raise ValueError(
            "Please select at least 1 column of values in pivot_longer.\n\n"
            "E.g. pivot_longer(data, _.some_col, _.another_col, ...)"
        )

    # note that this will include repeats in the data (e.g. two columns named "a")
    wide_values = __data.loc[:,value_vars]
    wide_cols = list(wide_values.columns)

    # strip prefix ------------------------------------------------------------
    if names_prefix is None:
        names = wide_cols
    elif isinstance(names_prefix, str):
        names = [re.sub(f"^{names_prefix}", "", name) for name in wide_cols]
    else:
        raise TypeError("names_prefix must be a string or None.")

    # start spec and split name into parts ------------------------------------
    # note that we set .name to be the names with names_prefix removed, do all
    # of the part splitting off that name, then set .name to the original values
    # at the very end.
    df_spec = pd.DataFrame({".name": names, ".value": values_to})

    if names_sep:
        df_spec = separate(df_spec, ".name", names_to, names_sep, remove=False)
    elif names_pattern:
        df_spec = extract(df_spec, ".name", names_to, names_pattern, remove=False)
    else:
        if len(names_to) == 1:
            df_spec = df_spec.assign(**{names_to[0]: df_spec[".name"]})
        else:
            raise TypeError(
                "pivot_longer either needs names_to to be string, or to receive "
                "names_sep or names_pattern arguments."
            )

    # setting names back to original
    df_spec[".name"] = wide_cols

    # transform columns -------------------------------------------------------
    if names_transform:
        _cols = list(df_spec.columns[2:])
        transforms = check_dict_of_functions(names_transform, _cols, "names_transform")

        for col_name, func in transforms.items():
            df_spec[col_name] = func(df_spec[col_name])

    return df_spec
    

@singledispatch2((pd.DataFrame, DataFrameGroupBy))
def pivot_longer_spec(
    __data,
    spec,
    names_repair: Optional[str] = "check_unique",
    values_drop_na: bool = False,
    values_ptypes = None,
    values_transform = None
):

    column_index = spec_to_multiindex(spec)

    wide_values = __data.loc[:,spec[".name"].unique()]
    wide_ids = __data.loc[:,~__data.columns.isin(wide_values.columns)]


    # reshape to long ---------------------------------------------------------
    inner_levels_na = pd.isna(column_index.names[1:])
    indx_to_drop = np.where(inner_levels_na)[0]

    if len(column_index.levels[0]) == 1:
        # simple case: only creating a single value column. in this case we use pd.melt,
        # since it can handle duplicate single and multi-index columns.
        _value_name = column_index.levels[0][0]

        wide_values.columns = column_index.droplevel(0)

        long_values = pd.melt(
            wide_values,
            value_vars=None,
            value_name=_value_name,
            ignore_index=False
        ).sort_index(level=-1).pipe(_drop_cols_by_position, indx_to_drop)

    else:
        # complex case: multiple value columns. Note that pandas throws an error if the
        # columns being unstacked contain duplicate column parts. E.g. if you split
        # duplicate columns x_1_1 and x_1_1 on "_".
        # (this behavior is fairly funky in dplyr, so probably okay to error)
        n_orig_levels = wide_values.index.nlevels

        if column_index.nlevels > 1:
            inner_levels = np.array(range(1, column_index.nlevels))
        else:
            # edge case, where only _values exists and is defined from matching
            # patterns in the columns. we can stack this only multiindex level, and it
            # will still set the _values as columns.
            inner_levels = np.array([0])
            indx_to_drop = np.array([0])

        wide_values.columns = column_index

        # note that levels named NA should not be in the final result, but are
        # needed for stacking.
        long_values_almost = (wide_values
            .stack(inner_levels.tolist())
            .droplevel(list(n_orig_levels + indx_to_drop))
        )

        # note: this is necessary for pandas <1.3 backwards compat. our column
        # index is a categorical, so reset_index is seen as trying to add a category
        # that doesn't exist to it... :/
        long_values_almost.columns = list(long_values_almost.columns)

        # once we drop earlier pandas versions, can cut out piece above
        long_values = (long_values_almost
            .reset_index(inner_levels[~inner_levels_na].tolist())
        )
    
    if values_drop_na:
        value_column_names = list(column_index.levels[0])
        long_values = long_values.dropna(subset=value_column_names, how="all")

    # transform values --------------------------------------------------------

    # TODO: names_transform for names, values_transform for values (including .value in names_to)

    transformed = long_values

    if values_transform:
        value_names = list(spec[".value"].unique())
        transforms = check_dict_of_functions(
            values_transform,
            value_names,
            "values_transform"
        )

        for col_name, f_transform in transforms.items():
            # TODO: error handling -- this won't work for dictionaries
            # list needs special handling, as it can only be applied to iterables,
            # not integers.
            long_values[col_name] = f_transform(long_values[col_name])

    # merge in id variables ---------------------------------------------------

    if wide_ids.shape[1] == 0:
        # no id columns, just return long values
        output_df = transformed
    else:
        output_df = pd.merge(wide_ids, transformed, left_index=True, right_index=True)
    
    # note that the pandas merge above has to add a suffix to duplicate columns,
    # so we need to use the original column names when repairing/validating.
    repaired_names = vec_as_names([*wide_ids.columns, *transformed.columns], repair=names_repair)
    output_df.columns = repaired_names

    return output_df


