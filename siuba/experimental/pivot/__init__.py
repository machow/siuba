import pandas as pd
import numpy as np

from typing import Union, Tuple, Dict, Optional
from pandas.core.groupby import DataFrameGroupBy

from siuba.dply.verbs import singledispatch2, gather, var_create, var_select


def pivot_longer_spec(data,
                      spec,
                      names_repair: Optional[str] = "check_unique",
                      values_drop_na: bool = False):
    raise NotImplementedError("TODO: see https://github.com/machow/siuba/issues/293")


@singledispatch2(pd.DataFrame)
def pivot_longer(
        __data,
        *args,
        names_to: Union[str, Tuple[str, ...]] = "name",
        names_prefix: Optional[str] = None,
        names_sep: Optional[str] = None,
        names_pattern: Optional[str] = None,
        names_ptypes: Optional[Tuple] = None,
        names_repair: str = "check_unique",
        values_to: str = "value",
        values_drop_na: bool = False,
        values_ptypes: Optional[Union[str, Tuple[str, ...]]] = None,
        values_transform: Optional[Dict] = dict(),
        ):
    
    if names_sep is not None and names_pattern is not None:
        raise ValueError("You may only use either `names_sep` or "
                         "`names_pattern`.")

    if isinstance(names_to, str):
        names_to = (names_to,)

    # Copied selection over from gather, maybe this can be compartmentalised?
    var_list = var_create(*args)
    od = var_select(__data.columns, *var_list)

    value_vars = list(od) or None

    id_vars = [col for col in __data.columns if col not in od]

    keep_data = __data.loc[:,id_vars]
    if value_vars is None:
        # While stack works in this case, it will later on merge in to the
        # original dataframe. To copy tidyr behaviour, we need to raise a
        # ValueError
        # stacked = __data.stack(dropna=values_drop_na)
        raise ValueError("Please provide at least 1 column or all columns "
                         "(shorthand: _[:]).")
    elif names_sep is not None or names_pattern is not None:
        to_stack = __data.loc[:,value_vars]
        column_index = (
            to_stack.columns.str.split(names_sep).map(tuple)
            if names_sep is not None
            # Split by names_pattern, and remove empty strings using filter
            else to_stack.columns.str.split(names_pattern).map(
                lambda x: tuple(list(filter(None, x)))
            )
        )
        split_lengths = np.array(column_index.map(len))

        if not np.all(split_lengths == split_lengths[0]):
            raise ValueError(
                    "Splitting by {} leads to unequal lenghts ({}).".format(
                        names_sep if names_sep is not None else names_pattern
                    )
                )
        
        if split_lengths[0] != len(names_to):
            raise ValueError("Splitting provided more values than provided in "
                             "`names_to`")
        
        # TODO: To set names for the new index, we need to feed in a list.
        # There's no particular reason to use a tuples as input in the first
        # place, might be worth reconsidering the choice of input format?
        # TODO: What if we don't use '_value' in the tuple? Need to check tidyr
        stack_idx = (
            [i for i, x in enumerate(list(names_to)) if x != "_value"]
            if names_to != ('_value',)
            else -1
        )
        names_to = [x if x != "_value" else None for x in names_to]

        column_index = column_index.set_names(names_to)

        to_stack.columns = column_index
        stacked = to_stack.stack(stack_idx)
        stacked = stacked.reset_index(level=stacked.index.nlevels - 1)

        if stack_idx == -1:
            stacked = stacked.drop(columns='level_1')
        if np.nan in names_to:
            stacked = stacked.drop(columns=[np.nan])
        if values_drop_na:
            stacked = stacked.dropna(axis = 1)
    else:
        stacked = __data.loc[:,value_vars].stack(dropna=values_drop_na)
        # Set column names for stack
        # As in tidyr `values_to` is ignored if `names_sep` or `names_pattern`
        # is provided.
        stacked.index.rename(names_to[0], level=1, inplace=True)
        stacked.name = values_to

    # values_transform was introduced in tidyr 1.1.0
    if values_to in values_transform:
        # TODO: error handling -- this won't work for dictionaries
        # list needs special handling, as it can only be applied to iterables,
        # not integers.
        if values_transform[values_to] == list:
            stacked = stacked.apply(lambda x: [x])
        else:
            stacked = stacked.apply(lambda x: values_transform[values_to](x))

    stacked_df = (
        # if `names_sep` or `names_pattern` are not provided `stacked` will
        # be a pd.Series and needs its index reset.
        stacked.reset_index(1)
        if names_sep is None and names_pattern is None
        else stacked
    )

    # If we want to pivot all but one, we are left with a `pd.Series`.
    # This needs to be converted to a DataFrame to serve as left element in a
    # merge
    if isinstance(keep_data, pd.Series):
        output_df = keep_data.to_frame().merge(stacked_df, left_index=True, right_index=True)
    elif keep_data.empty:
        output_df = stacked_df
    else:
        output_df = keep_data.merge(stacked_df, left_index=True, right_index=True)
    
    return output_df

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
