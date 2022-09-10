import pandas as pd
import numpy as np

from typing import Union, Tuple, Dict, Optional
from pandas.core.groupby import DataFrameGroupBy

from siuba.dply.verbs import gather, var_create, var_select, separate, extract
from siuba.siu import singledispatch2


def pivot_longer_spec(data,
                      spec,
                      names_repair: Optional[str] = "check_unique",
                      values_drop_na: bool = False):
    raise NotImplementedError("TODO: see https://github.com/machow/siuba/issues/293")


def create_spec(var_names, names_to, names_sep, names_pattern, values_to):
    df_spec = pd.DataFrame({"_name": var_names, "_value": values_to})

    if names_sep:
        df_spec = separate(df_spec, "_name", names_to, names_sep, remove=False)
    elif names_pattern:
        df_spec = extract(df_spec, "_name", names_to, names_pattern, remove=False)
    else:
        if len(names_to) > 1:
            raise TypeError(
                "pivot_longer either needs names_to to be string, or to receive "
                "names_sep or names_pattern arguments."
            )
        df_spec = df_spec.assign(**{names_to[0]: df_spec["_name"]})

    return df_spec


def spec_to_multiindex(df_spec):
    # _value will be the outer column index, and split name columns the inner.
    # this allows us to stack the inner part, while that _values stays as columns.
    internal = {"_value", "_name"}

    #other_cols = [name for name in df_spec.columns if name not in internal]
    #df_spec[other_cols].apply(lambda ser: tuple(ser), axis=1)
    other_cols = [nm for nm in df_spec.columns if nm not in internal]

    indx_cols = ["_value", *other_cols]
    indx_names = [None] + other_cols

    return pd.MultiIndex.from_frame(df_spec[indx_cols], names=indx_names)


def _drop_cols_by_position(df, locations):
    return df.loc[:, ~np.array([x in locations for x in range(df.shape[1])])]


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

    # select id columns and measure data --------------------------------------

    var_list = var_create(*args)
    od = var_select(__data.columns, *var_list)

    value_vars = list(od)

    if value_vars is None:
        raise ValueError(
            "Please select at least 1 column of values in pivot_longer.\n\n"
            "E.g. pivot_longer(data, _.some_col, _.another_col, ...)"
        )

    id_vars = [col for col in __data.columns if col not in od]
    wide_ids = __data.loc[:,id_vars]
    wide_values = __data.loc[:,value_vars]

    # note that this will include repeats in the data (e.g. two columns named "a")
    wide_cols = list(wide_values.columns)

    # the spec is usable by both pivot longer and wider
    df_spec = create_spec(wide_cols, names_to, names_sep, names_pattern, values_to)
    
    column_index = spec_to_multiindex(df_spec)

    # reshape to long ---------------------------------------------------------
    # note that this section could be moved to a function that takes only the
    # wide_values and a spec
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

        long_values = (wide_values .stack(inner_levels.tolist()).droplevel(list(n_orig_levels + indx_to_drop))
            .reset_index(inner_levels[~inner_levels_na].tolist())
        )
    
    if values_drop_na:
        value_column_names = list(column_index.levels[0])
        long_values = long_values.dropna(subset=value_column_names, how="all")

    # transform values --------------------------------------------------------

    # TODO: names_transform for names, values_transform for values (including .value in names_to)
    if len(names_to) > 1:
        # TODO: transforms with values in names
        transformed = long_values
    elif values_to in values_transform:
        # TODO: error handling -- this won't work for dictionaries
        # list needs special handling, as it can only be applied to iterables,
        # not integers.
        f_transform = values_transform[values_to]
        if f_transform == list:
            f_transform = lambda ser: ser.apply(lambda x: [x])
            
        transformed = long_values.copy()
        transformed[values_to] = f_transform(long_values[values_to])
    else:
        transformed = long_values

    # merge in id variables ---------------------------------------------------

    if wide_ids.shape[1] == 0:
        # no id columns, just return long values
        output_df = transformed
    else:
        output_df = wide_ids.merge(transformed, left_index=True, right_index=True)
    
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
