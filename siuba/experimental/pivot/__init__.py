from siuba.dply.verbs import singledispatch2, gather, var_create, var_select
import pandas as pd
import numpy as np

def pivot_longer_spec(data, spec, names_repair = "check_unique", values_drop_na = False):
    pass

@singledispatch2(pd.DataFrame)
def pivot_longer(
        __data,
        *args,
        names_to = "name",
        names_prefix = None,
        names_sep = None,
        names_pattern = None,
        names_ptypes = tuple(),
        names_repair = "check_unique",
        values_to = "value",
        values_drop_na = False,
        values_ptypes = tuple(),
        values_transform = dict(),
        ):
    
    # Copied selection over from gather, maybe this can be compartmentalised away?
    var_list = var_create(*args)
    od = var_select(__data.columns, *var_list)

    value_vars = list(od) or None

    id_vars = [col for col in __data.columns if col not in od]

    keep_data = __data.loc[:,id_vars]
    if value_vars is None:
        stacked = __data.stack(dropna=values_drop_na)
    else:
        stacked = __data.loc[:,value_vars].stack(dropna=values_drop_na)
    
    # Set column names for stack
    stacked.index.rename(names_to, level=1, inplace=True)
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

    stacked_df = stacked.reset_index(1)

    # If we want to pivot all but one, we are left with a pd.Series
    # This needs to be converted to a DataFrame to serve as left element in a merge
    if isinstance(keep_data, pd.Series):
        output_df = keep_data.to_frame().merge(stacked_df, left_index=True, right_index=True)
    else:
        output_df = keep_data.merge(stacked_df, left_index=True, right_index=True)

    return output_df
