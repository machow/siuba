from siuba.dply.verbs import singledispatch2, gather
import pandas as pd

@singledispatch2(pd.DataFrame)
def pivot_longer(
        __data,
        cols,
        names_to = "name",
        names_prefix = None,
        names_sep = None,
        names_pattern = None,
        names_ptypes = tuple(),
        names_repair = "check_unique",
        values_to = "value",
        values_drop_na = False,
        values_ptypes = tuple()
        ):
    return gather(__data, names_to, values_to, cols)
