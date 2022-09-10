import re
import pandas as pd

from collections import Counter
from typing import Callable



def group_vars(df: pd.DataFrame) -> "list[str]":
    groupings = df.grouper.groupings
    group_cols = [ping.name for ping in groupings]
    return group_cols


def reconstruct_tibble(
    input_: pd.DataFrame,
    output: pd.DataFrame,
    ungrouped_vars: "tuple[str]" = tuple()
):
    if isinstance(input_, pd.core.groupby.DataFrameGroupBy):
        old_groups = group_vars(input_)
        new_groups = list((set(old_groups) - set(ungrouped_vars)) & set(output.columns))

        return output.groupby(new_groups)
    
    return output


def check_dict_of_functions(d_funcs: "Callable | dict[str, Callable]", names, arg_name):
    if isinstance(d_funcs, dict):
        missing = set(d_funcs) - set(names)
        if missing:
            raise ValueError(
                f"`{arg_name}=` is a dictionary of functions, so must corresponding to "
                "column names in the data. No matching column names found for these "
                f"entries: {missing}"
            )
        return d_funcs
    elif callable(d_funcs):
        return {k: d_funcs for k in names}

    raise TypeError(
        f"{arg_name} must be a dictionary mapping column names to functions, or "
        "a single function."
    )


# vec_as_names ----------------------------------------------------------------

SEPERATOR = "___"

def _make_unique(name):
    pass

def _strip_suffix(name):
    return re.sub("(___?[0-9]*)+$", "", name)

def vec_as_names(names, *, repair: "str | Callable"):
    """Validate and repair column names.

    Parameters
    ----------
    names:
        A list-like of column names
    repair:
 
    """
    # minimal, unique, universal, check_unique
    # minimal: names can be accessed using df[<name>], e.g. "x", np.nan
    # unique: names are minimal and no duplicates. Can be accessed using df[name]
    # check_unique: 
    # universal: accessible by attribute (may throw an error if its a pandas method)

    if repair not in {"unique", "check_unique", "minimal"} and not callable(repair):
        raise NotImplementedError()

    validate_unique = callable(repair) or repair == "check_unique"

    # minimal ---
    if repair == "minimal":
        return names

    # custom function ---
    if callable(repair):
        names = repair(names)

    # check_unique ----
    raw_uniq = Counter(names)
    if len(raw_uniq) < len(names) and validate_unique:
        duplicates = [entry for entry, n in raw_uniq.items() if n > 1]

        raise ValueError(
            f"Names must be unique, but detected {len(duplicates)} duplicate name(s).\n\n"
            f"Duplicated names: {duplicates}"
        )


    stripped_names = list(map(_strip_suffix, names))
    uniq = Counter(stripped_names)

    # name repair unique ----
    result = []
    for ii, name in enumerate(stripped_names):
        if uniq[name] > 1:
            result.append(f"{name}___{ii}")
        else:
            result.append(name)
    
    return result
    
