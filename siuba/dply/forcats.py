import pandas as pd
import numpy as np

from siuba.siu import symbolic_dispatch
from collections import defaultdict

# fct_reorder -----------------------------------------------------------------

@symbolic_dispatch
def fct_reorder(fct, x, func = np.median, desc = False) -> pd.Categorical:
    """Return copy of fct, with categories reordered according to values in x.
    
    Arguments:
        fct: a pandas.Categorical, or array(-like) used to create one.
        x: values used to reorder categorical. Must be same length as fct.
        func: function run over all values within a level of the categorical.
        desc: whether to sort in descending order.

    Notes that NaN categories can't be ordered. When func returns NaN, sorting
    is always done with NaNs last.


    Examples:
        >>> fct_reorder(['a', 'a', 'b'], [4, 3, 2])
        ['a', 'a', 'b']
        Categories (2, object): ['b', 'a']

        >>> fct_reorder(['a', 'a', 'b'], [4, 3, 2], desc = True)
        ['a', 'a', 'b']
        Categories (2, object): ['a', 'b']

        >>> fct_reorder(['x', 'x', 'y'], [4, 0, 2], np.max)
        ['x', 'x', 'y']
        Categories (2, object): ['y', 'x']

    """

    x_vals = x.values if isinstance(x, pd.Series) else x
    s = pd.Series(x_vals, index = fct)

    # sort groups by calculated agg func. note that groupby uses dropna=True by default,
    # but that's okay, since pandas categoricals can't order the NA category
    ordered = s.groupby(level = 0).agg(func).sort_values(ascending = not desc)

    return pd.Categorical(fct, categories=ordered.index)


# fct_recode ------------------------------------------------------------------

@symbolic_dispatch
def fct_recode(fct, recat=None, **kwargs) -> pd.Categorical:
    """Return copy of fct with renamed categories.

    Arguments:
        fct: a pandas.Categorical, or array(-like) used to create one. 
        **kwargs: arguments of form new_name = old_name.

    Examples:
        >>> cat = ['a', 'b', 'c']
        >>> fct_recode(cat, z = 'c')
        ['a', 'b', 'z']
        Categories (3, object): ['a', 'b', 'z']

        # >>> fct_recode(cat, x = 'a', x = 'b')
        # >>> fct_recode(cat, x = ['a', 'b'])
        
    """

    if recat and not isinstance(recat, dict):
        raise TypeError("fct_recode requires named args or a dict.")

    if recat and kwargs:
        duplicate_keys = set(recat).intersection(set(kwargs))
        if duplicate_keys:
            raise ValueError(
                "The following recode name(s) were specified more than once: {}" \
                .format(duplicate_keys)
            )

    new_cats = {**recat, **kwargs} if recat else kwargs
    return fct_collapse(fct, new_cats)


# fct_collapse ----------------------------------------------------------------

@symbolic_dispatch
def fct_collapse(fct, recat, group_other = None) -> pd.Categorical:
    """Return copy of fct with categories renamed. Optionally group all others.

    Arguments:
        fct: a pandas.Categorical, or array(-like) used to create one.  
        recat: dictionary of form {new_cat_name: old_cat_name}. old_cat_name may be
               a list of existing categories, to be given the same name.
        group_other: an optional string, specifying what all other categories should be named.

    Notes:
        Resulting levels index is ordered according to the earliest level replaced.
        If we rename the first and last levels to "c", then "c" is the first level.

    Examples:
        >>> fct_collapse(['a', 'b', 'c'], {'x': 'a'})
        ['x', 'b', 'c']
        Categories (3, object): ['x', 'b', 'c']

        >>> fct_collapse(['a', 'b', 'c'], {'x': 'a'}, group_other = 'others')
        ['x', 'others', 'others']
        Categories (2, object): ['x', 'others']

        >>> fct_collapse(['a', 'b', 'c'], {'ab': ['a', 'b']})
        ['ab', 'ab', 'c']
        Categories (2, object): ['ab', 'c']

        >>> fct_collapse(['a', 'b', None], {'a': ['b']})
        ['a', 'a', NaN]
        Categories (1, object): ['a']

    """
    if not isinstance(fct, pd.Categorical):
        fct = pd.Categorical(fct)

    # each existing cat will map to a new one ----
    # need to know existing to new cat
    # need to know new cat to new code
    cat_to_new = {k: None for k in fct.categories}
    for new_name, v in recat.items():
        v = [v] if not np.ndim(v) else v
        for old_name in v:
            if cat_to_new[old_name] is not None:
                raise Exception("category %s was already re-assigned"%old_name)
            cat_to_new[old_name] = new_name

    # collapse all unspecified cats to group_other if specified ----
    for k, v in cat_to_new.items():
        if v is None:
            if group_other is not None:
                cat_to_new[k] = group_other
            else:
                cat_to_new[k] = k

    # map from old cat to new code ----
    # calculate new codes
    ordered_cats = {new: True for old, new in cat_to_new.items()}

    new_cat_set = {k: ii for ii, k in enumerate(ordered_cats)}

    # make an array, where the index is old code + 1 (so missing val index is 0)
    old_code_to_new = np.array(
            [-1] + [new_cat_set[new_cat] for new_cat in cat_to_new.values()]
    )

    # map old cats to new codes
    #remap_code = {old: new_cat_set[new] for old, new in cat_to_new.items()}
    new_codes = old_code_to_new[fct.codes + 1]
    new_cats = list(new_cat_set)
    return pd.Categorical.from_codes(new_codes, new_cats)


# fct_lump --------------------------------------------------------------------

@symbolic_dispatch
def fct_lump(fct, n = None, prop = None, w = None, other_level = "Other", ties = None) -> pd.Categorical:
    """
    Arguments:
        fct: a pandas.Categorical, or array(-like) used to create one.
        n: number of categories to keep.
        prop: (not implemented) keep categories that occur prop proportion of the time.
        w: array of weights corresponding to each value in fct.
        other_level: name for all lumped together levels.
        ties: (not implemented) method to use in the case of ties.

    Notes:
        Currently, one of n and prop must be specified.

    Examples:
        >>> fct_lump(['a', 'a', 'b', 'c'], n = 1)
        ['a', 'a', 'Other', 'Other']
        Categories (2, object): ['a', 'Other']

        # TODO: implement prop arg
        >>> fct_lump(['a', 'a', 'b', 'b', 'c', 'd'], prop = .2)
        ['a', 'a', 'b', 'b', 'Other', 'Other']
        Categories (3, object): ['a', 'b', 'Other']
        

    """

    if ties is not None:
        raise NotImplementedError("ties is not implemented")

    if n is None and prop is None:
        raise NotImplementedError("Either n or prop must be specified")

    keep_cats = _fct_lump_n_cats(fct, w, other_level, ties, n = n, prop = prop)
    return fct_collapse(fct, {k:k for k in keep_cats}, group_other = other_level)

def _fct_lump_n_cats(fct, w, other_level, ties, n = None, prop = None):
    # TODO: currently always selects n, even if ties

    # weights might be a Series, or array, etc..
    arr = _get_values(w) if w is not None else 1
    ser = pd.Series(arr, index = fct)
    counts = ser.groupby(level = 0).sum()

    if n is not None:
        ascending = n < 0
        sorted_arr = counts.sort_values(ascending = ascending)
        res = sorted_arr.iloc[:abs(n)]
    elif prop is not None:
        sorted_arr = counts.sort_values() / counts.sum()

        if prop < 0:
            res = sorted_arr.loc[sorted_arr <= abs(prop)]
        else:
            res = sorted_arr.loc[sorted_arr > prop]

    return res.index.values

def _get_values(x):
    # TODO: move into utility, note pandas now encouraging .array method
    if isinstance(x, pd.Series): return x.values

    return x


# fct_rev ---------------------------------------------------------------------

@symbolic_dispatch
def fct_rev(fct) -> pd.Categorical:
    """Return a copy of fct with category level order reversed.next
    
    Arguments:
        fct: a pandas.Categorical, or array(-like) used to create one.

    """

    if not isinstance(fct, pd.Categorical):
        fct = pd.Categorical(fct)

    rev_levels = list(reversed(fct.categories))

    return fct.reorder_categories(rev_levels)
