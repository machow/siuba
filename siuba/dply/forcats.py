import pandas as pd
import numpy as np

from siuba.siu import symbolic_dispatch
from collections import defaultdict


def _get_cat_order(x):
    if isinstance(x, pd.Series):
        arr = x.array
    else:
        arr = x

    if isinstance(arr, pd.Categorical):
        return arr.ordered

    return None

def _maybe_upcast(fct_in, fct_out):
    if isinstance(fct_in, pd.Series):
        return pd.Series(fct_out)

    return fct_out


# fct_inorder, fct_infreq -----------------------------------------------------

@symbolic_dispatch
def fct_inorder(fct, ordered=None):
    """Return a copy of fct, with categories ordered by when they first appear.

    Parameters
    ----------
    fct : list-like
        A pandas Series, Categorical, or list-like object
    ordered : bool
        Whether to return an ordered categorical. By default a Categorical inputs'
        ordered setting is respected. Use this to override it.

    See Also
    --------
    fct_infreq : Order categories by value frequency count.

    Examples
    --------
    
    >>> fct = pd.Categorical(["c", "a", "b"])
    >>> fct
    ['c', 'a', 'b']
    Categories (3, object): ['a', 'b', 'c']

    Note that above the categories are sorted alphabetically. Use fct_inorder
    to keep the categories in first-observed order.

    >>> fct_inorder(fct)
    ['c', 'a', 'b']
    Categories (3, object): ['c', 'a', 'b']

    fct_inorder also accepts pd.Series and list objects:

    >>> fct_inorder(["z", "a"])
    ['z', 'a']
    Categories (2, object): ['z', 'a']

    By default, the ordered setting of categoricals is respected. Use the ordered
    parameter to override it.

    >>> fct2 = pd.Categorical(["z", "a", "b"], ordered=True)
    >>> fct_inorder(fct2)
    ['z', 'a', 'b']
    Categories (3, object): ['z' < 'a' < 'b']

    >>> fct_inorder(fct2, ordered=False)
    ['z', 'a', 'b']
    Categories (3, object): ['z', 'a', 'b']

    """

    if ordered is None:
        ordered = _get_cat_order(fct)

    if isinstance(fct, (pd.Series, pd.Categorical)):
        uniq = fct.dropna().unique()

        if isinstance(uniq, pd.Categorical):
            # the result of .unique for a categorical is a new categorical
            # unsurprisingly, it also sorts the categories, so reorder manually
            # (note that this also applies to Series[Categorical].unique())
            categories = uniq.categories[uniq.dropna().codes]
            return pd.Categorical(fct, categories, ordered=ordered)

        # series in, so series out
        cat = pd.Categorical(fct, uniq, ordered=ordered)
        return pd.Series(cat)

    ser = pd.Series(fct)
    return pd.Categorical(fct, categories = ser.dropna().unique(), ordered=ordered)


@symbolic_dispatch
def fct_infreq(fct, ordered=None):
    """Return a copy of fct, with categories ordered by frequency (largest first)

    Parameters
    ----------
    fct : list-like
        A pandas Series, Categorical, or list-like object
    ordered : bool
        Whether to return an ordered categorical. By default a Categorical inputs'
        ordered setting is respected. Use this to override it.

    See Also
    --------
    fct_inorder : Order categories by when they're first observed.

    Examples
    --------

    >>> fct_infreq(["c", "a", "c", "c", "a", "b"])
    ['c', 'a', 'c', 'c', 'a', 'b']
    Categories (3, object): ['c', 'a', 'b']

    """

    if ordered is None:
        ordered = _get_cat_order(fct)


    # sort and create new categorical ----
    
    if isinstance(fct, pd.Categorical):
        # Categorical value counts are sorted in categories order
        # So to acheive the exact same result as the Series case below,
        # we need to use fct_inorder, so categories is in first-observed order.
        # This orders the final result by frequency, and then observed for ties.
        freq = fct_inorder(fct).value_counts().sort_values(ascending=False)

        # note that freq is a Series, but it has a CategoricalIndex.
        # we want the index values as shown, so we need to strip them out of
        # this nightmare index situation.
        categories = freq.index.categories[freq.index.dropna().codes]
        return pd.Categorical(fct, categories=categories, ordered=ordered)

    else:
        # Series sorts in descending frequency order
        ser = pd.Series(fct) if not isinstance(fct, pd.Series) else fct
        freq = ser.value_counts()
        cat = pd.Categorical(ser, categories=freq.index, ordered=ordered)

        if isinstance(fct, pd.Series):
            return pd.Series(cat)

        return cat


# fct_reorder -----------------------------------------------------------------

@symbolic_dispatch
def fct_reorder(fct, x, func = np.median, desc = False) -> pd.Categorical:
    """Return copy of fct, with categories reordered according to values in x.
    
    Parameters
    ----------
    fct :
        A pandas.Categorical, or array(-like) used to create one.
    x :
        Values used to reorder categorical. Must be same length as fct.
    func :
        Function run over all values within a level of the categorical.
    desc :
        Whether to sort in descending order.

    Notes
    -----
    NaN categories can't be ordered. When func returns NaN, sorting
    is always done with NaNs last.


    Examples
    --------

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

    out = pd.Categorical(fct, categories=ordered.index)
    return _maybe_upcast(fct, out)


# fct_recode ------------------------------------------------------------------

@symbolic_dispatch
def fct_recode(fct, recat=None, **kwargs) -> pd.Categorical:
    """Return copy of fct with renamed categories.

    Parameters
    ----------
    fct :
        A pandas.Categorical, or array(-like) used to create one. 
    **kwargs :
        Arguments of form new_name = old_name.

    Examples
    --------
    >>> cat = ['a', 'b', 'c']
    >>> fct_recode(cat, z = 'c')
    ['a', 'b', 'z']
    Categories (3, object): ['a', 'b', 'z']

    >>> fct_recode(cat, x = ['a', 'b'])
    ['x', 'x', 'c']
    Categories (2, object): ['x', 'c']

    >>> fct_recode(cat, {"x": ['a', 'b']})
    ['x', 'x', 'c']
    Categories (2, object): ['x', 'c']
        
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
    return _maybe_upcast(fct, fct_collapse(fct, new_cats))


# fct_collapse ----------------------------------------------------------------

@symbolic_dispatch
def fct_collapse(fct, recat, group_other = None) -> pd.Categorical:
    """Return copy of fct with categories renamed. Optionally group all others.

    Parameters
    ----------
    fct :
        A pandas.Categorical, or array(-like) used to create one.  
    recat :
        Dictionary of form {new_cat_name: old_cat_name}. old_cat_name may be
        a list of existing categories, to be given the same name.
    group_other :
        An optional string, specifying what all other categories should be named.
        This will always be the last category level in the result.

    Notes
    -----
    Resulting levels index is ordered according to the earliest level replaced.
    If we rename the first and last levels to "c", then "c" is the first level.

    Examples
    --------
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
        new_fct = pd.Categorical(fct)
    else:
        new_fct = fct

    # each existing cat will map to a new one ----
    # need to know existing to new cat
    # need to know new cat to new code
    cat_to_new = {k: None for k in new_fct.categories}
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

    # move the other group to last in the ordered set
    if group_other is not None:
        try:
            del ordered_cats[group_other]
            ordered_cats[group_other] = True
        except KeyError:
            pass

    # map new category name to code
    new_cat_set = {k: ii for ii, k in enumerate(ordered_cats)}

    # at this point, we need remap codes to the other category
    # make an array, where the index is old code + 1 (so missing val index is 0)
    old_code_to_new = np.array(
            [-1] + [new_cat_set[new_cat] for new_cat in cat_to_new.values()]
    )

    # map old cats to new codes
    #remap_code = {old: new_cat_set[new] for old, new in cat_to_new.items()}
    new_codes = old_code_to_new[new_fct.codes + 1]
    new_cats = list(new_cat_set)

    out = pd.Categorical.from_codes(new_codes, new_cats)
    return _maybe_upcast(fct, out)


# fct_lump --------------------------------------------------------------------

@symbolic_dispatch
def fct_lump(fct, n = None, prop = None, w = None, other_level = "Other", ties = None) -> pd.Categorical:
    """Return a copy of fct with categories lumped together.

    Parameters
    ----------
    fct :
        A pandas.Categorical, or array(-like) used to create one.
    n :
        Number of categories to keep.
    prop :
        (not implemented) keep categories that occur prop proportion of the time.
    w :
        Array of weights corresponding to each value in fct.
    other_level :
        Name for all lumped together levels.
    ties :
        (not implemented) method to use in the case of ties.

    Notes
    -----
    Currently, one of n and prop must be specified.

    Examples
    --------
    >>> fct_lump(['a', 'a', 'b', 'c'], n = 1)
    ['a', 'a', 'Other', 'Other']
    Categories (2, object): ['a', 'Other']

    >>> fct_lump(['a', 'a', 'b', 'b', 'c', 'd'], prop = .2)
    ['a', 'a', 'b', 'b', 'Other', 'Other']
    Categories (3, object): ['a', 'b', 'Other']
        
    """

    if ties is not None:
        raise NotImplementedError("ties is not implemented")

    if n is None and prop is None:
        raise NotImplementedError("Either n or prop must be specified")

    keep_cats = _fct_lump_n_cats(fct, w, other_level, ties, n = n, prop = prop)

    out = fct_collapse(fct, {k:k for k in keep_cats}, group_other = other_level)
    return _maybe_upcast(fct, out)


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
    
    Parameters
    ----------
    fct :
        A pandas.Categorical, or array(-like) used to create one.

    Examples
    --------
    >>> fct = pd.Categorical(["a", "b", "c"])
    >>> fct
    ['a', 'b', 'c']
    Categories (3, object): ['a', 'b', 'c']

    >>> fct_rev(fct)
    ['a', 'b', 'c']
    Categories (3, object): ['c', 'b', 'a']

    Note that this function can also accept a list.

    >>> fct_rev(["a", "b", "c"])
    ['a', 'b', 'c']
    Categories (3, object): ['c', 'b', 'a']


    """

    if not isinstance(fct, pd.Categorical):
        fct = pd.Categorical(fct)

    rev_levels = list(reversed(fct.categories))

    out = fct.reorder_categories(rev_levels)
    return _maybe_upcast(fct, out)
