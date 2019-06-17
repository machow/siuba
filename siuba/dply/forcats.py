import pandas as pd
import numpy as np
from ..siu import create_sym_call, Symbolic
from functools import singledispatch

# TODO: move into siu
def register_symbolic(f):
    @f.register(Symbolic)
    def _dispatch_symbol(__data, *args, **kwargs):
        return create_sym_call(f, __data.source, *args, **kwargs)

    return f


# fct_reorder -----------------------------------------------------------------

@register_symbolic
@singledispatch
def fct_reorder(fct, x, func = np.median):
    x_vals = x.values if isinstance(x, pd.Series) else x
    s = pd.Series(x_vals, index = fct)

    # for each cat, calc agg func, make values of ordered the codes
    ordered = s.groupby(level = 0).agg(func).sort_values()
    ordered[:] = np.arange(len(ordered))
    codes = ordered[s.index.values]
    return pd.Categorical.from_codes(codes, list(ordered.index))


# fct_recode ------------------------------------------------------------------

@register_symbolic
@singledispatch
def fct_recode(fct, **kwargs):
    if not isinstance(fct, pd.Categorical):
        fct = pd.Categorical(fct)

    rev_kwargs = {v:k for k,v in kwargs.items()}
    return fct.rename_categories(rev_kwargs)


# fct_collapse ----------------------------------------------------------------

@register_symbolic
@singledispatch
def fct_collapse(fct, recat, group_other = None):
    if not isinstance(fct, pd.Categorical):
        fct = pd.Categorical(fct)

    # each existing cat will map to a new one ----
    # need to know existing to new cat
    # need to know new cat to new code
    cat_to_new = {k: None for k in fct.categories}
    new_cat_set = {k: True for k in fct.categories} 
    for new_name, v in recat.items():
        v = [v] if not np.ndim(v) else v
        for old_name in v:
            if cat_to_new[old_name] is not None:
                raise Exception("category %s was already re-assigned"%old_name)
            cat_to_new[old_name] = new_name
            del new_cat_set[old_name]
            new_cat_set[new_name] = True    # add new cat

    # collapse all unspecified cats to group_other if specified ----
    for k, v in cat_to_new.items():
        if v is None:
            if group_other is not None:
                new_cat_set[group_other] = True
                cat_to_new[k] = group_other
                del new_cat_set[k]
            else:
                cat_to_new[k] = k

    # map from old cat to new code ----
    # calculate new codes
    new_cat_set = {k: ii for ii, k in enumerate(new_cat_set)}
    # map old cats to them
    remap_code = {old: new_cat_set[new] for old, new in cat_to_new.items()}

    new_codes = fct.map(remap_code)
    new_cats = list(new_cat_set.keys())
    return pd.Categorical.from_codes(new_codes, new_cats)


# fct_lump --------------------------------------------------------------------

@register_symbolic
@singledispatch
def fct_lump(fct, n = None, prop = None, w = None, other_level = "Other", ties = None):
    if ties is not None:
        raise NotImplementedError("ties is not implemented")
    
    if n is None and prop is None:
        raise NotImplementedError("Either n or prop must be specified")

    if prop is not None:
        raise NotImplementedError("prop arg is not implemented")

    keep_cats = _fct_lump_n_cats(fct, n, w, other_level, ties)
    return fct_collapse(fct, {k:k for k in keep_cats}, group_other = other_level) 

def _fct_lump_n_cats(fct, n, w, other_level, ties):
    # TODO: currently always selects n, even if ties
    ascending = n < 0
    arr = _get_values(w) if w is not None else 1
    ser = pd.Series(arr, index = fct)
    sorted_arr = ser.groupby(level = 0).sum().sort_values(ascending = ascending)
    return sorted_arr.iloc[:abs(n)].index.values

def _get_values(x):
    # TODO: move into utility, note pandas now encouraging .array method
    if isinstance(x, pd.Series): return x.values

    return x


# fct_rev ---------------------------------------------------------------------

@register_symbolic
@singledispatch
def fct_rev(fct):
    if not isinstance(fct, pd.Categorical):
        fct = pd.Categorical(fct)

    rev_levels = list(reversed(fct.categories))

    return fct.reorder_categories(rev_levels)
    
