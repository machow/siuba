import pandas as pd
import numpy as np
from functools import singledispatch
import itertools

from ..siu import Symbolic, create_sym_call,Call


def register_symbolic(f):
    # TODO: don't use singledispatch if it has already been done
    f = singledispatch(f)
    @f.register(Symbolic)
    def _dispatch_symbol(__data, *args, **kwargs):
        return create_sym_call(f, __data.source, *args, **kwargs)

    return f

def _coerce_to_str(x):
    if isinstance(x, (pd.Series, np.ndarray)):
        return x.astype(str)
    elif not np.ndim(x) < 2:
        raise ValueError("np.ndim must be less than 2, but is %s" %np.ndim(x))

    return pd.Series(x, dtype = str)


@register_symbolic
def str_c(x, *args, sep = "", collapse = None):
    all_args = itertools.chain([x], args)
    strings = list(map(_coerce_to_str, all_args))

    return np.sum(strings, axis = 0)

