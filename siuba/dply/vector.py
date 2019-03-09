import pandas as pd
import numpy as np
from functools import singledispatch
from ..siu import Symbolic

def register_symbolic(f):
    @f.register(Symbolic)
    def _dispatch_symbol(__data, *args, **kwargs):
        return cls(Call("__call__", f, __data.source, *args, **kwargs))

    return f


def cumall(): pass

def cumany(): pass

def cummean(): pass

def desc(): pass

def dense_rank(): pass

def percent_rank(): pass

def min_rank(): pass

def cume_dist(): pass


@register_symbolic
@singledispatch
def row_number(x):
    return pd.Series(np.arange(len(x)))


def ntile(): pass

def cume_dist(): pass

def between(): pass

def coalesce(): pass

def lead(): pass

def lag(): pass

def n(): pass

def n_distinct(): pass

def na_if(): pass

def near(): pass

def nth(): pass

def first(): pass

def last(): pass

__ALL__ = ["row_number"]
