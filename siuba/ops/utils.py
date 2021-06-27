from siuba.siu import symbolic_dispatch
from types import SimpleNamespace

class Namespace(SimpleNamespace):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __iter__(self):
        return iter(self.__dict__)

    def __getitem__(self, x):
        return self.__dict__[x]

    def __len__(self):
        return len(self.__dict__)

    def keys(self):
        return self.__iter__()


def not_implemented(f):
    def _raise_not_impl(*args, **kwargs) -> NotImplementedError:
        raise NotImplementedError("Method %s not implemented" %f.__name__)

    return symbolic_dispatch(_raise_not_impl)

# register --------------------------------------------------------------------

def register(namespace, cls, **kwargs):
    """Register concrete methods on many dispatchers.

    Parameters:
        namespace: a mapping holding singledispatch functions
        cls: the class to being registered to
        kwargs: <name of generic> = <concrete method>

    """

    for k,v in kwargs.items():
        namespace[k].register(cls, v)

# Op --------------------------------------------------------------------------

class Operation:
    def __init__(self, name, kind, arity, is_property = False, accessor = None):
        self.name = name
        self.kind = kind
        self.arity = arity
        self.is_property = is_property
        self.accessor = accessor
    
def operation(name, *args):
    def op_method(self, *args, **kwargs):
        return default(self, op_method, args, kwargs)

    op_method.operation = Operation(name, *args)
    op_method.__name__ = name

    dispatcher = symbolic_dispatch(op_method)

    return dispatcher


# Default dispatcher ----------------------------------------------------------

@symbolic_dispatch
def default(self, generic, args = tuple(), kwargs = {}):
    raise NotImplementedError()


# TODO: move into pd fast groups code

import pandas as pd

@default.register(pd.Series)
def _default_pd_series(self, generic, args = tuple(), kwargs = {}):
    op = generic.operation
    if op.accessor is not None:
        method = getattr(getattr(self, op.accessor), op.name)
    else:
        method = getattr(self, op.name)


    if op.is_property:
        return method

    return method(*args, **kwargs)

