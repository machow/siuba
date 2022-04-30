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
    from siuba.siu.visitors import FunctionLookupBound
    # TODO: MC-NOTE - move FunctionLookupBound to dispatchers?

    msg = "No default singledispatch implemented for %s" % name
    dispatcher = symbolic_dispatch(FunctionLookupBound(msg))
    dispatcher.operation = Operation(name, *args)
    dispatcher.__name__ = name

    return dispatcher


# Default dispatcher ----------------------------------------------------------

def _register_series_default(generic):
    import pandas as pd
    from functools import partial

    generic.register(pd.Series, partial(_default_pd_series, generic.operation))

def _default_pd_series(__op, self, *args, **kwargs):
    # Once we drop python 3.7 dependency, could make __op position only
    if __op.accessor is not None:
        method = getattr(getattr(self, __op.accessor), __op.name)
    else:
        method = getattr(self, __op.name)


    if __op.is_property:
        return method

    return method(*args, **kwargs)

