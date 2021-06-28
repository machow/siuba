# symbolic dispatch wrapper ---------------------------------------------------

from functools import singledispatch, update_wrapper
import inspect

from .calls import Call, FuncArg
from .symbolic import Symbolic, create_sym_call, strip_symbolic

def _dispatch_not_impl(func_name):
    def f(x, *args, **kwargs):
        raise TypeError("singledispatch function {func_name} not implemented for type {type}"
                            .format(func_name = func_name, type = type(x))
                            )

    return f

def symbolic_dispatch(f = None, cls = object):
    """Return a generic dispatch function with symbolic data implementations.

    The function dispatches (Call or Symbolic) -> FuncArg.

    Examples:
      ::

        @symbolic_dispatch(int)
        def add1(x): return x + 1
        
        @add1.register(str)
        def _add1_str: return int(x) + 1
        
        
        @symbolic_dispatch
        def my_func(x): raise NotImplementedError("no default method")

    """
    if f is None:
        return lambda f: symbolic_dispatch(f, cls)

    # TODO: don't use singledispatch if it has already been done
    dispatch_func = singledispatch(f)

    if cls is not object:
        dispatch_func.register(cls, f)
        dispatch_func.register(object, _dispatch_not_impl(dispatch_func.__name__))


    @dispatch_func.register(Symbolic)
    def _dispatch_symbol(__data, *args, **kwargs):
        return create_sym_call(FuncArg(dispatch_func), strip_symbolic(__data), *args, **kwargs)

    @dispatch_func.register(Call)
    def _dispatch_call(__data, *args, **kwargs):
        # TODO: want to just create call, for now use hack of creating a symbolic
        #       call and getting the source off of it...
        return strip_symbolic(create_sym_call(FuncArg(dispatch_func), __data, *args, **kwargs))

    return dispatch_func


    

