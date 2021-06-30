# symbolic dispatch wrapper ---------------------------------------------------

from functools import singledispatch, update_wrapper, wraps
import inspect

from .calls import Call, FuncArg, MetaArg, Lazy
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


# Verb dispatch  ==============================================================

class NoArgs: pass

def pipe_no_args(f, cls = NoArgs):
    @f.register(cls)
    def wrapper(__data, *args, **kwargs):
        return create_pipe_call(f, MetaArg("_"), *args, **kwargs)

    return f

def register_pipe(f, cls):
    @f.register(cls)
    def wrapper(*args, **kwargs):
        return create_pipe_call(f, MetaArg("_"), *args, **kwargs)
    return f


# option: no args, custom dispatch (e.g. register NoArgs)
# strips symbols
def singledispatch2(cls, f = None):
    """Wrap singledispatch. Making sure to keep its attributes on the wrapper.
    
    This wrapper has three jobs:
        1. strip symbols off of calls
        2. pass NoArgs instance for calls like some_func(), so dispatcher can handle
        3. return a Pipeable when the first arg of a call is a symbol
    """

    # classic way of allowing args to a decorator
    if f is None:
        return lambda f: singledispatch2(cls, f)

    # initially registers func for object, so need to change to pd.DataFrame
    dispatch_func = singledispatch(f)
    if isinstance(cls, tuple):
        for c in cls: dispatch_func.register(c, f)
    else:
        dispatch_func.register(cls, f)
    # then, set the default object dispatcher to create a pipe
    register_pipe(dispatch_func, object)

    # register dispatcher for Call, and NoArgs
    pipe_call(dispatch_func)
    pipe_no_args(dispatch_func)

    @wraps(dispatch_func)
    def wrapper(*args, **kwargs):
        strip_args = map(strip_symbolic, args)
        strip_kwargs = {k: strip_symbolic(v) for k,v in kwargs.items()}

        if not args:
            return dispatch_func(NoArgs(), **strip_kwargs)

        return dispatch_func(*strip_args, **strip_kwargs)

    return wrapper

# Pipe ========================================================================

# TODO: should be a subclass of Call?
class Pipeable:
    def __init__(self, f = None, calls = None):
        # symbolics like _.some_attr need to be stripped down to a call, because
        # calling _.some_attr() returns another symbolic.
        f = strip_symbolic(f)

        if f is not None:
            if calls is not None: raise Exception()
            self.calls = [f]
        else:
            self.calls = calls

    def __rshift__(self, x):
        """Handle >> syntax when pipe is on the left (lazy piping)."""
        if isinstance(x, Pipeable):
            return Pipeable(calls = self.calls + x.calls)
        elif isinstance(x, (Symbolic, Call)):
            call = strip_symbolic(x)
            return Pipeable(calls = self.calls + [call])
        elif callable(x):
            return Pipeable(calls = self.calls + [x])

        raise Exception()

    def __rrshift__(self, x):
        """Handle >> syntax when pipe is on the right (eager piping)."""
        if isinstance(x, (Symbolic, Call)):
            call = strip_symbolic(x)
            return Pipeable(calls = [call] + self.calls)
        elif callable(x):
            return Pipeable(calls = [x] + self.calls)

        return self(x)

    def __call__(self, x):
        res = x
        for f in self.calls:
            res = f(res)
        return res


def create_pipe_call(source, *args, **kwargs):
    first, *rest = args
    return Pipeable(Call(
            "__call__",
            strip_symbolic(source),
            strip_symbolic(first),
            *(Lazy(strip_symbolic(x)) for x in rest),
            **{k: Lazy(strip_symbolic(v)) for k,v in kwargs.items()}
            ))

def pipe_with_meta(f, *args, **kwargs):
    return create_pipe_call(f, MetaArg("_"), *args, **kwargs)


def pipe_call(f):
    @f.register(Call)
    def f_dispatch(__data, *args, **kwargs):
        call = __data
        if isinstance(call, MetaArg):
            # single _ passed as first arg to function
            # e.g. mutate(_, _.id)
            return create_pipe_call(f, call, *args, **kwargs)
        else:
            # more complex _ expr passed as first arg to function
            # e.g. mutate(_.id)
            return pipe_with_meta(f, call, *args, **kwargs)

    return f


