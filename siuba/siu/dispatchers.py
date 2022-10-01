# symbolic dispatch wrapper ---------------------------------------------------

from functools import singledispatch, update_wrapper, wraps
import inspect

from .calls import Call, FuncArg, MetaArg, Lazy, PipeCall, _Isolate
from .symbolic import Symbolic, create_sym_call, strip_symbolic

from typing import Callable

def _dispatch_not_impl(func_name):
    def f(x, *args, **kwargs):
        raise TypeError("singledispatch function {func_name} not implemented for type {type}"
                            .format(func_name = func_name, type = type(x))
                            )

    return f

def symbolic_dispatch(f = None, cls = object):
    """Return a generic dispatch function with symbolic data implementations.

    The function dispatches (Call or Symbolic) -> FuncArg.

    Parameters
    ----------
    cls :
        A class to dispatch on.
    f :
        A function to call if no classes match while dispatching.

    Examples
    --------

    Here is an example of running separate add functions on integers and strings.

    >>> @symbolic_dispatch(cls = int)
    ... def add1(x): return x + 1
    
    >>> @add1.register(str)
    ... def _add1_str(x): return int(x) + 1

    >>> add1(1)
    2

    >>> add1("1")
    2

    Note that passing a symbolic causes it to return a symbolic, so you can continue
    creating expressions.

    >>> from siuba.siu import _
    >>> type(add1(_.a.b) + _.c.d)
    <class 'siuba.siu.symbolic.Symbolic'>
    
    symbolic dispatch raises a NotImplementedError by default if it no function ``f``
    is passed. However, you can override the default as follows:
    
    >>> @symbolic_dispatch
    ... def my_func(x): raise NotImplementedError("some error message")

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

def pipe_no_args(f):
    """Register a concrete function that handles when a verb received no arguments."""
    @f.register(NoArgs)
    def wrapper(__data, *args, **kwargs):
        # e.g. head() -> Pipeable(_ -> head(_))
        return create_pipe_call(f, MetaArg("_"), *args, **kwargs)

    return f

def register_pipe(f, cls):
    """Register a concrete function that returns a Pipeable when called."""
    @f.register(cls)
    def wrapper(*args, **kwargs):
        return create_pipe_call(f, MetaArg("_"), *args, **kwargs)
    return f

def register_pipe_call(f):
    """Register a concrete function that ."""
    @f.register(Call)
    def f_dispatch(__data, *args, **kwargs):
        call = __data
        if isinstance(call, MetaArg):
            # single _ passed as first arg to function
            # e.g. mutate(_, _.id) -> Pipeable(_ -> mutate(_, _.id))
            return create_pipe_call(f, call, *args, **kwargs)
        else:
            # more complex _ expr passed as first arg to function
            # e.g. mutate(_.id) -> Pipeable(_ -> mutate(_, _.id))
            return create_pipe_call(f, MetaArg("_"), call, *args, **kwargs)

    return f



# option: no args, custom dispatch (e.g. register NoArgs)
# strips symbols
def verb_dispatch(cls, f = None):
    """Wrap singledispatch. Making sure to keep its attributes on the wrapper.
    
    This wrapper has three jobs:
        1. strip symbols off of calls
        2. pass NoArgs instance for calls like some_func(), so dispatcher can handle
        3. return a Pipeable when the first arg of a call is a symbol

    Parameters
    ----------
    cls :
        A class to dispatch on.
    f : 
        A function to call if no classes match while dispatching.
    """

    # classic way of allowing args to a decorator
    if f is None:
        return lambda f: verb_dispatch(cls, f)

    # initially registers func for object, so need to change to pd.DataFrame
    dispatch_func = singledispatch(f)
    if isinstance(cls, tuple):
        for c in cls: dispatch_func.register(c, f)
    else:
        dispatch_func.register(cls, f)
    # then, set the default object dispatcher to create a pipe
    register_pipe(dispatch_func, object)

    # register dispatcher for Call, and NoArgs
    register_pipe_call(dispatch_func)
    pipe_no_args(dispatch_func)

    @wraps(dispatch_func)
    def wrapper(*args, **kwargs):
        strip_args = map(strip_symbolic, args)
        strip_kwargs = {k: strip_symbolic(v) for k,v in kwargs.items()}

        if not args:
            return dispatch_func(NoArgs(), **strip_kwargs)

        return dispatch_func(*strip_args, **strip_kwargs)

    return wrapper

# TODO: deprecate / remove singledispatch2
singledispatch2 = verb_dispatch


# Pipe ========================================================================

class Pipeable:
    """DEPRECATED: please use the siuba.siu.call function.

    Enable function composition through the right bitshift (>>) operator.

    Parameters
    ----------
    f :
        A function to be called.
    calls : sequence, optional
        A list-like of functions to be called, with each result chained into the next.

    Examples
    --------

    >>> f = lambda x: x + 1

    Eager evaluation:

    >>> 1 >> Pipeable(f)
    2

    Defer to a pipe:

    >>> p = Pipeable(f) >> Pipeable(f)
    >>> 1 >> p
    3

    >>> p_undo = p >> (lambda x: x - 3)
    >>> 1 >> p_undo
    0

    >>> from siuba.siu import _
    >>> p_undo_sym = p >> (_ - 3)
    >>> 1 >> p_undo_sym
    0

    """

    def __init__(self, f = None, calls = None):
        import warnings
        warnings.warn("Pipeable is deprecated. Please use siuba.siu.call.")
        # symbolics like _.some_attr need to be stripped down to a call, because
        # calling _.some_attr() returns another symbolic.
        f = strip_symbolic(f)

        if f is not None:
            if calls is not None: raise Exception()
            self.calls = [f]
        elif calls is not None:
            self.calls = calls

    def __rshift__(self, x) -> "Pipeable":
        """Defer evaluation when pipe is on the left (lazy piping)."""
        if isinstance(x, Pipeable):
            return Pipeable(calls = self.calls + x.calls)
        elif isinstance(x, (Symbolic, Call)):
            call = strip_symbolic(x)
            return Pipeable(calls = self.calls + [call])
        elif callable(x):
            return Pipeable(calls = self.calls + [x])

        raise Exception()

    def __rrshift__(self, x):
        """Potentially evaluate result when pipe is on the right (eager piping).

        This function handles two cases:
            * callable >> pipe -> pipe
            * otherwise, evaluate the pipe

        """
        if isinstance(x, (Symbolic, Call)):
            call = strip_symbolic(x)
            return Pipeable(calls = [call] + self.calls)
        elif callable(x):
            return Pipeable(calls = [x] + self.calls)

        return self(x)

    def __call__(self, x):
        """Evaluate a pipe and return the result.

        Parameters
        ----------
        x :
            An object to be passed into the first function in the pipe.

        """
        res = x
        for f in self.calls:
            res = f(res)
        return res

def _prep_isolate_args(*args):
    result = []
    for ii, arg in enumerate(args):
        if ii == 0:
            result.append(strip_symbolic(arg))
        else:
            result.append(_Isolate(strip_symbolic(arg)))

    return result


def create_pipe_call(obj, *args, **kwargs) -> Call:
    """Return a Call of a function on its args and kwargs, wrapped in a Pipeable."""

    stripped_args = _prep_isolate_args(*args)

    return Call(
            "__call__",
            strip_symbolic(obj),
            *stripped_args,
            **{k: _Isolate(strip_symbolic(v)) for k,v in kwargs.items()}
            )

def create_eager_pipe_call(obj, *args, **kwargs) -> Call:

    return Call(
            "__call__",
            strip_symbolic(obj),
            *map(strip_symbolic, args),
            **{k: strip_symbolic(v) for k,v in kwargs.items()}
            )


def call(__func: "Callable | Call | Symbolic", *args, **kwargs):
    """Allow a function call to be used in a call (with >>).

    Parameters
    ----------
    __func:
        A function to be called as part of a call.
    *args:
        Additional position arguments to pass to the function.
    **kwargs:
        Additional keyword arguments to pass to the function.

    Examples
    --------

    The simplest use of the call is passing just the to-be-called function.

    >>> s = "a string"
    >>> s >> call(print)
    a string

    This is equivalent to explicitly passing ``_`` as a placeholder.

    >>> from siuba import _
    >>> s >> call(print, _)
    a string

    The explicit syntax is useful, because it allows us to pass more arguments.
    For example, the code below passes additional arguments to print.

    >>> "a" >> call(print, _, "string", sep=" ")
    a string

    You can transform the input data. For example, the code below passes "shout".upper()
    to print.

    >>> "shout" >> call(print, _.upper())
    SHOUT

    Since ``_`` is just a placeholder for the data on the left-hand-side of >>, you
    can pass it multiple times to the to-be-called function (e.g. print).

    >>> "nice" >> call(print, _, _, sep=" ")
    nice nice

    Alternatively, you can pass a siu expression straight to call.

    >>> "abc" >> call(_[0].upper())
    'A'
        
    """

    if isinstance(__func, (Symbolic, Call)):
        if args or kwargs:
            raise NotImplementedError(
                "If a siu expression (e.g. _) is the first argument to call, it must "
                "be the only argument. You can pass arguments using the form, "
                "call(_.some_method(1, 2, c = 3))."
            )
        return strip_symbolic(__func)
    if not args and not kwargs:
        # handle implicit case, call(some_func) -> call(some_func, _)
        return create_eager_pipe_call(__func, MetaArg("_"))

    return create_eager_pipe_call(__func, *args, **kwargs)


def pipe(__data, *args: Callable):
    """Pipe data through a chain of callables. Return the final result.

    Examples
    --------

    Case 1: pipe regular functions

    >>> pipe({"a": 1}, lambda x: x["a"], lambda x: x + 1)
    2

    Case 2: pipe to siu expressions

    >>> from siuba import _
    >>> pipe({"a": 1}, _["a"], _ + 1)
    2

    Case 3: call external function on siu expression

    >>> from siuba.siu import call
    >>> pipe({"a": 1}, call(isinstance, _["a"], int))
    True

    Case 4: _ as first arg to delay

    >>> f = pipe(_, lambda x: x["a"])
    >>> f
    PipeCall(...)

    >>> f({"a": 1})
    1

    Example: using with verb

    >>> from siuba import _, summarize
    >>> from siuba.data import mtcars
    >>> pipe(mtcars, summarize(res = _.hp.mean()))
            res
    0  146.6875
    """

    stripped = strip_symbolic(__data)

    # Special case: support backwards compatibility with old pipe() behavior ----
    # call() and Call.__rrshift__ now handle this behavior.
    if len(args) == 0:
        if isinstance(stripped, Call):
            return stripped
        else:
            return call(stripped)
        

    # When data is _, return a pipe call
    pipe_call = PipeCall(stripped, *map(strip_symbolic, args))

    if isinstance(stripped, MetaArg):
        return pipe_call

    return pipe_call()
