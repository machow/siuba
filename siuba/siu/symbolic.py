from functools import singledispatch

from .calls import Call, BinaryOp, BinaryRightOp, MetaArg, UnaryOp, SliceOp, FuncArg
from .format import Formatter

# Symbolic
# =============================================================================

def create_binary_op(op_name, left_op = True):
    def _binary_op(self, x):
        if left_op:
            node = BinaryOp(op_name, strip_symbolic(self), strip_symbolic(x))
        else:
            node = BinaryRightOp(op_name, strip_symbolic(self), strip_symbolic(x))

        return self.__class__(node, ready_to_call = True)
    return _binary_op

def create_unary_op(op_name):
    def _unary_op(self):
        node = UnaryOp(op_name, strip_symbolic(self))

        return self.__class__(node, ready_to_call = True)

    return _unary_op


class Symbolic(object):
    def __init__(self, source = None, ready_to_call = False):
        self.__source = MetaArg("_") if source is None else source
        self.__ready_to_call = ready_to_call

    def __array_ufunc__(self, ufunc, method, *inputs, **kwargs):
        """Handle numpy universal functions. E.g. np.sqrt(_)."""
        return array_ufunc(self, ufunc, method, *inputs, **kwargs)

    def __array_function__(self, func, types, args, kwargs):
        return array_function(self, func, types, *args, **kwargs)

    # since we override __eq__, we must explicitly set the hash method back to default
    __hash__ = object.__hash__

    # allowed methods ----

    def __getattr__(self, x):
        # temporary hack working around ipython pretty.py printing
        #if x == "__class__": return Symbolic

        return Symbolic(BinaryOp(
                "__getattr__",
                self.__source,
                strip_symbolic(x)
                ))
                

    def __call__(self, *args, **kwargs) -> "Symbolic":
        if self.__ready_to_call:
            return self.__source(*args, **kwargs)

        return create_sym_call(self.__source, *args, **kwargs)

    def __getitem__(self, x):
        return Symbolic(BinaryOp(
                "__getitem__",
                self.__source,
                slice_to_call(x),
                ),
                ready_to_call = True)

    
    def __invert__(self):
        if isinstance(self.__source, Call) and self.__source.func == "__invert__":
            return self.__source.args[0]
        else: 
            return self.__op_invert()


    def __op_invert(self):
        return Symbolic(UnaryOp('__invert__', self.__source), ready_to_call = True)

    def __rshift__(self, x):

        # Note that this and __rrshift__ are copied from Call
        stripped = strip_symbolic(x)

        if isinstance(stripped, Call):
            lhs_call = self.__source
            return Call._construct_pipe(MetaArg("_"), lhs_call, stripped)
        # strip_symbolic(self)(x)
        # x is a symbolic
        raise NotImplementedError("Symbolic may only be used on right-hand side of >> operator.")

    def __rrshift__(self, x):
        if isinstance(x, (Symbolic, Call)):
            raise NotImplementedError()

        return strip_symbolic(self)(x)


    # banned methods ----

    __contains__ = None
    __iter__ = None

    def __bool__(self):
        raise TypeError("Symbolic objects can not be converted to True/False, or used "
                        "with these keywords: not, and, or.")


    # representation ----
        
    def __repr__(self):
        return Formatter().format(self.__source)

    # unary operators ----
    # note that __invert__ is handled in a custom way above
    __neg__ = create_unary_op("__neg__")
    __pos__ = create_unary_op("__pos__")
    __abs__ = create_unary_op("__abs__")


    # binary operators ----
    __add__ = create_binary_op("__add__")
    __sub__ = create_binary_op("__sub__")
    __mul__ = create_binary_op("__mul__")
    __matmul__ = create_binary_op("__matmul__")
    __truediv__ = create_binary_op("__truediv__")
    __floordiv__ = create_binary_op("__floordiv__")
    __mod__ = create_binary_op("__mod__")
    __divmod__ = create_binary_op("__divmod__")
    __pow__ = create_binary_op("__pow__")
    __lshift__ = create_binary_op("__lshift__")
    __and__ = create_binary_op("__and__")
    __xor__ = create_binary_op("__xor__")
    __or__ = create_binary_op("__or__")
    __gt__ = create_binary_op("__gt__")
    __lt__ = create_binary_op("__lt__")
    __eq__ = create_binary_op("__eq__")
    __ne__ = create_binary_op("__ne__")
    __ge__ = create_binary_op("__ge__")
    __le__ = create_binary_op("__le__")


    __radd__ = create_binary_op("__radd__", False)
    __rsub__ = create_binary_op("__rsub__", False)
    __rmul__ = create_binary_op("__rmul__", False)
    __rmatmul__ = create_binary_op("__rmatmul__", False)
    __rtruediv__ = create_binary_op("__rtruediv__", False)
    __rfloordiv__ = create_binary_op("__rfloordiv__", False)
    __rmod__ = create_binary_op("__rmod__", False)
    __rdivmod__ = create_binary_op("__rdivmod__", False)
    __rpow__ = create_binary_op("__rpow__", False)
    __rlshift__ = create_binary_op("__rlshift__", False)
    __rand__ = create_binary_op("__rand__", False)
    __rxor__ = create_binary_op("__rxor__", False)
    __ror__ = create_binary_op("__ror__", False)


def create_sym_call(__source, *args, **kwargs):
    return Symbolic(Call(
            "__call__",
            strip_symbolic(__source),
            *map(strip_symbolic, args),
            **{k: strip_symbolic(v) for k,v in kwargs.items()}
            ),
            ready_to_call = True)


def slice_to_call(x):

    # TODO: uses similar code to SliceOp. make a walk_slice function?
    def f_strip(s):
        if isinstance(s, slice):
            return slice(*map(strip_symbolic, (s.start, s.stop, s.step)))

        return strip_symbolic(s)


    if isinstance(x, tuple):
        arg = tuple(map(f_strip, x))
    else:
        arg = f_strip(x)

    return SliceOp("__siu_slice__", arg)


def strip_symbolic(x):
    if isinstance(x, Symbolic):
        return x.__dict__["_Symbolic__source"]

    return x


def explain(symbol):
    """Print representation that resembles code used to create symbol."""
    if isinstance(symbol, Symbolic):
        return str(strip_symbolic(symbol))

    return str(symbol)


# Special numpy ufunc dispatcher
# =============================================================================
# note that this is essentially what dispatchers.symbolic_dispatch does...
# details on numpy array dispatch: https://github.com/numpy/numpy/issues/21387

@singledispatch
def array_function(self, func, types, *args, **kwargs):
    return func(*args, **kwargs)


@array_function.register(Call)
def _array_function_call(self, func, types, *args, **kwargs):
    return Call("__call__", FuncArg(array_function), self, func, types, *args, **kwargs)


@array_function.register(Symbolic)
def _array_function_sym(self, func, types, *args, **kwargs):
    f_concrete = array_function.dispatch(Call)

    call = f_concrete(
        strip_symbolic(self),
        func,
        types, 
        *map(strip_symbolic, args),
        **{k: strip_symbolic(v) for k, v in kwargs.items()}
    )

    return Symbolic(call)


@singledispatch
def array_ufunc(self, ufunc, method, *inputs, **kwargs):
    return getattr(ufunc, method)(*inputs, **kwargs)

@array_ufunc.register(Call)
def _array_ufunc_call(self, ufunc, method, *inputs, **kwargs):

    return Call("__call__", FuncArg(array_ufunc), self, ufunc, method, *inputs, **kwargs)


@array_ufunc.register(Symbolic)
def _array_ufunc_sym(self, ufunc, method, *inputs, **kwargs):
    f_concrete = array_ufunc.dispatch(Call)

    call = f_concrete(
        strip_symbolic(self),
        ufunc,
        method, 
        *map(strip_symbolic, inputs),
        **{k: strip_symbolic(v) for k, v in kwargs.items()}
    )

    return Symbolic(call)


# Do some gnarly method setting on Symbolic -----------------------------------
# =============================================================================

