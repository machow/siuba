import itertools
import operator

from abc import ABC

# TODO: symbolic formatting: __add__ -> "+"

BINARY_LEVELS = {
        "__add__": 2,
        "__sub__": 2,

        "__mul__": 1,
        "__matmul__": 1,
        "__truediv__": 1,
        "__floordiv__": 1,
        "__mod__": 1,
        "__divmod__": 1,

        "__pow__": 0,

        "__lshift__": 3,
        "__rshift__": 3,

        "__and__": 4,
        "__xor__": 4,
        "__or__": 4,
        "__gt__": 5,
        "__lt__": 5,
        "__eq__": 5,
        "__ne__": 5,
        "__ge__": 5,
        "__le__": "5",

        "__getattr__": 0,
        "__getitem__": 0,
        }

BINARY_OPS = {
        "__add__": "+",
        "__sub__": "-",
        "__mul__": "*",
        "__matmul__": "@",
        "__truediv__": "/",
        "__floordiv__": "//",
        "__mod__": "%",
        "__divmod__": "IDK",
        "__pow__": "**",
        "__lshift__": "<<",
        "__rshift__": ">>",
        "__and__": "&",
        "__xor__": "^",
        "__or__": "|",
        "__gt__": ">",
        "__lt__": "<",
        "__eq__": "==",
        "__ne__": "!=",
        "__ge__": ">=",
        "__le__": "<=",
        "__getattr__": ".",
        "__getitem__": "[",
        }

UNARY_OPS = {
        "__invert__": "~",
        "__neg__": "-",
        "__pos__": "+",
        "__abs__": "ABS => "
        }

# TODO: can it just be put in binary ops? Special handling in Symbolic class?
ALL_OPS = {**BINARY_OPS, **UNARY_OPS}

BINARY_RIGHT_OPS = {}
for k, v in BINARY_OPS.items():
    BINARY_RIGHT_OPS[k.replace("__", "__r", 1)] = k

for k, v in BINARY_LEVELS.copy().items():
    BINARY_LEVELS[k.replace("__", "__r", 1)] = v

# Functions
# ==============================================================================

def str_to_getitem_call(x):
    return Call("__getitem__", MetaArg("_"), x)
    

# Calls
# =============================================================================

class Call:
    """Represent python operations.

    This class is responsible for representing the pieces of a python expression,
    as a function, along with its args and kwargs.

    For example, "some_object.a" would be represented at the function "__getattr__",
    with the args `some_object`, and `"a"`.

    Parameters
    ----------
    func :
        Name of the function called. Class methods are represented using the names
        they have when defined on the class.
    *args :
        Arguments the function call uses.
    **kwargs :
        Keyword arguments the function call uses.

    Examples
    --------
    >>> Call("__add__", 1, 1)
    (1 + 1)

    See Also
    --------
    siuba.siu.Symbolic : Helper class for creating Calls.


    """
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __repr__(self):
        """Return a (best guess) python code representation of the Call.

        Note that this is not necessarily valid python code (e.g. if a python
        function is passed as a call argument).
        """
        # TODO: format binary, unary, call, associative
        if self.func in BINARY_OPS:
            op_repr = BINARY_OPS[self.func]
            fmt = "({args[0]} {func} {args[1]})"
        elif self.func in UNARY_OPS:
            op_repr = UNARY_OPS[self.func]
            fmt = "({func}{args[0]})"
        elif self.func == "getattr":
            op_repr = "."
            fmt = "({args[0]}{func}{args[1]})"
        else:
            op_repr, *arg_str = map(repr, self.args)
            kwarg_str = (str(k) + " = " + repr(v) for k,v in self.kwargs.items())

            combined_arg_str = ",".join(itertools.chain(arg_str, kwarg_str))
            fmt = "{}({})".format(op_repr, combined_arg_str)
            return fmt

        return fmt.format(
                    func = op_repr or self.func,
                    args = self.args,
                    kwargs = self.kwargs
                    )

    def __call__(self, x):
        """Evaluate a call over some context and return the result.

        Note that subclasses like MetaArg, simply return the context, so that a call
        acts like a unary function, with ``x`` as its argument.

        Parameters
        ----------
        x :
            Object passed down the call tree as context.

        Examples
        --------
        >>> expr = Call("__add__", MetaArg("_"), 2)
        >>> expr(1)   # 1 + 2
        3

        
        """
        args, kwargs = self.map_subcalls(self.evaluate_calls, args = (x,))
        inst, *rest = args

        #inst, *rest = (self.evaluate_calls(arg, x) for arg in self.args)
        #kwargs = {k: self.evaluate_calls(v, x) for k, v in self.kwargs.items()}
        
        # TODO: temporary workaround, for when only __get_attribute__ is defined
        if self.func == "__getattr__":
            return getattr(inst, *rest)
        elif self.func == "__getitem__":
            return operator.getitem(inst, *rest)
        elif self.func == "__call__":
            return getattr(inst, self.func)(*rest, **kwargs)

        # in normal case, get method to call, and then call it
        f_op = getattr(operator, self.func)
        return f_op(inst, *rest, **kwargs)

    @staticmethod
    def evaluate_calls(arg, x):
        if isinstance(arg, Call): return arg(x)

        return arg

    def copy(self) -> "Call":
        """Return a copy of this call object.

        Note that copies are made of child calls, but not their arguments.
        """
        args, kwargs = self.map_subcalls(lambda child: child.copy())
        return self.__class__(self.func, *args, **kwargs)

    def map_subcalls(self, f, args = tuple(), kwargs = None):
        """Call a function on all child calls.

        Parameters
        ----------
        f :
            A function to call on any child calls.
        args:
            Optional position arguments to pass to ``f``.
        kwargs:
            Optional keyword arguments to pass to ``f``.

        Returns
        -------
        A tuple of (new_args, new_kwargs) that can be used to recreate the original
        Call object with transformed (copies of) child nodes.

        See Also
        --------
        copy : Recursively calls map_subcalls to clone a call tree.

        """
        if kwargs is None: kwargs = {}

        new_args = tuple(f(arg, *args, **kwargs) if isinstance(arg, Call) else arg for arg in self.args)
        new_kwargs = {k: f(v, *args, **kwargs) if isinstance(v, Call) else v for k,v in self.kwargs.items()}

        return new_args, new_kwargs

    def map_replace(self, f):
        args, kwargs = self.map_subcalls(f)
        return self.__class__(self.func, *args, **kwargs)

    def iter_subcalls(self, f):
        yield from iter(arg for arg in self.args if instance(arg, Call))
        yield from iter(v for k,v in self.kwargs.items() if isinstance(v, Call))

    def op_vars(self, attr_calls = True):
        """Return set of all variable names used in Call
        
        Args:
            attr_calls: whether to include called attributes (e.g. 'a' from _.a())
        """
        varnames = set()

        op_var = self._get_op_var()
        if op_var is not None:
            varnames.add(op_var)

        if (not attr_calls
            and self.func == "__call__"
            and isinstance(self.args[0], Call)
            and self.args[0].func == "__getattr__"
            ):
            # skip obj, since it fetches an attribute this node is calling
            prev_obj, prev_attr = self.args[0].args
            all_args = itertools.chain([prev_obj], self.args[1:], self.kwargs.values())
        else:
            all_args = itertools.chain(self.args, self.kwargs.values())


        for arg in all_args:
            if isinstance(arg, Call):
                varnames.update(arg.op_vars(attr_calls = attr_calls))

        return varnames
    
    def _get_op_var(self):
        if self.func in ("__getattr__", "__getitem__") and isinstance(self.args[1], str):
            return self.args[1]

    def obj_name(self):
        obj = self.args[0]
        if isinstance(obj, Call):
            if obj.func == "__getattr__":
                return obj.args[0]
        elif hasattr(obj, '__name__'):
            return obj.__name__

        return None


class Lazy(Call):
    """Lazily return calls rather than evaluating them."""
    def __init__(self, func, arg = None):
        if arg is None:
            self.func = "<lazy>"
            self.args = [func]
        else:
            # will happen in generic node calls, e.g. self.__class__(self.func, ...)
            self.func = func
            self.args = [arg]

        self.kwargs = {}

    def __call__(self, x, *args, **kwargs):
        return self.args[0]



class UnaryOp(Call):
    """Represent unary call operations."""
    def __repr__(self):
        fmt = "{func}{args[0]}"

        func = UNARY_OPS[self.func]
        return fmt.format(func = func, args = self.args, kwargs = self.kwargs)


class BinaryOp(Call):
    """Represent binary call operations."""

    def __repr__(self):
        return self._repr(reverse = False)


    def _repr(self, reverse = False):
        func_name = self.get_func_name()

        level = BINARY_LEVELS[func_name]
        spaces = "" if level == 0 else " "

        args = self.args
        arg0 = "({args[0]})" if self.needs_paren(args[0]) else "{args[0]}"
        arg1 = "({args[1]})" if self.needs_paren(args[1]) else "{args[1]}"

        # handle binary ops that are not infix operators
        if self.func == "__getitem__":
            suffix = "]"
        else:
            suffix = ""

        # final, formatting
        fmt = arg0 + "{spaces}{func}{spaces}" + arg1 + suffix


        func = BINARY_OPS[func_name]
        if self.func == "__getattr__":
            # use bare string. eg _.a
            fmt_args = [repr(args[0]), args[1]]
        else:
            fmt_args = list(map(repr, args))
        
        if reverse:
            fmt_args = list(reversed(fmt_args))

        return fmt.format(func = func, args = fmt_args, spaces = spaces)

    def get_func_name(self):
        return BINARY_RIGHT_OPS.get(self.func, self.func)
        

    def needs_paren(self, x):
        if isinstance(x, BinaryOp):
            sub_lvl = BINARY_LEVELS[x.get_func_name()]
            level = BINARY_LEVELS[self.get_func_name()]
            if sub_lvl != 0 and sub_lvl > level:
                return True

        return False

class BinaryRightOp(BinaryOp):
    """Represent right associative binary call operations."""

    def __call__(self, x):
        inst, *rest = (self.evaluate_calls(arg, x) for arg in self.args)
        kwargs = {k: self.evaluate_calls(v, x) for k, v in self.kwargs.items()}
        
        # in normal case, get method to call, and then call it
        func_name = BINARY_RIGHT_OPS[self.func]
        f_op = getattr(operator, func_name)

        # TODO: in practice rest only has 1 item, but this is not enforced..
        return f_op(*rest, inst, **kwargs)

    def __repr__(self):
        return self._repr(reverse = True)


class DictCall(Call):
    """Calls representation of dictionary construction.

    Note that this class is important for two reasons:

      * Dictionary literal syntax cannot produce a call.
      * Calls cannot recognize subcalls nested inside containers. E.g. dicts, lists.
    """

    def __init__(self, f, *args, **kwargs):
        # TODO: validation, clean up class
        super().__init__(f, *args, **kwargs)
        self.args = (dict, dict(self.args[1]))

    def map_subcalls(self, f, args = tuple(), kwargs = None):
        if kwargs is None: kwargs = {}

        d = self.args[1]

        # TODO: don't use a giant comprehension
        new_d = {
            f(k, *args, **kwargs) if isinstance(k, Call) else k     # key part
            : f(v, *args, **kwargs) if isinstance(v, Call) else v   # val part
            for k,v in d.items()
            }

        return (self.args[0], new_d), {}

    def __call__(self, x):
        # TODO: note that it will not descend into dict to evaluate calls
        return self.args[1]


# Slice Calls -----------------------------------------------------------------
# note the metaclass SliceOp below, which registers SliceOpExt

class _SliceOpExt(Call):
    """Represent arguments for extended slice syntax.

    E.g. expressions like _[_:, 'a', 1:2, ] use a tuple of many slices.
    """

    def __init__(self, func, *args, **kwargs):
        self.func = "__siu_slice__"

        if kwargs:
            raise ValueError("a slice cannot accept keyword arguments")

        self.args = args
        self.kwargs = {}

    def __repr__(self):
        return ", ".join(map(self._repr_slice, self.args))

    def __call__(self, x):
        args, kwargs = self.map_subcalls(self.evaluate_calls, args = (x,))

        return args

    def map_subcalls(self, f, args=tuple(), kwargs=None):
        if kwargs is None: kwargs = {}

        # evaluate each argument, which can be a slice
        args = tuple(self._apply_slice_entry(f, obj, args, kwargs) for obj in self.args)

        return args, {}

    def op_vars(self, *args, **kwargs):
        def visit_op_vars(node, *args, **kwargs):
            if not isinstance(node, Call):
                return set()

            return node.op_vars(*args, **kwargs)

        results = set()
        for child in self.args:
            # TODO: refactor Call classes to use singledispatch, so we can call
            # op_vars directly on a slice object
            if isinstance(child, slice):
                for piece in (child.start, child.stop, child.step):
                    results.update(visit_op_vars(piece, *args, **kwargs))

            else:
                results.update(visit_op_vars(child, *args, **kwargs))

        return results

    @staticmethod
    def _apply_slice_entry(f, slice_, args, kwargs):
        if not isinstance(slice_, slice):
            # a slice argument can be simple literal. e.g. _['a']
            return f(slice_, *args, **kwargs) if isinstance(slice_, Call) else slice_

        slice_args = (slice_.start, slice_.stop, slice_.step)
        args = [
            f(entry, *args, **kwargs) if isinstance(entry, Call) else entry
            for entry in slice_args
            ]

        return slice(*args)

    @staticmethod
    def _unslice(slice_):
        if isinstance(slice_, slice):
            return (slice_.start, slice_.stop, slice_.step)

        return slice_

    @classmethod
    def _repr_slice(cls, slice_):
        if isinstance(slice_, slice):
            pieces = cls._unslice(slice_)

            # if the last piece is not specified, e.g. :1, then we want to represent
            # it like that, and not the equivalent :1:.
            if pieces[-1] is None:
                pieces = pieces[:-1]
        else:
            pieces = (slice_,)

        return ":".join(repr(x) if x is not None else "" for x in pieces)


class _SliceOpIndex(_SliceOpExt):
    """Special case of slicing, where getitem receives a single slice object."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        res = super().__call__(*args, **kwargs)
        return res[0]


class SliceOp(ABC):
    """Factory class for representing from single and extended slice calls. 

    Note that it has SliceOpIndex, and SliceOpExt registered as subclasses, so
    that this class can be used rather than the specific implementations.
    """

    def __new__(cls, func, *args, **kwargs):
        # must be constructed in the same way as __getitem__ sees the indexer.
        # that is, a single argument (that can be a tuple)
        if len(args) != 1:
            raise ValueError("SliceOpIndex allows 1 argument, but received %s" % len(args))

        elif isinstance(args[0], tuple):
            # general case, where calling returns a tuple of indexers.
            # e.g. _['a', ], or _[1:, :, :, :]
            return _SliceOpExt(func, *args[0])

        else:
            # special case, where calling returns a single indexer, rather than
            # a tuple of indexers (e.g. _['a'], or _[1:])
            return _SliceOpIndex(func, args[0])


SliceOp.register(_SliceOpExt)


# Special kinds of call arguments ----
# These functions insure that when using siu expressions generated by _,
# that call.args[0] is always another call. This allows them to trivially
# respond to calling a siu expression, map_subcalls, etc..
#
# In the future, could make a parent class for Call, with a restricted
# set of behavior similar to theirs.
#
# TODO: validate that call.args[0] is a Call in tree visitors?

class MetaArg(Call):
    """Represent an argument, by returning the argument passed to __call__."""

    def __init__(self, func, *args, **kwargs):
        self.func = "_"
        self.args = tuple()
        self.kwargs = {}

    def __repr__(self):
        return self.func

    def __call__(self, x):
        return x

class FuncArg(Call):
    """Represent a function to be called."""

    def __init__(self, func, *args, **kwargs):
        self.func = '__custom_func__'

        if func == '__custom_func__':
            func = args[0]

        self.args = tuple([func])
        self.kwargs = {}

    def __repr__(self):
        return repr(self.args[0])

    def __call__(self, x):
        return self.args[0]



