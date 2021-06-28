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

class Formatter:
    def __init__(self): pass

    def format(self, call, pad = 0):
        """Return a Symbolic or Call back as a nice tree, with boxes for nodes."""

        fmt_block = "█─"
        fmt_pipe = "├─"
        
        # TODO: why are some nodes still symbolic?
        if isinstance(call, Symbolic):
            return self.format(strip_symbolic(call))

        if isinstance(call, MetaArg):
            return "_"

        if isinstance(call, Call):
            call_str = fmt_block + ALL_OPS.get(call.func, repr(call.func))

            args_str = [self.format(arg) for arg in call.args]

            # format keyword args, making sure "└─<key> = █─" aligns box's children
            kwargs_str = []
            for k, v in call.kwargs.items():
                kwargs_str.append(
                        k + " = " + self.format(v, pad = len(k) + 3)
                        )

            all_args = [*args_str, *kwargs_str]
            padded = []
            for ii, entry in enumerate(all_args):
                chunk = self.fmt_pipe(
                        entry,
                        final = ii == len(all_args) - 1,
                        pad = pad
                        )
                padded.append(chunk)

            return "".join([call_str, *padded])

        return repr(call)


    @staticmethod
    def fmt_pipe(x, final = False, pad = 0):
        if not final:
            connector = "│ " if not final else "  "
            prefix = "├─"
        else:
            connector = "  "
            prefix = "└─"

        connector = "\n" + " "*pad + connector
        prefix = "\n" + " "*pad + prefix
        # NOTE: because visiting is depth first, this is essentially prepending
        # the text to the left.
        return prefix + connector.join(x.splitlines())


# Calls
# =============================================================================

class Call:
    """
    Representation of python operations.

    This class is responsible for representing the pieces of a python expression,
    as a function, along with its args and kwargs.

    For example, "some_object.a" would be represented at the function "__getattr__",
    with the args `some_object`, and `"a"`.

    Args:
        func: name of the function called. Class methods are represented using the names
              they have when defined on the class.
        *args: arguments the func call uses.
        **kwargs: keyword arguments the func call uses.


    """
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __repr__(self):
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

    def copy(self):
        args, kwargs = self.map_subcalls(lambda child: child.copy())
        return self.__class__(self.func, *args, **kwargs)

    def map_subcalls(self, f, args = tuple(), kwargs = None):
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
    def __repr__(self):
        fmt = "{func}{args[0]}"

        func = UNARY_OPS[self.func]
        return fmt.format(func = func, args = self.args, kwargs = self.kwargs)


class BinaryOp(Call):

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
    """evaluates both keys and vals."""

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
    """Factory class for creating slice calls. 

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
    def __init__(self, func, *args, **kwargs):
        self.func = "_"
        self.args = tuple()
        self.kwargs = {}

    def __repr__(self):
        return self.func

    def __call__(self, x):
        return x

class FuncArg(Call):
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


# Trees and Visitors ==========================================================
from .error import ShortException

class FunctionLookupError(ShortException): pass

class FunctionLookupBound:
    def __init__(self, msg):
        self.msg = msg

    def __call__(self):
        raise NotImplementedError(self.msg)


class CallVisitor:
    """
    A node visitor base class that walks the call tree and calls a
    visitor function for every node found.  This function may return a value
    which is forwarded by the `visit` method.

    Note: essentially a copy of ast.NodeVisitor
    """

    def visit(self, node):
        """Visit a node."""
        method = 'visit_' + node.func
        visitor = getattr(self, method, self.generic_visit)
        return visitor(node)

    def generic_visit(self, node):
        """Called if no explicit visitor function exists for a node."""
        
        node.map_subcalls(self.visit)

    @classmethod
    def quick_visitor(cls, visit_dict, node):
        """Class method to quickly define and run a custom visitor."""

        qv = type('QuickVisitor', cls, visit_dict)
        qv.visit(node)

class CallListener:
    """Generic listener. Each exit is called on a node's copy."""
    def enter(self, node):
        method = 'enter_' + node.func
        f_enter = getattr(self, method, self.generic_enter)

        return f_enter(node)

    def exit(self, node):
        method = 'exit_' + node.func
        f_exit = getattr(self, method, self.generic_exit)
        return f_exit(node)

    def generic_enter(self, node):
        args, kwargs = node.map_subcalls(self.enter)

        return self.exit(node.__class__(node.func, *args, **kwargs))

    def generic_exit(self, node):
        return node

    def enter_if_call(self, x):
        if isinstance(x, Call):
            return self.enter(x)

        return x

def get_attr_chain(node, max_n):
    # TODO: need to make custom calls their own Call class, then will not have to
    #       do these kinds of checks, since a __call__ will always be on a Call obj
    if not isinstance(node, Call):
        return [], node

    out = []
    ttl_n = 0
    crnt_node = node
    while ttl_n < max_n:
        if crnt_node.func != "__getattr__": break

        obj, attr = crnt_node.args
        out.append(attr)

        ttl_n += 1
        crnt_node = obj

    return list(reversed(out)), crnt_node


from inspect import isclass, isfunction
from typing import get_type_hints
from .utils import is_dispatch_func_subtype

class CallTreeLocal(CallListener):
    def __init__(
            self,
            local,
            call_sub_attr = None,
            chain_sub_attr = False,
            dispatch_cls = None,
            result_cls = None,
            call_props = None
            ):
        """
        Arguments:
            local: a dictionary mapping func_name: func, used to replace call expressions.
            call_sub_attr: a set of attributes signaling any subattributes are property
                           methods. Eg. {'dt'} to signify in _.dt.year, year is a property call.
            chain_sub_attr: whether to included the attributes in the above argument, when looking up
                           up a replacement for the property call. E.g. does local have a 'dt.year' entry.
            dispatch_cls: if custom calls are dispatchers, dispatch on this class. If none, use their name
                          to try and get their corresponding local function.
            result_cls: if custom calls are dispatchers, require their result annotation to be a subclass
                          of this class.
            call_props: property methods to potentially convert to local calls.
        """
        self.local = local
        self.call_sub_attr = set(call_sub_attr or [])
        self.chain_sub_attr = chain_sub_attr
        self.dispatch_cls = dispatch_cls
        self.result_cls = result_cls
        self.call_props = set(call_props or [])

    def translate(self, expr):
        """Return the translation of an expression.

        This method is meant to be a high-level entrypoint.

        """

        # note that by contract, don't need to strip symbolic
        return self.enter(strip_symbolic(expr))

    def dispatch_local(self, name):
        func = self.local[name]

        if hasattr(func, "dispatch") and self.result_cls is not None:
            return func.dispatch(self.result_cls)

        return func

    def create_local_call(self, name, prev_obj, cls, func_args = None, func_kwargs = None):
        # need call attr name (arg[0].args[1]) 
        # need call arg and kwargs
        func_args = tuple() if func_args is None else func_args
        func_kwargs = {} if func_kwargs is None else func_kwargs

        try:
            local_func = self.dispatch_local(name)
        except KeyError as err:
            raise FunctionLookupError("Missing translation for function call: %s"% name)

        if isinstance(local_func, FunctionLookupBound):
            raise FunctionLookupError(local_func.msg)
            #local_func()

        #if isclass(local_func) and issubclass(local_func, Exception):
        #    raise local_func

        return cls(
                "__call__",
                local_func,
                prev_obj,
                *func_args,
                **func_kwargs
                )

    def enter(self, node):
        # if no enter metthod for operators, like __invert__, try to get from local
        # TODO: want to only do this if func is the name of an infix op's method
        method = 'enter_' + node.func
        if not hasattr(self, method) and node.func in self.local:
            args, kwargs = node.map_subcalls(self.enter)
            return self.create_local_call(node.func, args[0], Call, args[1:], kwargs)

        return super().enter(node)

    def enter___getattr__(self, node):
        obj, attr = node.args
        if obj.func == "__getattr__" and obj.args[1] in self.call_sub_attr:
            prev_obj, prev_attr = obj.args

            # TODO: should always call exit?
            if self.chain_sub_attr:
                # use chained attribute to look up local function instead
                # e.g. dt.round, rather than round
                attr = prev_attr + '.' + attr
            return self.create_local_call(attr, prev_obj, Call)
        elif attr in self.call_props:
            return self.create_local_call(attr, obj, Call)

        return self.generic_enter(node)

    def enter___custom_func__(self, node):
        func = node(None)
        
        # TODO: not robust at all, need class for singledispatch? unique attr flag?
        if (hasattr(func, 'registry')
            and hasattr(func, 'dispatch')
            and self.dispatch_cls is not None
            ):
            # allow custom functions that dispatch on dispatch_cls
            f_for_cls = func.dispatch(self.dispatch_cls)

            if isinstance(f_for_cls, FunctionLookupBound):
                # TODO: this is a bit funky, since FLB raises when called
                raise FunctionLookupError(f_for_cls.msg)

            if (self.result_cls is None
                or is_dispatch_func_subtype(f_for_cls, self.dispatch_cls, self.result_cls)
                ):
                # matches return annotation type (or not required)
                return node.__class__(f_for_cls)
            
            raise FunctionLookupError(
                    "External function {name} can dispatch on the class {dispatch_cls}, but "
                    "must also have result annotation of (sub)type {result_cls}"
                        .format(
                            name = func.__name__,
                            dispatch_cls = self.dispatch_cls,
                            result_cls = self.result_cls
                            )
                    )

        # doesn't raise an error so we can look in locals for now
        # TODO: remove behavior, once all SQL dispatch funcs moved from locals
        return self.generic_enter(node)

    def enter___call__(self, node):
        """
        Overview:
             variables      _.x.method(1)         row_number(_.x, 1)
             ---------      -------------        --------------------
            
                            █─'__call__'         █─'__call__'                           
             obj            ├─█─.                ├─<function row_number
                            │ ├─█─.              ├─█─.                                  
                            │ │ ├─_              │ ├─_                                  
                            │ │ └─'x'            │ └─'x'                                
                            │ └─'method'         │                                   
                            └─1                  └─1
        """
        obj, *rest = node.args
        args = tuple(self.enter_if_call(child) for child in rest)
        kwargs = {k: self.enter_if_call(child) for k, child in node.kwargs.items()}

        attr_chain, target = get_attr_chain(obj, max_n = 2)
        if attr_chain:
            # want _.x.method() -> method(_.x), need to transform
            if attr_chain[0] in self.call_sub_attr:
                # e.g. _.dt.round()
                call_name = ".".join(attr_chain) if self.chain_sub_attr else attr_chain[-1]
                entered_target = self.enter_if_call(target)
            else:
                call_name = attr_chain[-1]
                entered_target = self.enter_if_call(obj.args[0])

        elif isinstance(obj, FuncArg) and self.dispatch_cls is None:
            # want function(_.x) -> new_function(_.x), has form
            call_name = obj.obj_name()
            # the first argument is basically "self"
            entered_target, *args = args
        else:
            # default to generic enter
            return self.generic_enter(node)

        return self.create_local_call(
                call_name, entered_target, node.__class__,
                args, kwargs
                )


# Also need NodeTransformer


# Symbolic
# =============================================================================

class Symbolic(object):
    def __init__(self, source = None, ready_to_call = False):
        self.__source = MetaArg("_") if source is None else source
        self.__ready_to_call = ready_to_call


    # allowed methods ----

    def __getattr__(self, x):
        # temporary hack working around ipython pretty.py printing
        #if x == "__class__": return Symbolic

        return Symbolic(BinaryOp(
                "__getattr__",
                self.__source,
                strip_symbolic(x)
                ))
                

    def __call__(self, *args, **kwargs):
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


    # banned methods ----

    __contains__ = None
    __iter__ = None

    def __bool__(self):
        raise TypeError("Symbolic objects can not be converted to True/False, or used "
                        "with these keywords: not, and, or.")


    # representation ----
        
    def __repr__(self):
        return Formatter().format(self.__source)


def create_sym_call(source, *args, **kwargs):
    return Symbolic(Call(
            "__call__",
            strip_symbolic(source),
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


def str_to_getitem_call(x):
    return Call("__getitem__", MetaArg("_"), x)

    
def strip_symbolic(x):
    if isinstance(x, Symbolic):
        return x.__dict__["_Symbolic__source"]

    return x


def explain(symbol):
    """Print representation that resembles code used to create symbol."""
    if isinstance(symbol, Symbolic):
        return str(strip_symbolic(symbol))

    return str(symbol)


# symbolic dispatch wrapper ---------------------------------------------------

from functools import singledispatch, update_wrapper
import inspect

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


    
# Do some gnarly method setting -----------------------------------------------

def create_binary_op(op_name, left_op = True):
    def _binary_op(self, x):
        if left_op:
            node = BinaryOp(op_name, strip_symbolic(self), strip_symbolic(x))
        else:
            node = BinaryRightOp(op_name, strip_symbolic(self), strip_symbolic(x))

        return Symbolic(node, ready_to_call = True)
    return _binary_op

def create_unary_op(op_name):
    def _unary_op(self):
        node = UnaryOp(op_name, strip_symbolic(self))

        return Symbolic(node, ready_to_call = True)

    return _unary_op

for k, v in BINARY_OPS.items():
    if k in {"__getattr__", "__getitem__"}: continue
    rop = k.replace("__", "__r", 1)
    setattr(Symbolic, k, create_binary_op(k))
    setattr(Symbolic, rop, create_binary_op(rop, left_op = False))

for k, v in UNARY_OPS.items():
    if k != "__invert__":
        setattr(Symbolic, k, create_unary_op(k))

Lam = Lazy

_ = Symbolic()

