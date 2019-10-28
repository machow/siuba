import itertools

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

        "__getattr__": 0
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
        "__getattr__": "."
        }

UNARY_OPS = {
        "__invert__": "~",
        "__neg__": "-",
        "__pos__": "+",
        "__abs__": "ABS => "
        }

# TODO: can it just be put in binary ops? Special handling in Symbolic class?
MISC_OPS = {
        "__getitem__": "["
        }

ALL_OPS = {**BINARY_OPS, **UNARY_OPS, **MISC_OPS}

for k, v in BINARY_OPS.copy().items():
    BINARY_OPS[k.replace("__", "__r", 1)] = v

for k, v in BINARY_LEVELS.copy().items():
    BINARY_LEVELS[k.replace("__", "__r", 1)] = v

class Formatter:
    def __init__(self): pass

    def format(self, call):
        fmt_block = "█─"
        fmt_pipe = "├─"
        
        # TODO: why are some nodes still symbolic?
        if isinstance(call, Symbolic):
            return self.format(call.source)

        if isinstance(call, MetaArg):
            return "_"

        if isinstance(call, Call):
            call_str = fmt_block + ALL_OPS.get(call.func, repr(call.func))

            args_str = (self.format(arg) for arg in call.args)

            # TODO: kwargs handling looks funny.. (e.g. _.a(b = _.c))
            kwargs_str = (k + " = " + self.format(v) for k,v in call.kwargs.items())

            all_args = [*args_str, *kwargs_str]
            if len(all_args):
                fmt_args = [*map(self.fmt_pipe, all_args[:-1]), self.fmt_pipe(all_args[-1], final = True)]
            else:
                fmt_args = []
            return "".join([call_str, *fmt_args])

        call_str = repr(call)
        indx = call_str.find("\n")

        if indx != -1:
            return call_str
        else:
            return call_str

    @staticmethod
    def fmt_pipe(x, final = False):
        if not final:
            connector = "\n│ " if not final else "\n  "
            prefix = "\n├─"
        else:
            connector = "\n  "
            prefix = "\n└─"
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
            op_repr, rest = self.args[0], self.args[1:]
            arg_str = map(repr, rest)
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
        inst, *rest = (self.evaluate_calls(arg, x) for arg in self.args)
        kwargs = {k: self.evaluate_calls(v, x) for k, v in self.kwargs.items()}
        
        # TODO: temporary workaround, for when only __get_attribute__ is defined
        if self.func == "__getattr__":
            return getattr(inst, *rest)

        # in normal case, get method to call, and then call it
        return getattr(inst, self.func)(*rest, **kwargs)

    @staticmethod
    def evaluate_calls(arg, x):
        if isinstance(arg, Call): return arg(x)

        return arg

    def copy(self):
        args, kwargs = self.map_subcalls(self.copy)
        return self.__class__(self.func, *args, **kwargs)

    def map_subcalls(self, f):
        new_args = tuple(f(arg) if isinstance(arg, Call) else arg for arg in self.args)
        new_kwargs = {k: f(v) if isinstance(v, Call) else v for k,v in self.kwargs.items()}

        return new_args, new_kwargs

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
        level = BINARY_LEVELS[self.func]
        spaces = "" if level == 0 else " "
        #if level < 4:
        #    spaces = " "*level
        #    fmt = "{args[0]}{spaces}{func}{spaces}{args[1]}"
        #else:
        #    spaces = " "
        #    fmt = "({args[0]}{spaces}{func}{spaces}{args[1]})"
        args = self.args
        arg0 = "({args[0]})" if self.needs_paren(args[0]) else "{args[0]}"
        arg1 = "({args[1]})" if self.needs_paren(args[1]) else "{args[1]}"
        fmt = arg0 + "{spaces}{func}{spaces}" + arg1


        func = BINARY_OPS[self.func]
        return fmt.format(func = func, args = self.args, kwargs = self.kwargs, spaces = spaces)

    def needs_paren(self, x):
        if isinstance(x, BinaryOp):
            sub_lvl = BINARY_LEVELS[x.func]
            level = BINARY_LEVELS[self.func]
            if sub_lvl != 0 and sub_lvl != level:
                return True

        return False


class DictCall(Call):
    """evaluates both keys and vals."""

    def __init__(self, f, *args, **kwargs):
        # TODO: validation, clean up class
        super().__init__(f, *args, **kwargs)
        self.args = (dict, dict(self.args[1]))

    def map_subcalls(self, f):
        d = self.args[1]

        new_d = {
                f(k) if isinstance(k, Call) else k: f(v) if isinstance(v, Call) else v
                        for k,v in d.items()
                        }

        return (self.args[0], new_d), {}

    def __call__(self, x):
        return self.args[1]


class MetaArg(Call):
    def __init__(self, func, *args, **kwargs):
        self.func = "_"
        self.args = args
        self.kwargs = kwargs

    def __repr__(self):
        return self.func

    def __call__(self, x):
        return x


# Trees and Visitors ==========================================================
from .error import ShortException

class FunctionLookupError(ShortException): pass


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


from inspect import isclass

class CallTreeLocal(CallListener):
    def __init__(
            self,
            local,
            call_sub_attr = None,
            chain_sub_attr = False,
            replace_calls = True
            ):
        """
        Arguments:
            local: a dictionary mapping func_name: func, used to replace call expressions.
            call_sub_attr: a set of attributes signaling any subattributes are property
                           methods. Eg. {'dt'} to signify in _.dt.year, year is a property call.
            chain_sub_attr: whether to included the attributes in the above argument, when looking up
                           up a replacement for the property call. E.g. does local have a 'dt.year' entry.
            replace_calls: whether all calls, including custom call objects should be replaced.
        """
        self.local = local
        self.call_sub_attr = set(call_sub_attr or [])
        self.chain_sub_attr = chain_sub_attr
        self.replace_calls = replace_calls

    def create_local_call(self, name, prev_obj, cls, func_args = None, func_kwargs = None):
        # need call attr name (arg[0].args[1]) 
        # need call arg and kwargs
        func_args = tuple() if func_args is None else func_args
        func_kwargs = {} if func_kwargs is None else func_kwargs

        try:
            local_func = self.local[name]
        except KeyError as err:
            raise FunctionLookupError("Missing translation for function call: %s"% name)

        if isclass(local_func) and issubclass(local_func, Exception):
            raise local_func

        return cls(
                "__call__",
                local_func,
                prev_obj,
                *func_args,
                **func_kwargs
                )

    def enter(self, node):
        # if no enter metthod for operators, like __invert__, try to get from local
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
            if attr_chain[0] in self.call_sub_attr and self.chain_sub_attr:
                # e.g. _.dt.round()
                call_name = ".".join(attr_chain)
                entered_target = self.enter_if_call(target)
            else:
                call_name = attr_chain[-1]
                entered_target = self.enter_if_call(obj.args[0])

        elif node.obj_name() is not None and self.replace_calls:
            # want function(_.x) -> new_function(_.x), has form
            call_name = node.obj_name()
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
        self.source = MetaArg("_") if source is None else source
        self.ready_to_call = ready_to_call

    def __getattr__(self, x):
        # temporary hack working around ipython pretty.py printing
        #if x == "__class__": return Symbolic

        return Symbolic(BinaryOp(
                "__getattr__",
                self.source,
                strip_symbolic(x)
                ))
                

    def __call__(self, *args, **kwargs):
        if self.ready_to_call:
            return self.source(*args, **kwargs)

        return create_sym_call(self.source, *args, **kwargs)

    def __getitem__(self, *args):
        return Symbolic(Call(
                "__getitem__",
                self.source,
                *map(slice_to_call, args)
                ),
                ready_to_call = True)

    
    def __invert__(self):
        if isinstance(self.source, Call) and self.source.func == "__invert__":
            return self.source.args[0]
        else: 
            return self.__op_invert()


    def __op_invert(self):
        return Symbolic(UnaryOp('__invert__', self.source), ready_to_call = True)

        
    def __repr__(self):
        return Formatter().format(self.source)


def create_sym_call(source, *args, **kwargs):
    return Symbolic(Call(
            "__call__",
            strip_symbolic(source),
            *map(strip_symbolic, args),
            **{k: strip_symbolic(v) for k,v in kwargs.items()}
            ),
            ready_to_call = True)


def slice_to_call(x):
    if isinstance(x, slice):
        args = map(strip_symbolic, (x.start, x.stop, x.step))
        return Call("__call__", slice, *args)
    
    return strip_symbolic(x)


def str_to_getitem_call(x):
    return Call("__getitem__", MetaArg("_"), x)

    
def strip_symbolic(x):
    if isinstance(x, Symbolic):
        return x.source

    return x


def explain(symbol):
    """Print representation that resembles code used to create symbol."""
    if isinstance(symbol, Symbolic):
        print(symbol.source)
    else: 
        print(symbol)


# symbolic dispatch wrapper ---------------------------------------------------

from functools import singledispatch

def symbolic_dispatch(f):
    # TODO: don't use singledispatch if it has already been done
    f = singledispatch(f)
    @f.register(Symbolic)
    def _dispatch_symbol(__data, *args, **kwargs):
        return create_sym_call(f, __data.source, *args, **kwargs)

    @f.register(Call)
    def _dispatch_call(__data, *args, **kwargs):
        # TODO: want to just create call, for now use hack of creating a symbolic
        #       call and getting the source off of it...
        return create_sym_call(f, __data, *args, **kwargs).source

    return f



# Do some gnarly method setting -----------------------------------------------

def create_binary_op(op_name):
    def _binary_op(self, x):
        node = BinaryOp(op_name, self.source, strip_symbolic(x))

        return Symbolic(node, ready_to_call = True)

    return _binary_op

def create_unary_op(op_name):
    def _unary_op(self):
        node = UnaryOp(op_name, self.source)

        return Symbolic(node, ready_to_call = True)

    return _unary_op

for k, v in BINARY_OPS.items():
    if k in {"__getattr__"}: continue
    rop = k.replace("__", "__r")
    setattr(Symbolic, k, create_binary_op(k))

for k, v in UNARY_OPS.items():
    if k != "__invert__":
        setattr(Symbolic, k, create_unary_op(k))

Lam = Lazy

_ = Symbolic()

