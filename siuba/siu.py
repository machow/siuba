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

RIGHT_ASSOC_OPS = {
        "__pow__": "**",
        "__lpow__": "**"
        }

ALL_OPS = {**BINARY_OPS, **UNARY_OPS, **MISC_OPS}

for k, v in {**BINARY_OPS}.items():
    BINARY_OPS[k.replace("__", "__r", 1)] = v


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
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __repr__(self):
        # TODO: format binary, unary, call, associative
        op_repr = BINARY_OPS.get(self.func, None)
        if op_repr:
            fmt = "({args[0]} {func} {args[1]})"
        elif self.func == "getattr":
            op_repr = "."
            fmt = "({args[0]}{func}{args[1]})"
        else:
            op_repr, rest = self.args[0], self.args[1:]
            arg_str = ", ".join(map(str, rest))
            kwarg_str = ", ".join(str(k) + " = " + str(v) for k,v in self.kwargs.items())
            fmt = "{}({}, {})".format(op_repr, arg_str, kwarg_str)
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

    def map_subcalls(self, f):
        new_args = tuple(f(arg) if isinstance(arg, Call) else arg for arg in self.args)
        new_kwargs = {k: f(v) if isinstance(v, Call) else v for k,v in self.kwargs.items()}

        return new_args, new_kwargs

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
                varnames.update(arg.op_vars())

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
        if self.func in RIGHT_ASSOC_OPS:
            fmt = "{func}{args[0]}"
        else:
            fmt = "{args[0]}{func}"

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


class CallTreeLocal(CallListener):
    def __init__(self, local, rm_attr = None, call_sub_attr = None):
        self.local = local
        self.rm_attr = set(rm_attr or [])
        self.call_sub_attr = set(call_sub_attr or [])

    def create_local_call(self, name, prev_obj, cls, func_args = None, func_kwargs = None):
        # need call attr name (arg[0].args[1]) 
        # need call arg and kwargs
        func_args = tuple() if func_args is None else func_args
        func_kwargs = {} if func_kwargs is None else func_kwargs

        try:
            local_func = self.local[name]
        except KeyError:
            raise Exception("No local entry %s"% name)


        return cls(
                "__call__",
                local_func,
                prev_obj,
                *func_args,
                **func_kwargs
                )

    def enter___getattr__(self, node):
        obj, attr = node.args
        if obj.func == "__getattr__" and obj.args[1] in self.call_sub_attr:
            args, kwargs = node.map_subcalls(self.enter)
            visited_obj = args[0]

            # TODO: should always call exit?
            return self.create_local_call(attr, visited_obj, node.__class__)

        return self.generic_enter(node)

    def exit___getattr__(self, node):
        # remove, e.g. the str attribute, so we can dispatch pandas str method calls
        if node.args[1] in self.rm_attr:
            obj = node.args[0]
            return obj

        obj, crnt_attr = node.args
        # e.g. _.somecol.str.endswith
        if obj.func == "__getattr__" and obj.args[1] in self.rm_attr:
            prev_obj = obj.args[0]
            return node.__class__(node.func, prev_obj, crnt_attr)

        return self.generic_exit(node)

    def exit___call__(self, node):
        # make sure to transform subcalls, no matter what happens
        obj = node.args[0]
        call_name = node.obj_name()
        if isinstance(obj, Call) and obj.func == "__getattr__":
            # since obj is getting the call we're interested in, we pull the call
            # from local and replace obj with whatever was earlier on the chain
            # e.g. _.a.b() has the form <prev_obj>.<obj>(), want <obj>(prev_obj>)
            prev_obj, attr = obj.args
            return self.create_local_call(
                    attr, prev_obj, node.__class__,
                    node.args[1:], node.kwargs
                    )
        elif call_name is not None:
            # node is calling, e.g., a literal function
            # use the function's name to replace with local call
            return self.create_local_call(
                    call_name, node.args[1], node.__class__,
                    node.args[2:], node.kwargs
                    )
        
        # otherwise, fall back to copying node
        return node.__class__(node.func, *node.args, **node.kwargs)




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

