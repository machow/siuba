import itertools

# [
# .. 'a'
# .. 'b':'c'

# +
# .. .
# .. .. _
# .. .. 'a'
# .. .
# .. .. _
# .. .. 'b'

# TODO: call formatting
# Displaying precedence (number of spaces)
# 1. *, @, /, //, %
# 2. +, -
# 3. <<, >>
# parentheses. &, ^, |
# parentheses. <, <= etc..

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

    def op_vars(self):
        varnames = set()

        op_var = self._get_op_var()
        if op_var is not None:
            varnames.add(op_var)

        all_args = itertools.chain(self.args, self.kwargs.values())
        for arg in all_args:
            if isinstance(arg, Call):
                varnames.update(arg.op_vars())

        return varnames
    
    def _get_op_var(self):
        if self.func in ("__getattr__", "__getitem__") and isinstance(self.args[1], str):
            return self.args[1]


class Lazy(Call):
    def __init__(self, func):
        self.func = "<lazy>"
        self.args = [func]
        self.kwargs = {}

    def __call__(self, x):
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



class MetaArg(Call):
    def __init__(self, func, *args, **kwargs):
        self.func = "_"
        self.args = args
        self.kwargs = kwargs

    def __repr__(self):
        return self.func

    def __call__(self, x):
        return x



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

        return Symbolic(Call(
                "__call__",
                self.source,
                *map(strip_symbolic, args),
                **{k: strip_symbolic(v) for k,v in kwargs.items()}
                ),
                ready_to_call = True)


    def __getitem__(self, *args):
        return Symbolic(Call(
                "__getitem__",
                self.source,
                *map(strip_symbolic, args)
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



def strip_symbolic(symbol):
    if isinstance(symbol, Symbolic):
        return symbol.source

    return symbol


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


#Attribute(
#        value = Name(id = '_', ctx = Load()),
#        attr = "a",
#        ctx = Load()
#        )
_.a

_(_.a + _.b, 2, 3)

