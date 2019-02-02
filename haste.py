import ast
from ast import Attribute, Name, Load, Call

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
        "__ge__": ">="
        }

UNARY_OPS = {
        "__invert__": "~",
        "__neg__": "-",
        "__pos__": "+",
        "__abs__": "ABS => "
        }

RIGHT_ASSOC_OPS = {
        "__pow__"
        "__lpow__"
        }

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

        if isinstance(call, Call):
            call_str = fmt_block + repr(call.func)

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
        connector = "\n├ " if not final else "\n  "
        return "\n├─" + connector.join(x.splitlines())

        

class Call:
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __repr__(self):
        op_repr = BINARY_OPS.get(self.func, None)
        if op_repr:
            fmt = "({args[0]} {func} {args[1]})"
        elif self.func == "getattr":
            op_repr = "."
            fmt = "({args[0]}{func}{args[1]})"
        else:
            fmt = "{func}(\n\t{args},\n\t{kwargs}\n)"
        return fmt.format(
                    func = op_repr or self.func,
                    args = self.args,
                    kwargs = self.kwargs
                    )

    def __call__(self, x):
        inst, *rest = (arg(x) if isinstance(arg, Call) else arg for arg in self.args)
        kwargs = {k: v(x) if isinstance(v, Call) else v for k, v in self.kwargs.items()}
        
        # TODO: temporary workaround, for when only __get_attribute__ is defined
        if self.func == "__getattr__":
            return getattr(inst, *rest)

        # in normal case, get method to call, and then call it
        return getattr(inst, self.func)(*rest, **kwargs)


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
        fmt = "({args[0]} {func} {args[1]})"

        func = BINARY_OPS[self.func]
        return fmt.format(func = func, args = self.args, kwargs = self.kwargs)



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

        return Symbolic(Call(
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
    rop = k.replace("__", "__r")
    setattr(Symbolic, k, create_binary_op(k))

for k, v in UNARY_OPS.items():
    if k != "__invert__":
        setattr(Symbolic, k, create_unary_op(k))

_ = Symbolic()


#Attribute(
#        value = Name(id = '_', ctx = Load()),
#        attr = "a",
#        ctx = Load()
#        )
ast.parse("X.a")
_.a

_(_.a + _.b, 2, 3)

