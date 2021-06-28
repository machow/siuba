from .calls import Call, MetaArg, FuncArg, Lazy, BinaryOp, _SliceOpIndex, DictCall, str_to_getitem_call
from .symbolic import Symbolic, strip_symbolic, create_sym_call, explain
from .visitors import CallTreeLocal, FunctionLookupBound, FunctionLookupError
from .dispatchers import symbolic_dispatch

Lam = Lazy

_ = Symbolic()

