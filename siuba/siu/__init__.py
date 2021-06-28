from .calls import Call, MetaArg, Lazy, BinaryOp
from .symbolic import Symbolic, strip_symbolic, create_sym_call
from .visitors import CallTreeLocal, FunctionLookupBound, FunctionLookupError

Lam = Lazy

_ = Symbolic()

