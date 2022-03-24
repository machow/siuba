from .calls import (
    Call,
    MetaArg,
    FuncArg,
    Lazy,
    BinaryOp,
    _SliceOpIndex,
    DictCall,
    str_to_getitem_call
)
from .symbolic import Symbolic, strip_symbolic, create_sym_call, explain
from .visitors import CallTreeLocal, CallVisitor, FunctionLookupBound, FunctionLookupError, ExecutionValidatorVisitor
from .dispatchers import symbolic_dispatch, singledispatch2, pipe_no_args, Pipeable

Lam = Lazy

_ = Symbolic()

pipe = Pipeable

