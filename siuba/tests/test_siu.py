from siuba.siu import strip_symbolic, FunctionLookupError, Symbolic, MetaArg, Call
import pytest

BINARY_OPS = [
    "+",
    "-",
    "*",
    "@",
    "//",
    #truediv
    #divmod
    "/",
    "%",
    #"_ ** _", #TODO: uses different formatting
    "<<",
    ">>",
    "&",
    "^",
    "|"
    ]

@pytest.fixture
def _():
    return Symbolic()

def test_source_attr(_):
    sym = _.source
    assert isinstance(sym, Symbolic)
    assert explain(sym) == "_.source"

def test_op_vars_slice(_):
    assert strip_symbolic(_.a[_.b:_.c]).op_vars() == {'a', 'b', 'c'}

# Truthiness should raise TypeError -------------------------------------------

import operator as op

@pytest.mark.parametrize('f, arg', [
    (op.contains, 'x'),
    (op.not_,None),
    (op.truth,None),
    (iter,None),
    ])
def test_siu_not_truth_value(_, f, arg):
    with pytest.raises(TypeError):
        f(_) if arg  is None else f(_, arg)
        f(_, arg)
    
def test_siu_not_truth_value_keywords(_):
    with pytest.raises(TypeError):
        _ and True

    with pytest.raises(TypeError):
        _ or True


# Explain  --------------------------------------------------------------------

from siuba.siu import explain

@pytest.mark.parametrize('code', [
    "-_",
    "+_",
    #abs,
    "~_",
    #complex,
    #int,
    #long,
    #float,
    #oct,
    #hex,
    #index,
    #round,
    #math.trunc
    #math.floor
    #math.ceil

    ])
def test_explain_unary(_, code):
    sym = eval(code, {'_': _})

    assert explain(sym) == code

@pytest.mark.parametrize('op', BINARY_OPS)
def test_explain_binary(_, op):
    left = "_ {op} 1".format(op = op)
    sym = eval(left, {'_': _})

    assert explain(sym) == left

    right = "1 {op} _".format(op = op)
    sym = eval(right, {'_': _})

    assert explain(sym) == right

@pytest.mark.parametrize('op', BINARY_OPS)
def test_explain_rhs_binary(_, op):
    right = "1 {op} _".format(op = op)

    sym = eval(right, {"_": _})

    assert explain(sym) == right

@pytest.mark.parametrize('op', BINARY_OPS)
def test_symbol_rhs_exec(_, op):
    if op == "@":
        pytest.skip("TODO: test with class where @ is implemented?")

    right = "1 {op} _".format(op = op)

    sym = eval(right, {'_': _})
    dst = eval(right, {'_': 2})

    assert sym(2) == dst

@pytest.mark.parametrize('expr', [
    "_['a']",
    "_.a",
    "_['a':'b']",
    "_['a':'b', 'c']",
    """_["'a'":'b']""",
    "_.a.mean(1,b = 2)",
    ])
def test_explain_other(_, expr):
    sym = eval(expr, {'_': _})
    assert explain(sym) == expr

@pytest.mark.parametrize('expr', [
    '_[slice(1,2)]',           # siu can't know concrete slice syntax
    '_["a"]',                  # siu can't know concrete quote syntax
    ])
def test_explain_failures(_, expr):
    sym = eval(expr, {'_': _})
    assert explain(sym) != expr


# Special Call subclasses =====================================================

# SliceOp ----

def test_sym_slice():
    from siuba.siu import _SliceOpIndex

    _ = Symbolic()

    sym = _[_ < 1]
    meta, slice_ops = strip_symbolic(sym).args
    assert isinstance(meta, MetaArg)
    assert isinstance(slice_ops, Call)
    assert isinstance(slice_ops, SliceOp)       # abc metaclass
    assert slice_ops.__class__ is _SliceOpIndex


    indexer = slice_ops(1)
    assert indexer is False

def test_sym_slice_multiple():
    from siuba.siu import _SliceOpExt

    _ = Symbolic()

    sym = _[_ < 1, :, :]

    meta, slice_ops = strip_symbolic(sym).args
    assert isinstance(meta, MetaArg)
    assert len(slice_ops.args) == 3
    assert isinstance(slice_ops.args[0], Call)
    assert isinstance(slice_ops, SliceOp)       # abc metaclass
    assert slice_ops.__class__ is _SliceOpExt

    indexer = slice_ops(1)
    assert indexer[0] is False
    assert indexer[1] == slice(None)
    
@pytest.mark.parametrize("expr, target", [
    ("_[1]", 1),
    ("_[1:]", slice(1, None)),
    ("_[:2]", slice(None, 2)),
    ("_[1:2]", slice(1, 2)),
    ("_[1:2:3]", slice(1, 2, 3)),
    ("_[slice(1,2,3)]", slice(1, 2, 3)),
    ("_[1, 'a']", (1, 'a')),
    ("_[(1, 'a')]", (1, 'a')),
    ("_[1, ]", (1,)),
    ("_[:, :, :]", (slice(None, None, None),)*3),
    ("_[_['a'], _['b']]", (1, 2)),
    ("_[_['a']:_['b']]", slice(1, 2)),
    ("_[_['a']:_['b'], :]", (slice(1, 2), slice(None))),
    ])
def test_slice_call_returns(_, expr, target):
    data = {'a': 1, 'b': 2}

    sym = eval(expr, {"_": _})
    index_op, slice_call = strip_symbolic(sym).args

    res = slice_call(data)
    assert res == target

# Node copying ================================================================

from siuba.siu import Call, BinaryOp, SliceOp, MetaArg, FuncArg, DictCall
# Call
@pytest.mark.parametrize('node', [
    Call("__call__", lambda x, y = 2: x + y, 1, y = 2),
    BinaryOp("__add__", 1, 2),
    SliceOp("__siu_slice__", slice(0, 1)),
    SliceOp("__siu_slice__", (slice(0, 1), slice(2, 3))),
    MetaArg("_"),
    FuncArg("__custom_func__", lambda x: x),
    FuncArg(lambda x: x),
    DictCall("__call__", dict, {'a': 1, 'b': 2})
    ])
def test_node_copy(node):
    copy = node.copy()
    assert isinstance(copy, node.__class__)
    assert copy is not node
    assert copy.func == node.func
    assert copy.args == node.args
    assert copy.kwargs == node.kwargs

def test_dict_call_child_copy():
    bin_op = BinaryOp('__add__', 1, 2)
    call = DictCall("__call__", dict, {'a': bin_op, 'b': 2})
    copy = call.copy()

    assert isinstance(call.args[1]['a'], BinaryOp)
    assert bin_op is not copy.args[1]['a']


# Symbolic dispatch ===========================================================

from siuba.siu import symbolic_dispatch, Call, FuncArg

def test_FuncArg():
    f = lambda x: 1
    expr = FuncArg(f)

    assert expr(None) is f

def test_FuncArg_in_call():
    call = Call(
            '__call__',
            FuncArg(lambda x, y: x + y),
            1, y = 2
            )

    assert call(None) == 3



def test_symbolic_dispatch(_):
    @symbolic_dispatch
    def f(x, y = 2):
        return x + y

    # w/ simple Call
    call1 = f(strip_symbolic(_), 3)
    assert isinstance(call1, Call)
    assert call1(2) == 5

    # w/ simple Symbol
    sym2 = f(_, 3)
    assert isinstance(sym2, Symbolic)
    assert sym2(2) == 5

    # w/ complex Call
    sym3 = f(_['a'], 3)
    assert sym3({'a': 2}) == 5



# Call Tree Local =============================================================
from siuba.siu import CallTreeLocal

@pytest.fixture
def ctl():
    local = {'f_a': lambda self: self}
    yield CallTreeLocal(local, call_sub_attr = ('str', 'dt'))


def test_call_tree_local_sub_attr_method(_, ctl):
    # sub attr gets stripped w/ method call
    call = ctl.enter(strip_symbolic(_.str.f_a()))
    assert call('x') == 'x'

def test_call_tree_local_sub_attr_property(_, ctl):
    # sub attr gets stripped w/ property access
    call = ctl.enter(strip_symbolic(_.str.f_a))
    assert call('x') == 'x'

def test_call_tree_local_sub_attr_alone(_, ctl):
    # attr alone is treated like a normal getattr
    call = ctl.enter(strip_symbolic(_.str))
    assert call.func == "__getattr__"
    assert call.args[1] == "str"

def test_call_tree_local_sub_attr_method_missing(_, ctl):
    # subattr raises lookup errors (method)
    with pytest.raises(FunctionLookupError):
        ctl.enter(strip_symbolic(_.str.f_b()))

def test_call_tree_local_sub_attr_property_missing(_, ctl):
    # subattr raises lookup errors (property)
    with pytest.raises(FunctionLookupError):
        ctl.enter(strip_symbolic(_.str.f_b))

# symbolic dispatch and call tree local ----
class SomeClass: pass

@pytest.fixture
def f_dispatch():
    @symbolic_dispatch
    def f(x):
        return 'default'

    @f.register(SomeClass)
    def _f_some_class(x):
        return 'some class'

    yield f


def test_call_tree_local_dispatch_cls_object(f_dispatch):
    ctl = CallTreeLocal(
            {'f_a': lambda self: self},
            dispatch_cls = object
            )

    call = Call("__call__", FuncArg(f_dispatch), MetaArg('_'))
    new_call = ctl.enter(call)
    assert new_call('na') == 'default'


def test_call_tree_local_dispatch_cls_subclass(f_dispatch):
    ctl = CallTreeLocal(
            {'f_a': lambda self: self},
            dispatch_cls = SomeClass
            )

    call = Call("__call__", FuncArg(f_dispatch), MetaArg('_'))
    new_call = ctl.enter(call)
    assert new_call('na') == 'some class'


# strict symbolic dispatch and call tree local ----

@pytest.fixture
def f_dispatch_strict():
    @symbolic_dispatch(cls = SomeClass)
    def f(x):
        return 'some class'

    yield f

def test_strict_dispatch_strict_default_fail(f_dispatch_strict):
    class Other(object): pass

    obj = Other()

    with pytest.raises(TypeError):
        f_dispatch_strict(obj)

def test_call_tree_local_dispatch_fail(f_dispatch_strict):
    ctl = CallTreeLocal(
            {'f_a': lambda self: self},
            dispatch_cls = object
            )

    call = Call("__call__", FuncArg(f_dispatch_strict), MetaArg('_'))

    # should be the default failure dispatch for object
    new_call = ctl.enter(call)
    with pytest.raises(TypeError):
        new_call('na')
