from siuba.siu import _, strip_symbolic, FunctionLookupError, Symbolic, MetaArg
import pytest

def test_op_vars_slice():
    assert strip_symbolic(_.a[_.b:_.c]).op_vars() == {'a', 'b', 'c'}

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



def test_symbolic_dispatch():
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


def test_call_tree_local_sub_attr_method(ctl):
    # sub attr gets stripped w/ method call
    call = ctl.enter(strip_symbolic(_.str.f_a()))
    assert call('x') == 'x'

def test_call_tree_local_sub_attr_property(ctl):
    # sub attr gets stripped w/ property access
    call = ctl.enter(strip_symbolic(_.str.f_a))
    assert call('x') == 'x'

def test_call_tree_local_sub_attr_alone(ctl):
    # attr alone is treated like a normal getattr
    call = ctl.enter(strip_symbolic(_.str))
    assert call.func == "__getattr__"
    assert call.args[1] == "str"

def test_call_tree_local_sub_attr_method_missing(ctl):
    # subattr raises lookup errors (method)
    with pytest.raises(FunctionLookupError):
        ctl.enter(strip_symbolic(_.str.f_b()))

def test_call_tree_local_sub_attr_property_missing(ctl):
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
 
