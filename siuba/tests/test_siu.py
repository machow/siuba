from siuba.siu import _, strip_symbolic, CallTreeLocal, FunctionLookupError
import pytest

def test_op_vars_slice():
    assert strip_symbolic(_.a[_.b:_.c]).op_vars() == {'a', 'b', 'c'}

# Call Tree Local =============================================================

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

