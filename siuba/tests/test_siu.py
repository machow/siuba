from siuba.siu import _, strip_symbolic

def test_op_vars_slice():
    assert strip_symbolic(_.a[_.b:_.c]).op_vars() == {'a', 'b', 'c'}
