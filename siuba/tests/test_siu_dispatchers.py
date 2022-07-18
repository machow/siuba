import pytest

from siuba.siu.dispatchers import call
from siuba.siu import _

# calls a function on data
# calls a function with an arg
# calls a function with a kwarg
# calls a function with data in another spot
# siu expr first arg not extra args allowed
# siu expr first arg simple
# "abc", 
def test_siu_call_no_args():
    assert 1 >> call(range) == range(1)

def test_siu_call_no_args_explicit():
    assert 1 >> call(range, _) == range(1)

def test_siu_call_pos_arg():
    assert 1 >> call(range, _, 2) == range(1, 2)


def test_siu_call_kwarg():
    assert "," >> call("a,b,c".split, _, maxsplit=1) == ["a", "b,c"]


def test_siu_call_arg_kwarg():
    assert 1 >> call("{0}_{1}_{b}".format, _, 2, b=3) == "1_2_3"


def test_siu_call_underscore_arg():
    assert 1 >> call(range, 2, 3, _) == range(2, 3, 1)


def test_siu_call_underscore_method():
    assert "a,b" >> call(_.split(",")) == ["a", "b"]


def test_siu_call_understcore_method_args():
    with pytest.raises(NotImplementedError):
        "a,b" >> call(_.split, _, ",")