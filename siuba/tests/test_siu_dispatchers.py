import pytest

from siuba.siu.dispatchers import pipe
from siuba.siu import _

# calls a function on data
# calls a function with an arg
# calls a function with a kwarg
# calls a function with data in another spot
# siu expr first arg not extra args allowed
# siu expr first arg simple
# "abc", 
def test_siu_pipe_no_args():
    assert 1 >> pipe(range) == range(1)

def test_siu_pipe_no_args_explicit():
    assert 1 >> pipe(range, _) == range(1)

def test_siu_pipe_pos_arg():
    assert 1 >> pipe(range, _, 2) == range(1, 2)


def test_siu_pipe_kwarg():
    assert "," >> pipe("a,b,c".split, _, maxsplit=1) == ["a", "b,c"]


def test_siu_pipe_arg_kwarg():
    assert 1 >> pipe("{0}_{1}_{b}".format, _, 2, b=3) == "1_2_3"


def test_siu_pipe_underscore_arg():
    assert 1 >> pipe(range, 2, 3, _) == range(2, 3, 1)


def test_siu_pipe_underscore_method():
    assert "a,b" >> pipe(_.split(",")) == ["a", "b"]


def test_siu_pipe_understcore_method_args():
    with pytest.raises(NotImplementedError):
        "a,b" >> pipe(_.split, _, ",")
