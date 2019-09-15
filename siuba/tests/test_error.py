from siuba import error
import sys
import pytest


@pytest.fixture(scope = "function")
def last_exc():
    # Note: can't really insert on sys.last_value here, and clean up after...
    err = error.ShortException("testing", short = True)

    yield err

def test_siuba_error_last(last_exc):
    # test part
    assert last_exc.__suppress_context__ is True
    sys.last_value = last_exc
    
    err2 = error.last()

    assert last_exc is err2
    err2.__suppress_context__ is False


def test_siuba_error_shortexception_from_verb():

    err = error.ShortException.from_verb("Mutate", "some_arg_name", "some message", short = True)

    assert "Mutate" in str(err)
    assert "some_arg_name" in str(err)
    assert "some message" in str(err)

    assert err.__suppress_context__ is True

    assert error.ERROR_INSTRUCTIONS in str(err)

def test_siuba_error_instruction(last_exc):
    sys.last_value = last_exc
    full_error_code = str(last_exc).split(":")[-1].lstrip()

    with pytest.raises(error.ShortException) as err:
        exec(full_error_code)
