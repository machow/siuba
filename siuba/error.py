from contextlib import contextmanager, AbstractContextManager
import sys
import copy

ERROR_INSTRUCTIONS = "To see the full error, run: import siuba.error; raise siuba.error.last()"

def last():
    """Return last error with suppress context set to False.
    
    Note: makes a copy of last error, since interactive shells like ipython
          do later handling of exceptions, so a context manager is not an option.

    """

    err = sys.last_value

    if err is None:
        return None

    err.__suppress_context__ = False
    return err


class ShortException(Exception):
    """Exception that provides the option (short) to remove context from stack traces.
    
    This greatly shortens the stack. Instructions are provided for viewing the whole thing.

    """
    def __init__(self, msg, short = False):
        if short: 
            full_msg = str(msg) + "\n\n" + ERROR_INSTRUCTIONS
        else:
            full_msg = msg

        self.args = (full_msg,)
        self.__suppress_context__ = short


    @classmethod
    def from_verb(cls, verb_name, arg_name, err, *args, **kwargs):
        error_msg = "\nVerb: {verb}\nArg: {arg}\nError: {err}.".format(
                verb = verb_name, arg = arg_name, err = err
                )
        
        return cls(error_msg, *args, **kwargs)

