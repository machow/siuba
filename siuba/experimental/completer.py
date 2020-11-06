from IPython import get_ipython
from IPython.core.history import HistoryAccessor
from IPython.core.completer import cursor_to_position, _FakeJediCompletion
from typing import Iterable, Any
from siuba.siu import Symbolic

import jedi
import functools


def _jedi_matches(self, cursor_column:int, cursor_line:int, text:str) -> Iterable[Any]:
    """

    Return a list of :any:`jedi.api.Completions` object from a ``text`` and
    cursor position.

    Parameters
    ----------
    cursor_column : int
        column position of the cursor in ``text``, 0-indexed.
    cursor_line : int
        line position of the cursor in ``text``, 0-indexed
    text : str
        text to complete

    Debugging
    ---------

    If ``IPCompleter.debug`` is ``True`` may return a :any:`_FakeJediCompletion`
    object containing a string with the Jedi debug information attached.
    """
    df = _find_df_in_history(self.shell)
    if df and isinstance(self.shell.user_ns.get("_"), Symbolic):
        namespaces = [{**self.namespace, "_": self.namespace[df]}]
    else:
        namespaces = [self.namespace]

    if self.global_namespace is not None:
        namespaces.append(self.global_namespace)

    completion_filter = lambda x:x
    offset = cursor_to_position(text, cursor_line, cursor_column)
    # filter output if we are completing for object members
    if offset:
        pre = text[offset-1]
        if pre == '.':
            if self.omit__names == 2:
                completion_filter = lambda c:not c.name.startswith('_')
            elif self.omit__names == 1:
                completion_filter = lambda c:not (c.name.startswith('__') and c.name.endswith('__'))
            elif self.omit__names == 0:
                completion_filter = lambda x:x
            else:
                raise ValueError("Don't understand self.omit__names == {}".format(self.omit__names))

    interpreter = jedi.Interpreter(text[:offset], namespaces)
    try_jedi = True

    try:
        # find the first token in the current tree -- if it is a ' or " then we are in a string
        completing_string = False
        try:
            first_child = next(c for c in interpreter._get_module().tree_node.children if hasattr(c, 'value'))
        except StopIteration:
            pass
        else:
            # note the value may be ', ", or it may also be ''' or """, or
            # in some cases, """what/you/typed..., but all of these are
            # strings.
            completing_string = len(first_child.value) > 0 and first_child.value[0] in {"'", '"'}

        # if we are in a string jedi is likely not the right candidate for
        # now. Skip it.
        try_jedi = not completing_string
    except Exception as e:
        # many of things can go wrong, we are using private API just don't crash.
        if self.debug:
            print("Error detecting if completing a non-finished string :", e, '|')

    if not try_jedi:
        return []
    try:
        return filter(completion_filter, interpreter.complete(column=cursor_column, line=cursor_line + 1))
    except Exception as e:
        if self.debug:
            return [_FakeJediCompletion('Oops Jedi has crashed, please report a bug with the following:\n"""\n%s\ns"""' % (e))]
        else:
            return []


def _match_df_name(dfs, commands):
    if not dfs:
        return []

    for command in reversed(commands):
        exact_match = [df for df in dfs if df == command]
        if exact_match:
            return exact_match[0]

        method_match = [df for df in dfs if df + "." in command]
        if method_match:
            return method_match[0]

        assign_match = [df for df in dfs if command.startswith(df + " = ")]
        if assign_match:
            return assign_match[0]

        import_match = [df for df in dfs if "import " + df in command]
        if import_match:
            return import_match[0]

        in_expression = [df for df in dfs if df + "," in command or df + ")" in command]
        if in_expression:
            return in_expression[0]

        any_match = [df for df in dfs if df in command]
        if any_match:
            return any_match[0]

    # return one of the dataframes
    return dfs[0]


def _find_df_in_history(shell):
    dfs = shell.run_line_magic("who_ls", "DataFrame")

    history = HistoryAccessor()
    commands = [command for _, _, command in history.get_tail(include_latest=True)]

    return _match_df_name(dfs, commands)


def siuba_jedi_override(shell):
    shell.Completer._jedi_matches = functools.partial(_jedi_matches, shell.Completer)


shell = get_ipython()

if shell is not None:
    siuba_jedi_override(shell)
