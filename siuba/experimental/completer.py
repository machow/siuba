from IPython import get_ipython
from IPython.core.history import HistoryAccessor
from IPython.core.completer import cursor_to_position, _FakeJediCompletion
from typing import Iterable, Any
from functools import wraps, partial
from contextlib import contextmanager

from siuba.siu import Symbolic


def order_results_cols_first(completions: "iter[Any]", df) -> "list[Any]":
    # TODO: annotate this as a list of jedi completions objs
    cols = set(df.columns)
    return sorted(completions, key = lambda x: 0 if x.name in cols else 1)


def wrap_jedi_matches(_jedi_matches):
    @wraps(_jedi_matches)
    def wrapper(self, *args, **kwargs):
        orig_namespace = self.namespace

        # find DataFrame and inject into namespace
        df_name = _find_df_in_history(self.shell)

        # guard to fallback to default behavior ----
        if df_name is None or not isinstance(self.shell.user_ns.get("_"), Symbolic):
            return _jedi_matches(*args, **kwargs)

        # insert dataframe and autocomplete ----
        df = self.namespace[df_name]
        self.namespace = {**self.namespace, "_": df}

        try:
            completions = _jedi_matches(*args, **kwargs)
        finally:
            self.namespace = orig_namespace

        self._all_completions = list(completions)

        return order_results_cols_first(self._all_completions, df)

        #return completions

    return wrapper


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
    # wrap the bound method _jedi_matches. Note that Completer is actually an instance
    self = shell.Completer
    wrapped = wrap_jedi_matches(self._jedi_matches)
    shell.Completer._jedi_matches = partial(wrapped, self)


shell = get_ipython()

if shell is not None:
    siuba_jedi_override(shell)
