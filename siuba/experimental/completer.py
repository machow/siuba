from IPython import get_ipython
from IPython.core.history import HistoryAccessor


class _ShellCompletion(object):
    def __init__(self, shell, target_df):
        self.old_ns = shell.user_ns

        old_ns = shell.Completer.namespace
        target_df = shell.user_ns[target_df]
        shell.Completer.namespace = {**old_ns, "_": target_df}

        self.old_custom_completers = shell.Completer.custom_completers
        shell.Completer.custom_completers = None

        self.shell = shell

    def __enter__(self):
        return self.shell

    def __exit__(self):
        shell = self.shell

        shell.Completer.namespace = self.old_ns
        shell.Completer.custom_completers = self.old_custom_completers


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


def symbolic_completer(shell, event):

    df = _find_df_in_history(shell)

    with _ShellCompletion(shell, df) as shell:
        _, _, _, jedi = shell.Completer.complete(event.symbol)

    return [event.symbol + completions.name_with_symbols for completions in jedi]


shell = get_ipython()
shell.set_hook("complete_command", symbolic_completer, re_key=".*_.*")
