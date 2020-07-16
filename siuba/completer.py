from IPython import get_ipython


class ShellCompletion(object):
    def __init__(self, shell, target_df):
        self.old_ns = shell.user_ns

        old_ns = shell.Completer.namespace
        target_df = shell.user_ns["mtcars"]
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


def symbolic_completer(shell, event):

    with ShellCompletion(shell, "mtcars") as shell:
        _, _, _, jedi = shell.Completer.complete(event.symbol)

    return [
        event.symbol + completions.name_with_symbols for completions in jedi
    ]


shell = get_ipython()
shell.set_hook('complete_command', symbolic_completer, re_key = '.*_.*')
