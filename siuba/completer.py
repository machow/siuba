from IPython import get_ipython


def symbolic_completer(shell, event):
    old_ns = shell.Completer.namespace
    target_df = shell.user_ns["mtcars"]
    shell.Completer.namespace = {**old_ns, "_": target_df}

    old_custom_completers = shell.Completer.custom_completers
    shell.Completer.custom_completers = None

    _, _, _, jedi = shell.Completer.complete(event.symbol)

    shell.Completer.namespace = old_ns
    shell.Completer.custom_completers = old_custom_completers

    return [
        event.symbol + completions.name_with_symbols for completions in jedi
    ]


shell = get_ipython()
shell.set_hook('complete_command', symbolic_completer, re_key = '.*_.*')
