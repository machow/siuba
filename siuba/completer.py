from IPython import get_ipython


def symbolic_completer(shell, event):
    old_ns = shell.Completer.namespace
    target_df = shell.user_ns["mtcars"]
    shell.Completer.namespace = {**old_ns, "_": target_df}

    jedi = shell.Completer._jedi_matches(len(event.symbol), 0, event.symbol)

    shell.Completer.namespace = old_ns

    return [
        event.symbol + completions.name_with_symbols for completions in jedi
    ]


shell = get_ipython()
shell.set_hook('complete_command', symbolic_completer, re_key = '.*_.*')
