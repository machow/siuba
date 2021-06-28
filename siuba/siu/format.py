from .calls import ALL_OPS, Call, MetaArg

class Formatter:
    def __init__(self): pass

    def format(self, call, pad = 0):
        """Return a Symbolic or Call back as a nice tree, with boxes for nodes."""

        fmt_block = "█─"
        fmt_pipe = "├─"
        
        if isinstance(call, MetaArg):
            return "_"

        if isinstance(call, Call):
            call_str = fmt_block + ALL_OPS.get(call.func, repr(call.func))

            args_str = [self.format(arg) for arg in call.args]

            # format keyword args, making sure "└─<key> = █─" aligns box's children
            kwargs_str = []
            for k, v in call.kwargs.items():
                kwargs_str.append(
                        k + " = " + self.format(v, pad = len(k) + 3)
                        )

            all_args = [*args_str, *kwargs_str]
            padded = []
            for ii, entry in enumerate(all_args):
                chunk = self.fmt_pipe(
                        entry,
                        final = ii == len(all_args) - 1,
                        pad = pad
                        )
                padded.append(chunk)

            return "".join([call_str, *padded])

        return repr(call)


    @staticmethod
    def fmt_pipe(x, final = False, pad = 0):
        if not final:
            connector = "│ " if not final else "  "
            prefix = "├─"
        else:
            connector = "  "
            prefix = "└─"

        connector = "\n" + " "*pad + connector
        prefix = "\n" + " "*pad + prefix
        # NOTE: because visiting is depth first, this is essentially prepending
        # the text to the left.
        return prefix + connector.join(x.splitlines())



