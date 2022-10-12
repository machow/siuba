import pandas as pd
from pandas.api import types as pd_types

from pandas.core.groupby import DataFrameGroupBy
from .verbs import var_select, var_create
from ..siu import FormulaContext, Call, strip_symbolic, Fx, FuncArg
from ..siu.dispatchers import verb_dispatch, symbolic_dispatch, create_eager_pipe_call

from collections.abc import Mapping
from contextvars import ContextVar
from contextlib import contextmanager
from typing import Callable, Any

DEFAULT_MULTI_FUNC_TEMPLATE = "{col}_{fn}"
DEFAULT_SINGLE_FUNC_TEMPLATE = "{col}"


ctx_verb_data = ContextVar("data")
ctx_verb_window = ContextVar("window")


def _is_symbolic_operator(f):
    # TODO: consolidate these checks, make the result of symbolic_dispatch a class.
    return callable(f) and getattr(f, "_siu_symbolic_operator", False)


def _require_across(call, verb_name):
    if (
        not isinstance(call, Call) 
        or not (call.args and getattr(call.args[0], "__name__", None) == "across")
    ):
        raise NotImplementedError(
            f"{verb_name} currently only allows a top-level across as an unnamed argument.\n\n"
            f"Example: {verb_name}(some_data, across(...))"
        )


def _eval_with_context(ctx, window_ctx, data, expr):
    # TODO: should just set the translator as context (e.g. agg translater, etc..)
    token = ctx_verb_data.set(ctx)
    token_win = ctx_verb_window.set(window_ctx)

    try:
        return expr(data)
    finally:
        ctx_verb_data.reset(token)
        ctx_verb_window.reset(token_win)


@contextmanager
def _set_data_context(ctx, window):
    try:
        token = ctx_verb_data.set(ctx)
        token_win = ctx_verb_window.set(window)
        yield
    finally:
        ctx_verb_data.reset(token)
        ctx_verb_window.reset(token_win)



# TODO: handle DataFrame manipulation in pandas / sql backends
class AcrossResult(Mapping):
    def __init__(self, *args, **kwargs):
        self.d = dict(*args, **kwargs)

    def __getitem__(self, k):
        return self.d[k]

    def __iter__(self):
        return iter(self.d)

    def __len__(self):
        return len(self.d)


def _across_setup_fns(fns) -> "dict[str, Callable[[FormulaContext], Any]]":
    final_calls = {}
    if isinstance(fns, (list, tuple)):
        raise NotImplementedError(
            "Specifying functions as a list or tuple is not supported. "
            "Please use a dictionary to define multiple functions to apply. \n\n"
            "E.g. across(_[:], {'round': Fx.round(), 'round2': Fx.round() + 1})"
        )
    elif isinstance(fns, dict):
        for name, fn_call_raw in fns.items():
            # symbolics get stripped by default for arguments to verbs, but
            # these are inside a dictionary, so need to strip manually.
            fn_call = strip_symbolic(fn_call_raw)

            if isinstance(fn_call, Call):
                final_calls[name] = fn_call

            elif callable(fn_call):
                final_calls[name] = create_eager_pipe_call(FuncArg(fn_call), Fx)

            else:
                raise TypeError(
                    "All functions to be applied in across must be a siuba.siu.Call, "
                    f"but received a function of type {type(fn_call)}"
                )

    elif isinstance(fns, Call):
        final_calls["fn1"] = fns

    elif callable(fns):
        final_calls["fn1"] = create_eager_pipe_call(FuncArg(fns), Fx)

    else:
        raise NotImplementedError(f"Unsupported function type in across: {type(fns)}")

    return final_calls


def _get_name_template(fns, names: "str | None") -> str:
    if names is not None:
        return names

    if callable(fns):
        return DEFAULT_SINGLE_FUNC_TEMPLATE

    return DEFAULT_MULTI_FUNC_TEMPLATE


@verb_dispatch(pd.DataFrame)
def across(__data, cols, fns, names: "str | None" = None) -> pd.DataFrame:

    name_template = _get_name_template(fns, names)
    selected_cols = var_select(__data.columns, *var_create(cols), data=__data)

    fns_map = _across_setup_fns(fns)

    results = {}
    for old_name, new_name in selected_cols.items():
        if new_name is None:
            new_name = old_name

        crnt_ser = __data[old_name]
        context = FormulaContext(Fx=crnt_ser, _=__data)

        for fn_name, fn in fns_map.items():
            fmt_pars = {"fn": fn_name, "col": new_name}
            
            res = fn(context)
            results[name_template.format(**fmt_pars)] = res

    # ensure at least one result is not a scalar, so we don't get the classic
    # pandas error: "If using all scalar values, you must pass an index"
    index = None
    if results:
        _, v = next(iter(results.items()))
        if pd_types.is_scalar(v):
           index = [0] 

    return pd.DataFrame(results, index=index)


@symbolic_dispatch(cls = pd.Series)
def where(x) -> bool:
    if not isinstance(x, bool):
        raise TypeError("Result of where clause must be a boolean (True or False).")

    return x
    
