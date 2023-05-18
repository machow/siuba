from siuba.dply.across import across, _get_name_template, _across_setup_fns, ctx_verb_data, ctx_verb_window
from siuba.dply.tidyselect import var_select, var_create
from siuba.siu import FormulaContext, Call, FormulaArg
from siuba.siu.calls import str_to_getitem_call
from siuba.siu.visitors import CallListener

from .backend import LazyTbl
from .utils import _sql_select, _sql_column_collection
from .translate import ColumnCollection

from sqlalchemy import sql


class ReplaceFx(CallListener):
    def __init__(self, replacement):
        self.replacement = replacement

    def exit(self, node):
        res = super().exit(node)
        if isinstance(res, FormulaArg):
            return str_to_getitem_call(self.replacement)

        return res


@across.register(LazyTbl)
def _across_lazy_tbl(__data: LazyTbl, cols, fns, names: "str | None" = None) -> LazyTbl:
    raise NotImplementedError(
        "across() cannot called directly on a LazyTbl. Please use it inside a verb, "
        "like mutate(), summarize(), filter(), arrange(), group_by(), etc.."
    )
    #selectable = __data.last_op
    #
    #columns = selectable.alias().columns
    #if not isinstance(columns, ImmutableColumnCollection):
    #    raise TypeError(str(type(columns)))

    #res_cols = across(columns, cols, fns, names)

    #return __data.append_op(_sql_select(res_cols))


@across.register(ColumnCollection)
def _across_sql_cols(
    __data: ColumnCollection,
    cols,
    fns,
    names: "str | None" = None
) -> ColumnCollection:

    lazy_tbl = ctx_verb_data.get()
    window = ctx_verb_window.get()

    name_template = _get_name_template(fns, names)
    selected_cols = var_select(__data, *var_create(cols), data=__data)

    fns_map = _across_setup_fns(fns)
    
    results = []

    # iterate over columns ----
    for new_name, old_name in selected_cols.items():
        if old_name is None:
            old_name = new_name

        crnt_col = __data[old_name]
        #context = FormulaContext(Fx=crnt_col, _=__data)

        # iterate over functions ----
        for fn_name, fn in fns_map.items():
            fmt_pars = {"fn": fn_name, "col": new_name}

            fn_replaced = ReplaceFx(old_name).enter(fn)
            new_call = lazy_tbl.shape_call(
                fn_replaced,
                window,
                verb_name="Across",
                arg_name = f"function {fn_name} of {len(fns_map)}"
            )

            res, windows, _ = lazy_tbl.track_call_windows(new_call, __data)

            #res = new_call(context)
            res_name = name_template.format(**fmt_pars)
            results.append(res.label(res_name))

    return _sql_column_collection(results)
