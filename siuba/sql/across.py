from siuba.dply.across import across, _get_name_template, _across_setup_fns, ctx_verb_data, ctx_verb_window
from siuba.dply.tidyselect import var_select, var_create
from siuba.siu import FormulaContext, Call

from .backend import LazyTbl
from .utils import _sql_select, _sql_column_collection

from sqlalchemy import sql


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


@across.register(sql.base.ImmutableColumnCollection)
def _across_sql_cols(
    __data: sql.base.ImmutableColumnCollection,
    cols,
    fns,
    names: "str | None" = None
) -> sql.base.ImmutableColumnCollection:

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
        context = FormulaContext(Fx=crnt_col, _=__data)

        # iterate over functions ----
        for fn_name, fn in fns_map.items():
            fmt_pars = {"fn": fn_name, "col": new_name}

            new_call = lazy_tbl.shape_call(
                fn,
                window,
                verb_name="Across",
                arg_name = f"function {fn_name} of {len(fns_map)}"
            )

            res = new_call(context)
            res_name = name_template.format(**fmt_pars)
            results.append(res.label(res_name))

    return _sql_column_collection(results)
