import pandas as pd
import siuba.ops

from sqlalchemy import sql
from sqlalchemy import types as sqla_types

from siuba.dply.verbs import collect, distinct
from siuba.sql import LazyTbl
from siuba.sql.utils import (
    _sql_select,
    _sql_column_collection,
    _sql_add_columns,
    _sql_with_only_columns,
    _sql_case
)

from .pivot_wide import (
    pivot_wider,
    pivot_wider_spec,
    build_wider_spec,
    _is_select_everything,
    _tidy_select,
    _select_expr_slice
)

from .utils import vec_as_names
from .sql_pivot_long import _safe_to_dict

_OPS_DEFAULT=siuba.ops.max

@build_wider_spec.register
def _build_wider_spec(__data: LazyTbl, *args, **kwargs):
    # building the spec only really needs the columns names. however, because we 
    # matched tidyr behavior, we just pass a DataFrame in for now.
    raise NotImplementedError(
        "build_wider_spec currently requires a DataFrame. Please collect() your "
        f"data first. Received type: {type(__data)}"
    )


@pivot_wider_spec.register
def _pivot_wider_spec(
    __data: LazyTbl,
    spec,
    names_repair = "check_unique",
    id_cols = None,
    id_expand = False,
    values_fill = None,
    values_fn = _OPS_DEFAULT,
    unused_fn = None
):
    # Key differences:
    # * values_fn by default is "MAX"

    lazy_tbl = __data
    __data = pd.DataFrame(columns = list(__data.last_op.alias().columns.keys()))

    if id_expand:
        raise NotImplementedError()

    if values_fill is not None:
        raise NotImplementedError()

    if isinstance(values_fn, str):
        _f = lazy_tbl.translator.aggregate.local.get(values_fn)
        if _f is None:
            raise ValueError(
                f"values_fn={repr(values_fn)} does not have a SQL translation."
            )
        values_fn = _f
    elif not hasattr(values_fn, "dispatch"):
        raise NotImplementedError(
            "values_fn currently must be column operation function. For example:\n\n"
            "from siuba.ops import mean\n"
            "pivot_wider(..., values_fn=mean)"
        )

    # TODO: all of this down to "pivot to wide" taken from original func ------
    if _is_select_everything(id_cols):
        id_cols = None

    if unused_fn is not None:
        raise NotImplementedError()

    if not isinstance(id_expand, bool):
        raise TypeError("`id_expand` argument must be True or False.")

    
    # tidyselect ----

    name_vars = spec.columns[~spec.columns.isin([".name", ".value"])].tolist()
    val_vars = spec.loc[:, ".value"].unique().tolist()

    # select id columns
    if id_cols is None:
        others = {*name_vars, *val_vars}
        id_vars = [col for col in __data.columns if col not in others]
    else:
        id_vars = _tidy_select(__data, id_cols, "id_cols")

    id_var_bad = set(id_vars) & set([*name_vars, *val_vars])
    if id_var_bad:
        raise ValueError(
            "id_cols contains columns that are in "
            f"names_from or values_from: {id_var_bad}."
        )


    # pivot to wide -----------------------------------------------------------
    
    # each row of spec becomes a CASE_WHEN.
    # spec columns: .name, .value, <name_cols...>
    # SELECT
    #     FN(CASE
    #         WHEN {<name_col_key1>} == {<name_col_val1>} and [...] THEN {.value1}
    #     ) AS .name1,
    #     ... AS .name2,
    #     ... AS .name3
    sel_alias = lazy_tbl.last_op.alias()
    sel_cols = sel_alias.columns
    dispatch_cls = lazy_tbl.translator.aggregate.dispatch_cls

    wide_name_cols = []
    for row in _safe_to_dict(spec, orient="records"):
        when_clause = sql.and_(sel_cols[k] == row[k] for k in name_vars)
        when_then = (when_clause, sel_cols[row[".value"]])

        col = values_fn(dispatch_cls(), _sql_case(when_then))

        wide_name_cols.append(col)

    wide_id_cols = [sel_cols[id_] for id_ in id_vars]

    repaired_names = vec_as_names([*id_vars, *spec[".name"]], repair=names_repair)
    labeled_cols = [
        col.label(name) for name, col in
        zip(repaired_names, [*wide_id_cols, *wide_name_cols])
    ]
    
    final_sel = _sql_select(labeled_cols).group_by(*wide_id_cols)

    return lazy_tbl.append_op(final_sel)


# simply calls build_wider_spec and pivot_wider_spec
@pivot_wider.register
def _pivot_wider(
    __data: LazyTbl,
    id_cols=None,
    id_expand=False,
    names_from="name",
    names_prefix="",
    names_sep="_",
    names_glue=None,
    names_sort=None,
    names_vary="fastest",
    names_expand=False,
    names_repair="check_unique",
    values_from="value",
    values_fill=None,
    values_fn=_OPS_DEFAULT,
    unused_fn=None
):
    # note that we use three forms of the data: __data for tidyselect,
    # distinct_data for spec creation, and lazy_tbl for the actual pivot
    lazy_tbl = __data    
    __data = pd.DataFrame(columns = list(__data.last_op.alias().columns.keys()))

    # tidyselect variable names -----------------------------------------------
    # adapted from pivot_wide
    name_vars = _tidy_select(__data, names_from, "names_from")
    val_vars = _tidy_select(__data, values_from, "values_from")
    if id_cols is None:
        others = {*name_vars, *val_vars}

        id_cols = tuple([col for col in __data.columns if col not in others])
        id_vars = _select_expr_slice(id_cols)
    else:
        id_vars = id_cols


    # create dummy data with all names_from levels ----------------------------
    distinct_data = collect(distinct(lazy_tbl, *name_vars)).copy()
    distinct_data[list(val_vars)] = True

    vec_as_names(list(distinct_data.columns), repair="check_unique")


    # build spec and pivot ----------------------------------------------------
    spec = build_wider_spec(
        distinct_data,
        names_from = names_from,
        values_from = values_from,
        names_prefix = names_prefix,
        names_sep = names_sep,
        names_glue = names_glue,
        names_sort = names_sort,
        names_vary = names_vary,
        names_expand = names_expand
    )


    out = pivot_wider_spec(
        lazy_tbl,
        spec,
        names_repair = names_repair,
        id_cols = id_vars,
        id_expand = id_expand,
        values_fill = values_fill,
        values_fn = values_fn,
        unused_fn = unused_fn
    )

    return out
