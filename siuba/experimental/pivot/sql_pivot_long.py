import pandas as pd

from sqlalchemy import sql
from sqlalchemy import types as sqla_types

from siuba.dply.verbs import spread
from siuba.sql import LazyTbl
from siuba.sql.utils import (
    _sql_select,
    _sql_column_collection,
    _sql_add_columns,
    _sql_with_only_columns,
)


from .pivot_long import pivot_longer, pivot_longer_spec, build_longer_spec, spec_to_multiindex


def unpack_spec_row(d):
    internal = {".name", ".value"}
    return d[".name"], d[".value"], {k:v for k,v in d.items() if k not in internal}


def _safe_to_dict(df, *args, **kwargs):
    """Return something like df.to_dict(), but ensure t contains no numpy types.

    For context on this pandas issue, see issues linked in this PR:
    https://github.com/pandas-dev/pandas/issues/13258

    """
    import json
    return json.loads(df.to_json(*args, **kwargs))


def _values_to_select(sel_columns, spec_row: dict, value_vars: "list[str]"):
    final_cols = []
    for long_name in value_vars:
        wide_name = spec_row[long_name]
        if pd.isna(wide_name):
            final_cols.append(sql.null().label(long_name))
        else:
            final_cols.append(sel_columns[wide_name].label(long_name))

    return final_cols


@build_longer_spec.register
def _build_longer_spec(__data: LazyTbl, *args, **kwargs):
    # building the spec only really needs the columns names. however, because we 
    # matched tidyr behavior, we just pass a DataFrame in for now.
    df_data = pd.DataFrame(columns = list(__data.last_op.alias().columns.keys()))

    return build_longer_spec(df_data, *args, **kwargs)


@pivot_longer_spec.register
def _pivot_longer_spec(
    __data: LazyTbl,
    spec,
    names_repair = "check_unique",
    values_drop_na: bool = False,
    values_ptypes = None,
    values_transform = None
) -> LazyTbl:

    if values_ptypes is not None:
        raise NotImplementedError()

    if values_transform is not None:
        raise NotImplementedError()


    sel = __data.last_op
    sel_alias = sel.alias()


    # extract info from spec ----

    column_index = spec_to_multiindex(spec)

    wide_names = list(spec[".name"].unique())
    wide_ids = [name for name in sel_alias.columns.keys() if name not in wide_names]

    long_name_vars = [k for k in spec.columns if k not in {".name", ".value"}]
    long_val_vars = list(spec[".value"].unique())


    # guard against bad specifications ----

    bad_names = set(wide_names) - set(sel_alias.columns.keys())
    if bad_names:
        raise ValueError(f"Pivot spec contains columns not in the data: {bad_names}")


    # reshape to long (via union all) ----

    sel_cols = sel_alias.columns

    # each row maps <new_name>: literal for name vars, or <new_name>: column 
    aligned_vars = spread(spec, ".value", ".name")

    union_parts = []
    for row in _safe_to_dict(aligned_vars, orient="records"):
        id_cols = [sel_cols[_id] for _id in wide_ids]
        
        # TODO: handle when value name (row[k]) is NULL
        value_cols = _values_to_select(sel_cols, row, long_val_vars)
        name_cols = [
            sql.literal(row[k]).label(k)
            for k in long_name_vars
        ]

        union_parts.append(_sql_select([*id_cols, *name_cols, *value_cols]))

    # TODO: what is the base class we are willing to let the select type be?
    # this is a CompoundSelect. Shares GenerativeSelect with sql.select()
    sel_union = sql.union_all(*union_parts)

    if values_drop_na:
        alias = sel_union.alias()

        # TODO: sqlalchemy 1.4+ prefers .is_not()
        bool_clause = sql.and_(*[alias.columns[k].isnot(None) for k in long_val_vars])

        return __data.append_op(alias.select().where(bool_clause))

    return __data.append_op(sel_union)
        

# simply calls build_longer_spec and pivot_longer_spec
pivot_longer.register(LazyTbl, pivot_longer.dispatch(pd.DataFrame))



