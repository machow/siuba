from sqlalchemy import sql

from siuba.dply.verbs import summarize

from .mutate import _sql_upsert_columns, _eval_expr_arg, _eval_expr_kwarg

from ..utils import lift_inner_cols, _sql_with_only_columns
from ..backend import LazyTbl, get_single_from


@summarize.register(LazyTbl)
def _summarize(__data, *args, **kwargs):
    # https://stackoverflow.com/questions/14754994/why-is-sqlalchemy-count-much-slower-than-the-raw-query

    # get query with correct from clause, and maybe unneeded subquery
    safe_from = __data.last_select.alias()
    result_names, sel = _aggregate_cols(__data, safe_from, args, kwargs, "Summarize")

    # see if we can remove subquery
    out_sel = _collapse_select(sel, safe_from)

    from_tbl = get_single_from(out_sel)
    group_cols = [from_tbl.columns[k] for k in __data.group_by]

    final_sel = out_sel.group_by(*group_cols)

    new_data = __data.append_op(final_sel, group_by = tuple(), order_by = tuple())
    return new_data


def _collapse_select(outer_sel, inner_alias):
    # check whether any outer columns reference an inner label ----
    inner_sel = inner_alias.element

    columns = lift_inner_cols(outer_sel)
    inner_cols = lift_inner_cols(inner_sel)

    inner_labels = set([
        x.name for x in inner_cols 
        if isinstance(x, sql.elements.Label)
    ])

    col_requires_cte = set(inner_alias.columns[k] for k in inner_labels)

    bad_refs = []

    def collect_refs(el):
        if el in col_requires_cte:
            bad_refs.append(el)

    for col in columns:
        sql.util.visitors.traverse(col, {}, {"column": collect_refs})

    # if possible, remove the outer query ----
    if not (bad_refs or len(inner_sel._group_by_clause)):
        from sqlalchemy.sql.elements import ColumnClause, Label

        from_obj = get_single_from(inner_sel)
        adaptor = sql.util.ClauseAdapter(
            from_obj,
            adapt_on_names=True,
            include_fn=lambda c: isinstance(c, (ColumnClause, Label))
        )

        new_cols = []
        for col in columns:
            if isinstance(col, Label):
                res = adaptor.traverse(col.element).label(col.name)
                new_cols.append(res)

            else:
                new_cols.append(adaptor.traverse(col))
        #new_cols = list(map(adaptor.traverse, columns))

        return _sql_with_only_columns(inner_sel, new_cols)

    return outer_sel


def _aggregate_cols(__data, subquery, args, kwargs, verb_name):
    # cases:
    #   * grouping cols can not be overwritten (in dbplyr they can't be ref'd)
    #   * no existing labels referred to - can use same select
    #   * existing labels referred to - need 1 subquery tops
    #   * groups + summarize columns can replace everything

    def get_label_clauses(clause):
        out = []
        sql.util.visitors.traverse(clause, {}, {"label": lambda c: out.append(c)})

        return out

    def quote_varname(x):
        return f"`{x}`"

    def validate_references(arg_name, expr, verb_name):
        bad_varnames = get_label_clauses(expr)
        repr_names = ", ".join(map(quote_varname, [el.name for el in bad_varnames]))

        if not bad_varnames:
            return

        raise NotImplementedError(
            f"In SQL, you cannot refer to a column created in the same {verb_name}. "
            f"`{arg_name}` refers to columns created earlier: {repr_names}."
        )

    sel = subquery.select()

    final_cols = {k: subquery.columns[k] for k in __data.group_by}

    # handle args ----
    for ii, func in enumerate(args):
        cols_result = _eval_expr_arg(__data, sel, func, verb_name, window=False)
        
        for col in cols_result:
            validate_references(col.name, col.element, verb_name)
            final_cols[col.name] = col

        sel = _sql_upsert_columns(sel, cols_result)


    # handle kwargs ----
    for new_name, func in kwargs.items():
        labeled = _eval_expr_kwarg(__data, sel, func, new_name, verb_name, window=False)

        validate_references(labeled.name, labeled.element, verb_name)
        final_cols[new_name] = labeled

        sel = _sql_upsert_columns(sel, [labeled])

    return list(final_cols), _sql_with_only_columns(sel, list(final_cols.values()))
