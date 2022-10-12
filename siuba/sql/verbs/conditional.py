import warnings

from sqlalchemy import sql

from siuba.dply.verbs import case_when, if_else
from siuba.siu import Call

from ..backend import LazyTbl


@case_when.register(sql.base.ImmutableColumnCollection)
def _case_when(__data, cases):
    # TODO: will need listener to enter case statements, to handle when they use windows
    if isinstance(cases, Call):
        cases = cases(__data)

    whens = []
    case_items = list(cases.items())
    n_items = len(case_items)

    else_val = None
    for ii, (expr, val) in enumerate(case_items):
        # handle where val is a column expr
        if callable(val):
            val = val(__data)

        # handle when expressions
        #if ii+1 == n_items and expr is True:
        #    else_val = val
        if expr is True:
            # note: only sqlalchemy v1.3 requires wrapping in literal
            whens.append((sql.literal(expr), val))
        elif callable(expr):
            whens.append((expr(__data), val))
        else:
            whens.append((expr, val))

    return sql.case(whens, else_ = else_val)


@case_when.register(LazyTbl)
def _case_when(__data, cases):
    raise NotImplementedError(
        "`case_when()` must be used inside a verb like `mutate()`, when using a "
        "SQL backend."
    )
        

# if_else ---------------------------------------------------------------------

@if_else.register(sql.elements.ColumnElement)
def _if_else(cond, true_vals, false_vals):
    whens = [(cond, true_vals)]
    return sql.case(whens, else_ = false_vals)
