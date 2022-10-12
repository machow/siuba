"""
Implements LazyTbl to represent tables of SQL data, and registers it on verbs.

This module is responsible for the handling of the "table" side of things, while
translate.py handles translating column operations.


"""

from ..backend import LazyTbl
from ..utils import _sql_simplify_select

from siuba.dply.verbs import show_query


@show_query.register(LazyTbl)
def _show_query(tbl, simplify = False, return_table = True):
    #query = tbl.last_op #if not simplify else 
    compile_query = lambda query: query.compile(
                dialect = tbl.source.dialect,
                compile_kwargs = {"literal_binds": True}
            )


    if simplify:
        # try to strip table names and labels where unnecessary
        simple_sel = _sql_simplify_select(tbl.last_select)

        explained = compile_query(simple_sel)
    else:
        # use a much more verbose query
        explained = compile_query(tbl.last_select)

    if return_table:
        print(str(explained))
        return tbl

    return str(explained)
