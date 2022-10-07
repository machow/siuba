from siuba.dply.verbs import head

from ..backend import LazyTbl

@head.register(LazyTbl)
def _head(__data, n = 5):
    sel = __data.last_select
    
    return __data.append_op(sel.limit(n))
