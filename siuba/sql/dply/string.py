from siuba.dply.string import str_c
from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy import sql

@str_c.register(ClauseElement)
def _str_c_sql(x, *args, sep = "", collapse = None) -> ClauseElement:
    """
    Example:
    """

    if collapse is not None:
        raise NotImplementedError("For SQL, collapse argument of str_c not supported")

    if sep != "":
        raise NotImplementedError('For SQL, sep argument of str_c must be ""')

    return sql.func.concat(x, *args)
