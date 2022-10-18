from . import (
    arrange,
    compute,
    conditional,
    count,
    distinct,
    explain,
    filter,
    group_by,
    head,
    join,
    mutate,
    select,
    summarize,
)

def __getattr__(name):
    import warnings

    if name == "LazyTbl":
        from ..backend import LazyTbl

        warnings.warn(
            "Importing LazyTbl from siuba.sql.verbs is deprecated. Please use "
            "`from siuba.sql import LazyTbl`",
            DeprecationWarning
        )
        return LazyTbl

    raise AttributeError(f"module 'siuba.sql.verbs' has no attribute '{name}'")

