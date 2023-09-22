# version ---------------------------------------------------------------------
__version__ = "0.4.4"

# default imports--------------------------------------------------------------
from .siu import _, Fx, Lam, pipe, Pipeable
from .dply.across import across
from .dply.verbs import (
    # Dply ----
    group_by, ungroup, 
    select, rename,
    mutate, transmute, filter, summarize,
    arrange, distinct,
    count, add_count,
    head,
    top_n,
    # Tidy ----
    spread, gather,
    nest, unnest,
    expand, complete,
    separate, unite, extract,
    # Joins ----
    join, inner_join, full_join, left_join, right_join, semi_join, anti_join,
    # TODO: move to vectors
    if_else, case_when,
    collect, show_query,
    tbl,
)

# necessary, since _ won't be exposed in import * by default
__all__ = [
    '_',
    "Fx",
    "pipe", "Pipeable",
    "across",
    "group_by", "ungroup", 
    "select", "rename",
    "mutate", "transmute", "filter", "summarize",
    "arrange", "distinct",
    "count", "add_count",
    "head",
    "top_n",
    # Tidy ----
    "spread", "gather",
    "nest", "unnest",
    "expand", "complete",
    "separate", "unite", "extract",
    # Joins ----
    "join", "inner_join", "full_join", "left_join", "right_join", "semi_join", "anti_join",
    "if_else", "case_when",
    "collect", "show_query",
    "tbl"
]
