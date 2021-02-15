# TODO: this is to make sure everything can be wired up. Should refactor
#       CallTreeLocal to be less finicky.
# NOTE: put at bottom to avoid issue with circular import
#       could be moved into separate module
from .generics import ALL_OPS, ALL_ACCESSORS, ALL_PROPERTIES
from siuba.siu import CallTreeLocal

# TODO: I hate this
def create_pandas_translator(local, dispatch_cls, result_cls):
    return CallTreeLocal(
            local,
            call_sub_attr = tuple(ALL_ACCESSORS),
            chain_sub_attr = True,
            dispatch_cls = dispatch_cls,
            result_cls = result_cls,
            call_props = tuple(ALL_PROPERTIES)
            )

