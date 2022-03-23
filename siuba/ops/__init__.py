from .generics import ALL_OPS, PLAIN_OPS
from .utils import _register_series_default

# register default series methods on all operations
for _generic in ALL_OPS.values():
    _register_series_default(_generic)

del _generic
del _register_series_default


# import accessor generics. These are included in ALL_OPS, but since we want
# users to be able to import from them, also need to be modules. Start their
# names with underscores just to keep the files together.

globals().update(PLAIN_OPS)

