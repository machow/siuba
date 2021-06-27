# modules allow accessor functions to be import
# e.g. from siuba.ops.str import upper
from .generics import ops_dt

globals().update(dict(ops_dt))
del ops_dt
