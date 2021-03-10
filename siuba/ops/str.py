# modules allow accessor functions to be import
# e.g. from siuba.ops.str import upper
from .generics import ops_str

globals().update(dict(ops_str))
del ops_str
