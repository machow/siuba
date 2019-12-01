from .verbs import *

# preceed w/ underscore so it isn't exported by default
# we just want to register the singledispatch funcs
from .dply import vector as _vector
from .dply import string as _string
