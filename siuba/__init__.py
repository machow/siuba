# version ---------------------------------------------------------------------
__version__ = "1.0.0a3"

# default imports--------------------------------------------------------------
from .siu import _, Lam
from .dply.verbs import *
from .dply.verbs import __all__ as ALL_DPLY

# necessary, since _ won't be exposed in import * by default
__all__ = ['_', *ALL_DPLY]
