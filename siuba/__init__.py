# version ---------------------------------------------------------------------
__version__ = "0.4.0rc1"

# default imports--------------------------------------------------------------
from .siu import _, Fx, Lam
from .dply.across import across
from .dply.verbs import *
from .dply.verbs import __all__ as ALL_DPLY

# necessary, since _ won't be exposed in import * by default
__all__ = ['_', "Fx", "across", *ALL_DPLY]
