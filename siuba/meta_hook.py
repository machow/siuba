"""
DEPRECATED.

Note that this module was experimental, and created very early in siuba's development.
You should not rely on it for anything important.
"""

from importlib.abc import Loader, MetaPathFinder
from importlib.machinery import ModuleSpec
from importlib.util import find_spec
import importlib
from types import ModuleType
import sys
import warnings

from functools import wraps
import pdb
import readline

warnings.warn(
    "The siuba.meta_hook module is DEPRECATED and will be removed in a future release."
)




class CallFinder(MetaPathFinder):
    def __init__(self, f):
        self.enabled = True
        self.f = f

    def find_spec(self, fullname, path, target = None):
        if not self.enabled:
            return None

        pkg, *subpkgs = fullname.split('.')
        if fullname.startswith("siuba.meta_hook") and len(subpkgs) > 1:
        #if pkg == 'meta_hook' and len(subpkgs):
            self.enabled = False
            spec = find_spec(".".join(subpkgs[1:]))
            self.enabled = True
            #spec.loader = CallLoader(self.f, spec.loader, spec)
            return ModuleSpec(fullname, CallLoader(self.f, spec))
        elif pkg == "meta_hook":
            pass

    #def invalidate_caches(self):
    #    pass

class CallLoader(Loader):
    def __init__(self, f, spec):
        self.f = f
        self.orig_spec = spec

    def create_module(self, spec):
        #self.orig_module = self.orig_spec.loader.create_module(self.orig_spec)
        self.orig_module = importlib.import_module(self.orig_spec.name)        
        #self.orig_module = self.orig_loader.create_module(spec)
        #if self.orig_module is None:
        #    self.orig_module = ModuleType(spec.name)

        #return self.orig_module
        return None


    def exec_module(self, module):
        #self.orig_loader.exec_module(self.orig_module)

        #for k,v in self.orig_module.__dict__.items():
        all_items = list(self.orig_module.__dict__.items())
        for k,v in all_items:
            if k.startswith('_'):
                module.__dict__[k] = v
            else:
                module.__dict__[k] = self.f(v)


from .siu import Symbolic, Call

def lazy_func(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return Symbolic(source = f)(*args, **kwargs)

    return wrapper


if sys.meta_path[0].__class__.__name__ == "CallFinder":
    sys.meta_path[0] = CallFinder(lazy_func)
else:
    sys.meta_path = [CallFinder(lazy_func)] + sys.meta_path

__path__ = ''

