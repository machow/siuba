from importlib.abc import Loader, MetaPathFinder
from importlib.machinery import ModuleSpec
from importlib.util import find_spec
from types import ModuleType
import sys

from functools import wraps




class CallFinder(MetaPathFinder):
    def __init__(self, f):
        self.enabled = True
        self.f = f

    def find_spec(self, fullname, path, target = None):
        if not self.enabled:
            return None

        pkg, *subpkgs = fullname.split('.')
        if pkg == 'meta_hook' and len(subpkgs):
            self.enabled = False
            spec = find_spec(".".join(subpkgs))
            self.enabled = True
            spec.loader = CallLoader(self.f, spec.loader, spec)
            return spec

    #def invalidate_caches(self):
    #    pass

class CallLoader(Loader):
    def __init__(self, f, orig_loader, spec):
        self.f = f
        self.orig_loader = orig_loader
        self.orig_module = None
        self.orig_spec = spec

    def create_module(self, spec):
        self.orig_module = self.orig_loader.create_module(spec)
        if self.orig_module is None:
            self.orig_module = ModuleType(spec.name)

        return self.orig_module


    def exec_module(self, module):
        self.orig_loader.exec_module(self.orig_module)
        for k,v in self.orig_module.__dict__.items():
            if k.startswith('_'):
                module.__dict__[k] = v
            else:
                module.__dict__[k] = self.f(v)


from haste import Symbolic, Call

def lazy_func(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return Symbolic(Call(f, *args, **kwargs))

    return wrapper


if sys.meta_path[0].__class__.__name__ == "CallFinder":
    sys.meta_path[0] = CallFinder(lazy_func)
else:
    sys.meta_path = [CallFinder(lazy_func)] + sys.meta_path

__path__ = ''

