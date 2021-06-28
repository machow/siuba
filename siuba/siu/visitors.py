# Trees and Visitors ==========================================================
from .calls import Call, FuncArg
from .error import ShortException
from .symbolic import strip_symbolic

class FunctionLookupError(ShortException): pass

class FunctionLookupBound:
    def __init__(self, msg):
        self.msg = msg

    def __call__(self):
        raise NotImplementedError(self.msg)


class CallVisitor:
    """
    A node visitor base class that walks the call tree and calls a
    visitor function for every node found.  This function may return a value
    which is forwarded by the `visit` method.

    Note: essentially a copy of ast.NodeVisitor
    """

    def visit(self, node):
        """Visit a node."""
        method = 'visit_' + node.func
        visitor = getattr(self, method, self.generic_visit)
        return visitor(node)

    def generic_visit(self, node):
        """Called if no explicit visitor function exists for a node."""
        
        node.map_subcalls(self.visit)

    @classmethod
    def quick_visitor(cls, visit_dict, node):
        """Class method to quickly define and run a custom visitor."""

        qv = type('QuickVisitor', cls, visit_dict)
        qv.visit(node)

class CallListener:
    """Generic listener. Each exit is called on a node's copy."""
    def enter(self, node):
        method = 'enter_' + node.func
        f_enter = getattr(self, method, self.generic_enter)

        return f_enter(node)

    def exit(self, node):
        method = 'exit_' + node.func
        f_exit = getattr(self, method, self.generic_exit)
        return f_exit(node)

    def generic_enter(self, node):
        args, kwargs = node.map_subcalls(self.enter)

        return self.exit(node.__class__(node.func, *args, **kwargs))

    def generic_exit(self, node):
        return node

    def enter_if_call(self, x):
        if isinstance(x, Call):
            return self.enter(x)

        return x

def get_attr_chain(node, max_n):
    # TODO: need to make custom calls their own Call class, then will not have to
    #       do these kinds of checks, since a __call__ will always be on a Call obj
    if not isinstance(node, Call):
        return [], node

    out = []
    ttl_n = 0
    crnt_node = node
    while ttl_n < max_n:
        if crnt_node.func != "__getattr__": break

        obj, attr = crnt_node.args
        out.append(attr)

        ttl_n += 1
        crnt_node = obj

    return list(reversed(out)), crnt_node


from inspect import isclass, isfunction
from typing import get_type_hints
from .utils import is_dispatch_func_subtype

class CallTreeLocal(CallListener):
    def __init__(
            self,
            local,
            call_sub_attr = None,
            chain_sub_attr = False,
            dispatch_cls = None,
            result_cls = None,
            call_props = None
            ):
        """
        Arguments:
            local: a dictionary mapping func_name: func, used to replace call expressions.
            call_sub_attr: a set of attributes signaling any subattributes are property
                           methods. Eg. {'dt'} to signify in _.dt.year, year is a property call.
            chain_sub_attr: whether to included the attributes in the above argument, when looking up
                           up a replacement for the property call. E.g. does local have a 'dt.year' entry.
            dispatch_cls: if custom calls are dispatchers, dispatch on this class. If none, use their name
                          to try and get their corresponding local function.
            result_cls: if custom calls are dispatchers, require their result annotation to be a subclass
                          of this class.
            call_props: property methods to potentially convert to local calls.
        """
        self.local = local
        self.call_sub_attr = set(call_sub_attr or [])
        self.chain_sub_attr = chain_sub_attr
        self.dispatch_cls = dispatch_cls
        self.result_cls = result_cls
        self.call_props = set(call_props or [])

    def translate(self, expr):
        """Return the translation of an expression.

        This method is meant to be a high-level entrypoint.

        """

        # note that by contract, don't need to strip symbolic
        return self.enter(strip_symbolic(expr))

    def dispatch_local(self, name):
        func = self.local[name]

        if hasattr(func, "dispatch") and self.result_cls is not None:
            return func.dispatch(self.result_cls)

        return func

    def create_local_call(self, name, prev_obj, cls, func_args = None, func_kwargs = None):
        # need call attr name (arg[0].args[1]) 
        # need call arg and kwargs
        func_args = tuple() if func_args is None else func_args
        func_kwargs = {} if func_kwargs is None else func_kwargs

        try:
            local_func = self.dispatch_local(name)
        except KeyError as err:
            raise FunctionLookupError("Missing translation for function call: %s"% name)

        if isinstance(local_func, FunctionLookupBound):
            raise FunctionLookupError(local_func.msg)
            #local_func()

        #if isclass(local_func) and issubclass(local_func, Exception):
        #    raise local_func

        return cls(
                "__call__",
                local_func,
                prev_obj,
                *func_args,
                **func_kwargs
                )

    def enter(self, node):
        # if no enter metthod for operators, like __invert__, try to get from local
        # TODO: want to only do this if func is the name of an infix op's method
        method = 'enter_' + node.func
        if not hasattr(self, method) and node.func in self.local:
            args, kwargs = node.map_subcalls(self.enter)
            return self.create_local_call(node.func, args[0], Call, args[1:], kwargs)

        return super().enter(node)

    def enter___getattr__(self, node):
        obj, attr = node.args
        if obj.func == "__getattr__" and obj.args[1] in self.call_sub_attr:
            prev_obj, prev_attr = obj.args

            # TODO: should always call exit?
            if self.chain_sub_attr:
                # use chained attribute to look up local function instead
                # e.g. dt.round, rather than round
                attr = prev_attr + '.' + attr
            return self.create_local_call(attr, prev_obj, Call)
        elif attr in self.call_props:
            return self.create_local_call(attr, obj, Call)

        return self.generic_enter(node)

    def enter___custom_func__(self, node):
        func = node(None)
        
        # TODO: not robust at all, need class for singledispatch? unique attr flag?
        if (hasattr(func, 'registry')
            and hasattr(func, 'dispatch')
            and self.dispatch_cls is not None
            ):
            # allow custom functions that dispatch on dispatch_cls
            f_for_cls = func.dispatch(self.dispatch_cls)

            if isinstance(f_for_cls, FunctionLookupBound):
                # TODO: this is a bit funky, since FLB raises when called
                raise FunctionLookupError(f_for_cls.msg)

            if (self.result_cls is None
                or is_dispatch_func_subtype(f_for_cls, self.dispatch_cls, self.result_cls)
                ):
                # matches return annotation type (or not required)
                return node.__class__(f_for_cls)
            
            raise FunctionLookupError(
                    "External function {name} can dispatch on the class {dispatch_cls}, but "
                    "must also have result annotation of (sub)type {result_cls}"
                        .format(
                            name = func.__name__,
                            dispatch_cls = self.dispatch_cls,
                            result_cls = self.result_cls
                            )
                    )

        # doesn't raise an error so we can look in locals for now
        # TODO: remove behavior, once all SQL dispatch funcs moved from locals
        return self.generic_enter(node)

    def enter___call__(self, node):
        """
        Overview:
             variables      _.x.method(1)         row_number(_.x, 1)
             ---------      -------------        --------------------
            
                            █─'__call__'         █─'__call__'                           
             obj            ├─█─.                ├─<function row_number
                            │ ├─█─.              ├─█─.                                  
                            │ │ ├─_              │ ├─_                                  
                            │ │ └─'x'            │ └─'x'                                
                            │ └─'method'         │                                   
                            └─1                  └─1
        """
        obj, *rest = node.args
        args = tuple(self.enter_if_call(child) for child in rest)
        kwargs = {k: self.enter_if_call(child) for k, child in node.kwargs.items()}

        attr_chain, target = get_attr_chain(obj, max_n = 2)
        if attr_chain:
            # want _.x.method() -> method(_.x), need to transform
            if attr_chain[0] in self.call_sub_attr:
                # e.g. _.dt.round()
                call_name = ".".join(attr_chain) if self.chain_sub_attr else attr_chain[-1]
                entered_target = self.enter_if_call(target)
            else:
                call_name = attr_chain[-1]
                entered_target = self.enter_if_call(obj.args[0])

        elif isinstance(obj, FuncArg) and self.dispatch_cls is None:
            # want function(_.x) -> new_function(_.x), has form
            call_name = obj.obj_name()
            # the first argument is basically "self"
            entered_target, *args = args
        else:
            # default to generic enter
            return self.generic_enter(node)

        return self.create_local_call(
                call_name, entered_target, node.__class__,
                args, kwargs
                )


# Also need NodeTransformer



