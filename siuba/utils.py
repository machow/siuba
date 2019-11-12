# TODO: move siu.py into its own folder, add this to it (w/ Call Trees)
from typing import Any, Union, TypeVar
import inspect

def is_union(x):
    return getattr(x, '__origin__', None) is Union

def get_union_args(x):
    return getattr(x, '__args__', getattr(x, '__union_args__', None))

def is_flex_subclass(x, cls):
    if x is Any:
        return True
    
    return issubclass(x, cls)

def is_dispatch_func_subtype(f, input_cls, output_cls):
    """Returns whether a singledispatch function is subtype of some input and result class.
    
    A function is a subtype if it is input contravariant, and result covariant.
    
    Rules for evaluating return types <= output_cls:
        * Any always returns True
        * Union[A, B] returns true if either A or B are covariant
        * f(arg_name:TypeVar) -> TypeVar compares input_cls and output_cls
        * Simple return types checked via issubclass
        
    Args:
        input_cls - input class for first argument to function
        output_cls - output class for function result
    
    """
    sig = inspect.signature(f)
    # result annotation
    res_type = sig.return_annotation
    
    # first parameter annotation
    par0 = next(iter(sig.parameters.values()))
    par_type0 = par0.annotation
    
    # Case 1: no annotation
    if res_type is None:
        return False
    
    # Case 2: fancy annotations: Union, generic TypeVar
    if is_union(res_type) and get_union_args(res_type):
        sub_types = get_union_args(res_type)
        # passes if any unioned types are subclasses
        return any(map(lambda x: is_flex_subclass(x, output_cls), sub_types))
    elif isinstance(res_type, TypeVar):
        if res_type == par_type0:
            # using a generic type variable as first arg and result
            # return type must be covariant on input_cls
            return issubclass(input_cls, output_cls) and res_type.__covariant__
        else:
            raise TypeError("Generic type used as result, but not as first parameter")

    return is_flex_subclass(res_type, output_cls)
