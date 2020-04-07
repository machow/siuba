from siuba.siu import _, MetaArg, strip_symbolic
import itertools

def enrich_spec_entry(entry):
    accessors = ['str', 'dt', 'cat', 'sparse']
    expr = strip_symbolic(eval(entry["example"], {"_": _}))

    accessor = [ameth for ameth in accessors if ameth in expr.op_vars()] + [None]

    tmp = {
            **entry,
            'is_property': expr.func == "__getattr__",
            'expr_frame': replace_meta_args(expr, _.x, _.y, _.z),
            'expr_series': expr,
            'accessor': accessor[0],
            }
    tmp['action'] = {
            **entry['action'],
            'data_arity': count_call_type(expr, MetaArg),
            }

    return tmp

def get_type_info(call):
    call = strip_symbolic(call)
    func = call.func
    if isinstance(func, str) and func != "__rshift__":
        raise ValueError("Expected first expressions was >>")
        
    out = {}
    expr, result = call.args
    
    accessors = ['str', 'dt', 'cat', 'sparse']
    
    accessor = [ameth for ameth in accessors if ameth in expr.op_vars()] + [None]
            
    return dict(
        expr_series = expr,
        expr_frame = replace_meta_args(expr, _.x, _.y, _.z),
        accessor = accessor[0],
        result = result.to_dict(),
        is_property = expr.func == "__getattr__",
        data_arity = count_call_type(expr, MetaArg)
    )

def count_call_type(call, cls):
    """Counts the number of MetaArgs"""
    meta_count = 0
    def incr_meta_count(node):
        nonlocal meta_count
        if isinstance(node, cls): 
            meta_count += 1
        node.map_subcalls(incr_meta_count)
    
    incr_meta_count(call)
    
    return meta_count

def replace_meta_args(call, *args):
    replacements = map(strip_symbolic, args)
    
    def replace_next(node):
        # swap in first replacement for meta arg (when possible)
        if isinstance(node, MetaArg):
            return next(replacements)
        
        # otherwise, recurse into node
        args, kwargs = node.map_subcalls(replace_next)

        # return a copy of node
        return node.__class__(node.func, *args, **kwargs)

    return replace_next(call)

def dump_spec(spec, stream = None):
    import yaml
    # TODO: do this without mutating SafeDumper...
    yaml.SafeDumper.yaml_representers[None] = lambda self, data: \
        yaml.representer.SafeRepresenter.represent_str(
            self,
            str(data),
        )

    return yaml.safe_dump(spec, stream)
