import json
import yaml
import pkg_resources
import pandas as pd

from siuba.ops import ALL_OPS

from siuba.siu import FunctionLookupBound
from siuba.sql.utils import get_dialect_translator

SQL_BACKENDS = ["postgresql", "redshift", "sqlite", "mysql", "bigquery", "snowflake"]
ALL_BACKENDS = SQL_BACKENDS + ["pandas"]

methods = pd.DataFrame(
        [{"full_name": k, **vars(f.operation)} for k, f in ALL_OPS.items()]
        )


# SQL spec construction -------------------------------------------------------

def read_dialect(name):
    translator = get_dialect_translator(name)

    support = []
    for k in ALL_OPS:
        support.append(read_sql_op(k, name, translator))

    df_support = pd.DataFrame(support)

    return df_support



def read_sql_op(name, backend, translator):
    # TODO: MC-NOTE - cleanup this code
    from siuba.siu.visitors import CodataVisitor, FunctionLookupError
    from siuba.ops.utils import Operation
    co_win = CodataVisitor(translator.window.dispatch_cls)
    co_agg = CodataVisitor(translator.aggregate.dispatch_cls)

    disp_win = translator.window.local[name]
    disp_agg = translator.aggregate.local[name]

    try:
        f_win = co_win.validate_dispatcher(disp_win, strict=False)
        if isinstance(f_win, FunctionLookupBound):
            win_supported = False
        elif disp_win.dispatch(object) is f_win:
            win_supported = False
        else:
            win_supported = True
    except FunctionLookupError:
        f_win = None
        win_supported = False


    try:
        f_agg = co_agg.validate_dispatcher(disp_agg)
        if isinstance(f_agg, FunctionLookupBound):
            agg_supported = False
        else:
            agg_supported = True
    except FunctionLookupError:
        agg_supported = False

    # window functions should be a superset of agg functions
    if f_win is None and agg_supported:
        raise Exception("agg functions in %s without window funcs: %s" %(backend, name))

    if win_supported and not agg_supported:
        flags = "no_aggregate"
    elif agg_supported and not win_supported:
        flags = "no_mutate"
    else:
        flags = ""

    if win_supported or agg_supported:
        metadata = getattr(f_win, "operation", {})
        if isinstance(metadata, Operation):
            metadata = {**vars(metadata)}
        meta = {"is_supported": True, "flags": flags, **metadata}

    else:
        meta = {"is_supported": False, "flags": flags}
    
    return {"full_name": name, "backend": backend, "metadata": meta}


# pandas spec construction ----------------------------------------------------

def read_pandas_ops():
    from siuba.experimental.pd_groups.groupby import SeriesGroupBy

    all_meta = []
    for k, v in ALL_OPS.items():
        has_impl = not isinstance(v.dispatch(SeriesGroupBy), FunctionLookupBound)
        all_meta.append({
            "full_name": k,
            "metadata": dict(is_supported = has_impl),
            "backend": "pandas"}
            )

    return pd.DataFrame(all_meta)

# process examples ------------------------------------------------------------
# examples currently have form like _ + _, or _.corr(_)
# these need to be converted to _.x + _.y, and _x.corr(_.y)

def enrich_spec_entry(entry):
    from siuba.siu import _, strip_symbolic
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

    return tmp

def replace_meta_args(call, *args):
    from siuba.siu import strip_symbolic, MetaArg
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


# Main ========================================================================

sql_methods    = pd.concat(list(map(read_dialect, SQL_BACKENDS)))
pandas_methods = pd.DataFrame(read_pandas_ops())

wide_backends = (
        pd.concat([sql_methods, pandas_methods])
        .pivot("full_name", "backend", "metadata")
        )

full_methods = methods.merge(wide_backends, how = "left", on = "full_name")

# replace NA entries--but pandas' fillna has custom behavior around dicts, so
# need to use this terrible hack
def set_default_support(d):
    if d.get("is_supported"):
        return {**d, "support": "supported"}

    return {**d, "is_supported": False, "support": "maydo"}

for be_name in ALL_BACKENDS:
    full_methods[be_name] = full_methods[be_name].apply(set_default_support)


# Nest backends from each being a column, to a single column of dicts
df_spec = (full_methods
        .assign(backends = lambda _: _[ALL_BACKENDS].to_dict(orient = "records"))
        .drop(columns = ALL_BACKENDS)
        )

fname_spec = pkg_resources.resource_filename("siuba.ops.support", "examples.yml")
with open(fname_spec, "r") as f:
    orig_spec = yaml.safe_load(f)

df_spec["example"] = df_spec.full_name.apply(lambda x: orig_spec[x])

# roundtrip through JSON to get rid of custom numpy types
# probably a much better way to do this
json_spec = df_spec.set_index("full_name").to_json(orient = "index")
raw_spec = json.loads(json_spec)

spec = {k: enrich_spec_entry(entry) for k, entry in raw_spec.items()} 


