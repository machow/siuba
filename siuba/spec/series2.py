from siuba.ops import ALL_OPS
import pandas as pd

from siuba.siu import FunctionLookupBound

from siuba import _, select, filter, mutate, left_join, if_else

methods = (pd.DataFrame([vars(f.operation) for k, f in ALL_OPS.items()])
        >> mutate(
            full_name = if_else(_.accessor.notna(), _.accessor + '.' + _.name, _.name)
        )
)

#from siuba import *
#filter(methods, _.accessor.str.contains("str"))


from siuba.sql.utils import get_dialect_translator


def read_sql_op(name, f):
    support =  not isinstance(f, FunctionLookupBound)
    metadata = getattr(f, "operation", {})
    return {"full_name": name, "is_supported": support, **metadata}

def read_pandas_ops():
    from siuba.ops import ALL_OPS
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

    

def read_dialect(name):
    translator = get_dialect_translator(name)

    win_support = []
    for k, v in translator.window.local.items():
        win_support.append(read_sql_op(k, v))

    agg_support = []
    for k, v in translator.aggregate.local.items():
        agg_support.append(read_sql_op(k, v))

    wins = pd.DataFrame(win_support).convert_dtypes()
    aggs = pd.DataFrame(agg_support).convert_dtypes()

    missing = set(aggs.full_name) - set(wins.full_name)
    if len(missing) > 0:
        raise Exception("agg functions in %s without window funcs: %s" %(name, missing))

    from siuba import _, mutate, transmute

    res = (wins
        >> left_join(
            _,
            select(aggs, _.full_name, _.is_supported),
            "full_name"
            )
        >> mutate(
                is_supported = _["is_supported_x"] | _["is_supported_y"],
                flags = if_else(_.is_supported & ~_["is_supported_x"], "no_mutate", ""),
            )
        >> select(-_.is_supported_x, -_.is_supported_y)
        >> transmute(
            _.full_name,
            backend = name,
            metadata = _.drop(columns = ["full_name", "backend"]).to_dict(orient = "records")
            )
        >> mutate(
            metadata = _.metadata.map(rm_na_entries)
            )
        )

    return res


def rm_na_entries(mapping):
    return {k: v for k,v in mapping.items() if not pd.isna(v)}

from siuba import *

sql_backend_names = ["postgresql", "redshift", "sqlite"]
    
sql_methods = pd.concat(list(map(read_dialect, sql_backend_names)))

pandas_methods = pd.DataFrame(read_pandas_ops())

wide_backends = pd.concat([sql_methods, pandas_methods]) >> spread("backend", "metadata")

full_methods = (methods
  >> left_join(_, wide_backends, "full_name")
  )

def set_default_support(d):
    if pd.isna(d):
        return {"is_supported": False, "support": "maydo"}

    if d.get("is_supported"):
        return {**d, "support": "supported"}

    return {**d, "is_supported": False, "support": "maydo"}

# replace NA entries--but pandas' fillna has custom behavior around dicts, so
# need to use this terrible hack
all_backend_names = sql_backend_names + ["pandas"]
for be_name in all_backend_names:
    full_methods[be_name] = full_methods[be_name].apply(set_default_support)

df_spec = (full_methods
    >> mutate(backends = _[all_backend_names].to_dict(orient = "records"))
    >> select(-_[all_backend_names])
    )

spec = df_spec.to_dict(orient = "records")

import yaml

import pkg_resources
fname_spec = pkg_resources.resource_filename("siuba.spec", "series.yml")
with open(fname_spec, "r") as f:
    orig_spec = yaml.load(f, Loader = yaml.SafeLoader)

df_spec["example"] = df_spec.full_name.apply(lambda x: orig_spec[x]["example"])

# roundtrip through JSON to get rid of custom numpy types
import json
json_spec = df_spec.set_index("full_name").to_json(orient = "index")
spec = json.loads(json_spec)

#__pos__:
#  action:
#    kind: elwise
#    status: supported
#  backends:
#    postgresql:
#      status: todo
#  category: _special_methods
#  example: +_


# kind, name, category, is_property, accessor, arity, (full_name)
# backend:
#   input_type, result_type, status, flags
# example 
