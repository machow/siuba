from siuba.siu import CallListener, strip_symbolic
from siuba.dply.verbs import singledispatch2, mutate
from pandas.core.groupby import DataFrameGroupBy
import pandas as pd
    

class AttrStripper(CallListener):
    """
    Returns a Call with attribute access for specified attribute names removed.

    Example:
        from siuba import _, strip_symbolic
        attr_strip = AttrStripper({'hp'})

        # returns Call: _ + _.rank()
        attr_strip.enter(strip_symbolic(_.hp + _.hp.rank()))
    """
            
    def __init__(self, rm_attr):
        self.rm_attr = rm_attr
        
    def exit___getattr__(self, node):
        obj, attr_name = node.args
        if attr_name in self.rm_attr:
            return obj
        
        return node

    @classmethod
    def run_on_node(cls, node, *args, **kwargs):
        inst = cls(*args, **kwargs)
        return inst.enter(node)


def _grouped_transform_or_apply(data, call):
    # get variable names used, ignoring called methods
    var_names = call.op_vars(attr_calls = False)
    if len(var_names) == 1:
        # when only a single column in expression, use more efficient transform
        col_name = list(var_names)[0]
        new_call = AttrStripper.run_on_node(call, var_names)

        return data[col_name].transform(new_call)
    else:
        # TODO: analyze n groups: if 1 value per group, is agg, if has all needed rows is mutate
        # otherwise, use generic apply, and handle two issues
        # issue 1: it can return a DataFrame
        res = data.apply(call)
        if isinstance(res, pd.DataFrame):
            raise ValueError()
        
        # issue 2: its index is group info + orig index, 
        group_names = [x.name for x in data.grouper.groupings]
        #group_names = res.index.names[:-1]
        gdf_index = data.obj.set_index(group_names)
        # put rows in original order
        return res[gdf_index.index].values
    

@singledispatch2(object)
def mutate2(__data, **kwargs):
    """Warning: this function is experimental"""
    # by default dispatch to regular mutate
    f_mutate = mutate.registry[type(__data)]
    return f_mutate(__data, **kwargs)


@mutate2.register(DataFrameGroupBy)
def _mutate2_group_by(__data, **kwargs):
    new_df = __data.obj.copy()
    crnt_df = __data
    for k, call in kwargs.items():
        new_df[k] = _grouped_transform_or_apply(crnt_df, call)
        crnt_df = new_df.groupby(__data.grouper.groupings)
    return crnt_df


# Tests =======================================================================

# op returns len of series
# op returns series with single value
# op returns scalar

