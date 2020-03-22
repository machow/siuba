from siuba.spec.series import spec
from siuba.siu import CallTreeLocal

from siuba.experimental.pd_groups.translate import SeriesGroupBy, GroupByAgg, GROUP_METHODS


# TODO: make into CallTreeLocal factory function

out = {}
for name, entry in spec.items():
    #if entry['result']['type']: continue
    kind = entry['action'].get('kind') or entry['action'].get('status')
    key = (kind.title(), entry['action']['data_arity'])
    meth = GROUP_METHODS[key]
    out[name] = meth(
            name = name.split('.')[-1],
            is_property = entry['is_property'],
            accessor = entry['accessor']
            )

call_listener = CallTreeLocal(
        out,
        call_sub_attr = ('str', 'dt', 'cat', 'sparse'),
        chain_sub_attr = True,
        dispatch_cls = GroupByAgg,
        result_cls = SeriesGroupBy
        )


# Fast group by verbs =========================================================

from siuba.siu import Call
from siuba.dply.verbs import mutate, filter, summarize, singledispatch2, DataFrameGroupBy, _regroup
from pandas.core.dtypes.inference import is_scalar

def grouped_eval(__data, expr, require_agg = False):
    if is_scalar(expr):
        return expr
    
    if isinstance(expr, Call):
        call = call_listener.enter(expr)

        #
        grouped_res = call(__data)

        if isinstance(grouped_res, GroupByAgg):
            # TODO: may want to validate its grouper
            if require_agg:
                # need an agg, got an agg. we are done.
                if not grouped_res._orig_grouper is __data.grouper:
                    raise ValueError("Incompatible groupers")
                return grouped_res
            else:
                # broadcast from aggregate to original length (like transform)
                return grouped_res._broadcast_agg_result()
        elif isinstance(grouped_res, SeriesGroupBy) and not require_agg:
            # TODO: may want to validate its grouper
            return grouped_res.obj
        else:
            # can happen right now if user selects, e.g., a property of the
            # groupby object, like .dtype, which returns a single value
            # in the future, could restrict set of operations user could perform
            raise ValueError("Result must be subclass of SeriesGroupBy")

    raise ValueError("Grouped expressions must be a siu expression or scalar")



# Fast mutate ----

@singledispatch2(DataFrameGroupBy)
def fast_mutate(__data, **kwargs):
    """Warning: this function is experimental"""
    out = __data.obj.copy()
    groupings = __data.grouper.groupings

    for name, expr in kwargs.items():
        res = grouped_eval(__data, expr)
        out[name] = res

    return out.groupby(groupings)


@fast_mutate.register(object)
def _fast_mutate_default(__data, **kwargs):
    # TODO: had to register object second, since singledispatch2 sets object dispatch
    #       to be a pipe (e.g. unknown types become a pipe by default)
    # by default dispatch to regular mutate
    f = mutate.registry[type(__data)]
    return f(__data, **kwargs)


# Fast filter ----

@singledispatch2(DataFrameGroupBy)
def fast_filter(__data, *args):
    """Warning: this function is experimental"""
    import pandas as pd
    out = []
    groupings = __data.grouper.groupings

    for expr in args:
        res = grouped_eval(__data, expr)
        out.append(res)

    filter_df = filter.registry[pd.DataFrame]

    df_result = filter_df(__data.obj, *out)

    # TODO: research how to efficiently & robustly subset groupings
    group_names = [ping.name for ping in groupings]
    return df_result.groupby(group_names)


@fast_filter.register(object)
def _fast_filter_default(__data, *args, **kwargs):
    # TODO: had to register object second, since singledispatch2 sets object dispatch
    #       to be a pipe (e.g. unknown types become a pipe by default)
    # by default dispatch to regular mutate
    f = filter.registry[type(__data)]
    return f(__data, *args, **kwargs)


# Fast summarize ----

@singledispatch2(DataFrameGroupBy)
def fast_summarize(__data, **kwargs):
    """Warning: this function is experimental"""
    groupings = __data.grouper.groupings

    # TODO: better way of getting this frame?
    out = __data.grouper.result_index.to_frame()
    
    for name, expr in kwargs.items():
        # special case: set scalars directly
        res = grouped_eval(__data, expr, require_agg = True)

        if isinstance(res, GroupByAgg):
            # TODO: would be faster to check that res has matching grouper, since
            #       here it goes through the work of matching up indexes (which if
            #       the groupers match are identical)
            out[name] = res.obj

        # otherwise, assign like a scalar
        else:
            out[name] = res

    return out.reset_index(drop = True)


@fast_summarize.register(object)
def _fast_summarize_default(__data, **kwargs):
    # TODO: had to register object second, since singledispatch2 sets object dispatch
    #       to be a pipe (e.g. unknown types become a pipe by default)
    # by default dispatch to regular mutate
    f = summarize.registry[type(__data)]
    return f(__data, **kwargs)

