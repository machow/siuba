from siuba.spec.series import spec
from siuba.siu import CallTreeLocal, FunctionLookupError

from siuba.experimental.pd_groups.translate import SeriesGroupBy, GroupByAgg, GROUP_METHODS
from siuba.experimental.pd_groups.groupby import broadcast_agg, is_compatible


# TODO: make into CallTreeLocal factory function

out = {}
call_props = set()
for name, entry in spec.items():
    #if entry['result']['type']: continue
    kind = entry['action'].get('kind') or entry['action'].get('status')
    key = (kind.title(), entry['action']['data_arity'])

    # add properties like df.dtype, so we know they are method calls
    if entry['is_property'] and not entry['accessor']:
        call_props.add(name)


    meth = GROUP_METHODS[key](
            name = name.split('.')[-1],
            is_property = entry['is_property'],
            accessor = entry['accessor']
            )

    # TODO: returning this exception class from group methods is weird, but I 
    #       think also used in tests
    if meth is NotImplementedError:
        continue

    out[name] = meth

call_listener = CallTreeLocal(
        out,
        call_sub_attr = ('str', 'dt', 'cat', 'sparse'),
        chain_sub_attr = True,
        dispatch_cls = GroupByAgg,
        result_cls = SeriesGroupBy,
        call_props = call_props
        )


# Fast group by verbs =========================================================

from siuba.siu import Call
from siuba.dply.verbs import mutate, filter, summarize, singledispatch2, DataFrameGroupBy
from pandas.core.dtypes.inference import is_scalar
import warnings

def fallback_warning(expr, reason):
    warnings.warn(
            "The expression below cannot be executed quickly. "
            "Using the slow (but general) pandas apply method."
            "\n\nExpression: {}\nReason: {}"
                .format(expr, reason)
            )


def grouped_eval(__data, expr, require_agg = False):
    if is_scalar(expr):
        return expr
    
    if isinstance(expr, Call):
        try:
            call = call_listener.enter(expr)
        except FunctionLookupError as e:
            fallback_warning(expr, str(e))
            call = expr

        #
        grouped_res = call(__data)

        if isinstance(grouped_res, SeriesGroupBy):
            if not is_compatible(grouped_res, __data):
                raise ValueError("Incompatible groupers")

            # TODO: may want to validate result is correct length / index?
            #       e.g. a SeriesGroupBy could be compatible and not an agg
            if require_agg:
                return grouped_res.obj
            else:
                # broadcast from aggregate to original length (like transform)
                return broadcast_agg(grouped_res)

        else:
            # can happen right now if user selects, e.g., a property of the
            # groupby object, like .dtype, which returns a single value
            # in the future, could restrict set of operations user could perform
            raise ValueError("Result must be subclass of SeriesGroupBy")

    raise ValueError("Grouped expressions must be a siu expression or scalar")



# Fast mutate ----

def _transform_args(args):
    out = []
    for expr in args:
        if is_scalar(expr):
            out.append(expr)
        elif isinstance(expr, Call):
            try:
                call = call_listener.enter(expr)
                out.append(call)
            except FunctionLookupError as e:
                fallback_warning(expr, str(e))
                return None
        elif callable(expr):
            return None

    return out

def _copy_dispatch(dispatcher, cls, func = None):
    if func is None:
        return lambda f: _copy_dispatch(dispatcher, cls, f)

    # Note stripping symbolics may occur twice. Once in the original, and once
    # in this dispatcher.
    new_dispatch = singledispatch2(cls, func)
    new_dispatch.register(object, dispatcher)

    return new_dispatch


@_copy_dispatch(mutate, DataFrameGroupBy)
def fast_mutate(__data, **kwargs):
    """Warning: this function is experimental"""

    # transform call trees, potentially bail out to slow method --------
    new_vals = _transform_args(kwargs.values())

    if new_vals is None:
        return mutate(__data, **kwargs)


    # perform fast method ----
    out = __data.obj.copy()
    groupings = __data.grouper.groupings


    for name, expr in zip(kwargs, new_vals):
        res = grouped_eval(__data, expr)
        out[name] = res

    return out.groupby(groupings)


# Fast filter ----

@_copy_dispatch(filter, DataFrameGroupBy)
def fast_filter(__data, *args):
    """Warning: this function is experimental"""

    # transform call trees, potentially bail out to slow method --------
    new_vals = _transform_args(args)

    if new_vals is None:
        return filter(__data, *args)

    # perform fast method ----
    out = []
    groupings = __data.grouper.groupings

    for expr in args:
        res = grouped_eval(__data, expr)
        out.append(res)

    filter_df = filter.dispatch(__data.obj.__class__)

    df_result = filter_df(__data.obj, *out)

    # TODO: research how to efficiently & robustly subset groupings
    group_names = [ping.name for ping in groupings]
    return df_result.groupby(group_names)


# Fast summarize ----

@_copy_dispatch(summarize, DataFrameGroupBy)
def fast_summarize(__data, **kwargs):
    """Warning: this function is experimental"""

    # transform call trees, potentially bail out to slow method --------
    new_vals = _transform_args(kwargs.values())

    if new_vals is None:
        return summarize(__data, **kwargs)

    # perform fast method ----
    groupings = __data.grouper.groupings

    # TODO: better way of getting this frame?
    out = __data.grouper.result_index.to_frame()
    
    for name, expr in kwargs.items():
        # special case: set scalars directly
        res = grouped_eval(__data, expr, require_agg = True)

        out[name] = res

    return out.reset_index(drop = True)


