from siuba.siu import CallTreeLocal, FunctionLookupError
from .groupby import SeriesGroupBy

from .translate import (
        forward_method,
        not_implemented,
        method_agg_op,
        method_win_op_agg_result
        )


# THE REAL DIALECT FILE LET'S DO THIS
# ====================================

from siuba.ops import ALL_OPS
from siuba import ops

# register concrete implementations for all ops -------------------------------
# note that this may include versions that would error (e.g. tries to look
# up a Series method that doesn't exist). Custom implementations to fix
# are registered over these further down
for  dispatcher in ALL_OPS.values():#vars(ops).values():
    #try:
    forward_method(dispatcher)
    #except KeyError:
    #    pass


# custom implementations ------------------------------------------------------

def register_method(ns, op_name, f, is_property = False, accessor = None):
    generic = ns[op_name]
    return generic.register(SeriesGroupBy, f(op_name, is_property, accessor))

# add to new op spec
# create new call tree

# aggregate ----

NOT_IMPLEMENTED_AGG = [
        "bool", "dot", "empty", "equals", "hasnans", "is_unique", "kurt",
        "kurtosis", "memory_usage", "nbytes", "product"
        ]

for f_name in NOT_IMPLEMENTED_AGG:
    ALL_OPS[f_name].register(SeriesGroupBy, not_implemented(f_name))

# size is a property on ungrouped, but not grouped pandas data.
# since siuba follows the ungrouped API, it's used as _.x.size, and
# just needs its implementation registered as a *non*-property.
ops.size.register(SeriesGroupBy, method_agg_op("size", is_property = False, accessor = None))


# window ----
NOT_IMPLEMENTED_WIN = [
        "asof", "at", "autocorr", "cat.remove_unused_categories", 
        "convert_dtypes", "drop_duplicates", "duplicated", "get",
        "iat", "iloc", "infer_objects", "is_monotonic",
        ]

for f_name in NOT_IMPLEMENTED_WIN:
    ALL_OPS[f_name].register(SeriesGroupBy, not_implemented(f_name))


# a few functions apply window operations, but return an agg-like result
forward_method(ops.is_monotonic_decreasing, method_win_op_agg_result)
forward_method(ops.is_monotonic_increasing, method_win_op_agg_result)


# NOTE TODO: these methods could be implemented, but depend on the type of 
# time index they're operating on
NOT_IMPLEMENTED_DT = [
    "dt.qyear", "dt.to_pytimedelta", "dt.to_timestamp", "dt.total_seconds", "dt.tz_convert",
    "to_period","dt.to_pydatetime"
    ]

for f_name in NOT_IMPLEMENTED_DT:
    ALL_OPS[f_name].register(SeriesGroupBy, not_implemented(f_name))





# ====================================

from .translate import GroupByAgg, SeriesGroupBy

# TODO: use pandas call tree creator
from siuba.ops.generics import ALL_PROPERTIES, ALL_ACCESSORS

call_listener = CallTreeLocal(
        ALL_OPS,
        call_sub_attr = ALL_ACCESSORS,
        chain_sub_attr = True,
        dispatch_cls = GroupByAgg,
        result_cls = SeriesGroupBy,
        call_props = ALL_PROPERTIES
        )


# Fast group by verbs =========================================================

from siuba.siu import Call
from siuba.dply.verbs import mutate, filter, summarize, singledispatch2, DataFrameGroupBy, _regroup
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

@singledispatch2(DataFrameGroupBy)
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

    filter_df = filter.registry[__data.obj.__class__]

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

