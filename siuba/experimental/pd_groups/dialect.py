from siuba.siu import CallTreeLocal, FunctionLookupError, ExecutionValidatorVisitor
from .groupby import SeriesGroupBy

from .translate import (
        forward_method,
        not_implemented,
        method_agg_op,
        method_win_op_agg_result
        )

from siuba.experimental.pd_groups.groupby import SeriesGroupBy, GroupByAgg, broadcast_agg, is_compatible


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

call_validator = ExecutionValidatorVisitor(GroupByAgg, SeriesGroupBy)


# Fast group by verbs =========================================================

from siuba.siu import Call, singledispatch2
from siuba.dply.verbs import mutate, filter, summarize, DataFrameGroupBy
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
            call_validator.visit(call)

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
                call_validator.visit(call)
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


