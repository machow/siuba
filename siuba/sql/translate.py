"""
This module holds default translations from pandas syntax to sql for 3 kinds of operations...

1. scalar - elementwise operations (e.g. array1 + array2)
2. aggregation - operations that result in a single number (e.g. array1.mean())
3. window - operations that do calculations across a window
            (e.g. array1.lag() or array1.expanding().mean())

It is broken into 5 sections:

* Warnings
* Column classes
* Custom over clauses
* Translation function generators (e.g. sql_agg("stdev"))
* SqlTranslator for translating symbolic expressions.

"""

from sqlalchemy import sql

from siuba.siu import FunctionLookupBound, FunctionLookupError


# warning for when sql defaults differ from pandas ============================
import warnings


class SqlFunctionLookupError(FunctionLookupError): pass


class SiubaSqlRuntimeWarning(UserWarning): pass

def warn_arg_default(func_name, arg_name, arg, correct):
    warnings.warn(
            "\n{func_name} sql translation defaults "
            "{arg_name} to {arg}. To return identical result as pandas, use "
            "{arg_name} = {correct}.\n\n"
            "This warning only displays once per function".format(
                func_name = func_name, arg_name = arg_name, arg = repr(arg), correct = repr(correct)
                ),
            SiubaSqlRuntimeWarning
            )


# Column data types ===========================================================

from sqlalchemy.sql.elements import ClauseElement

class SqlBase(ClauseElement): pass

class SqlColumn(SqlBase): pass

class SqlColumnAgg(SqlBase): pass


# Custom over clause handling  ================================================

from sqlalchemy.sql.elements import Over


class CustomOverClause(Over):
    """Base class for custom window clauses in SQL translation."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


    def set_over(self, group_by, order_by):
        raise NotImplementedError()

    def has_over(self):
        return self.order_by is not None or self.group_by is not None


    @classmethod
    def func(cls, name):
        raise NotImplementedError()


    


class AggOver(CustomOverClause):
    """Over clause for uses of functions min, max, avg, that return one value.

    Note that this class does not set order by, which is how these functions
    generally become their cumulative versions.

    E.g. mean(x) -> AVG(x) OVER (partition_by <group vars>)
    """

    def set_over(self, group_by, order_by = None):
        self.partition_by = group_by
        return self

    @classmethod
    def func(cls, name):
        sa_func = getattr(sql.func, name)
        def f(codata, col, *args, **kwargs) -> AggOver:
            return cls(sa_func(col, *args, **kwargs))

        return f


class RankOver(CustomOverClause): 
    """Over clause for ranking functions.

    Note that in python we might call rank(col), but in SQL the ranking column
    is defined using order by.

    E.g. rank(y) -> rank() OVER (partition by <group vars> order by y)
    """
    def set_over(self, group_by, order_by = None):
        crnt_partition = getattr(self.partition_by, 'clauses', tuple())
        self.partition_by = sql.elements.ClauseList(*crnt_partition, *group_by.clauses)
        return self

    @classmethod
    def func(cls, name):
        sa_func = getattr(sql.func, name)
        def f(codata, col) -> RankOver:
            return cls(sa_func(), order_by = col)

        return f


class CumlOver(CustomOverClause):
    """Over clause for cumulative versions of functions like sum, min, max.

    Note that this class is also currently used for aggregates that might require
    ordering, like nth, first, etc..

    e.g. cumsum(x) -> SUM(x) OVER (partition by <group vars> order by <order vars>)
    e.g. nth(0) -> NTH_VALUE(1) OVER (partition by <group vars> order by <order vars>)

    """
    def set_over(self, group_by, order_by):
        self.partition_by = group_by


        # do not override order by if it was set by the user. this might happen
        # in functions like nth, which gives the option to set it.
        if self.order_by is None or not len(self.order_by):
            if not len(order_by):
                warnings.warn(
                        "No order by columns explicitly set in window function. SQL engine"
                        "does not guarantee a row ordering. Recommend using an arrange beforehand.",
                        RuntimeWarning
                        )

            self.order_by = order_by


        return self

    @classmethod
    def func(cls, name, rows=(None, 0)):
        sa_func = getattr(sql.func, name)
        def f(codata, col, *args, **kwargs) -> CumlOver:
            return cls(sa_func(col, *args, **kwargs), rows = rows)

        return f

# convenience aliases for class methods above
# TODO MC-NOTE: funcs like AggOver.func should not have codata as the first argument, 
# since they are simple sqlalchemy subclasses. However, they're used as function
# factories in the dialects...
win_agg = AggOver.func
win_over = RankOver.func
win_cumul = CumlOver.func


# Simple clause translation generators ========================================

def sql_agg(name):
    sa_func = getattr(sql.func, name)
    return lambda codata, col: sa_func(col)

def sql_scalar(name):
    sa_func = getattr(sql.func, name)
    return lambda codata, col, *args: sa_func(col, *args)

def sql_colmeth(meth, *outerargs):
    def f(codata, col, *args) -> SqlColumn:
        return getattr(col, meth)(*outerargs, *args)
    return f

def sql_ordered_set(name, is_analytic=False):
    """Generate function for ordered and hypothetical set aggregates.

    Hypothetical-set aggregates take an argument, and return a value for each
    element of the argument. For example: rank(2) WITHIN GROUP (order by x).
    In this case, the hypothetical ranks 2 relative to x.

    Ordered set aggregates are like percentil_cont(.5) WITHIN GROUP (order by x),
    which calculates the median of x.
    """
    sa_func = getattr(sql.func, name)

    if is_analytic:
        return lambda codata, col, *args: AggOver(
            sa_func(*args).within_group(col)
        )

    return lambda codata, col, *args: sa_func(*args).within_group(col)

# Handling not implemented translations ----

def sql_not_impl(msg = ""):

    return FunctionLookupBound(msg or "function not implemented")

def win_absent(name):
    # Return an error, that is picked up by the translator.
    # this allows us to report errors at translation, rather than call time.
    return FunctionLookupBound("SQL dialect does not support window function {}.".format(name))

# Annotations ----
# these functions wrap translation generators, in order to provide metadata
# for running, e.g., unit tests. This is important in cases where the SQL 
# output does not match exactly what pandas does (for example, it returns a 
# float, when pandas returns an int).

def annotate(f = None, **kwargs):
    # allow it to work as a decorator
    if f is None:
        return lambda f: annotate(f, **kwargs)

    if hasattr(f, "operation"):
        raise ValueError("function already has an operation attribute")

    f.operation = kwargs

    return f

def wrap_annotate(f, **kwargs):
    from functools import wraps

    @wraps(f)
    def wrapper(*args, **kwargs):
        return f(*args, **kwargs)

    wrapper.operation = kwargs

    return wrapper


#  Translator =================================================================

from siuba.ops.translate import create_pandas_translator


def extend_base(cls, **kwargs):
    """Register concrete methods onto generic functions for pandas Series methods."""
    from siuba.ops import ALL_OPS
    for meth_name, f in kwargs.items():
        ALL_OPS[meth_name].register(cls, f)


# TODO: should inherit from a ITranslate class (w/ abstract translate method)
class SqlTranslator:
    """Translates symbolic column operations to sqlalchemy clauses.

    Note that SqlTranslator's main job is to hold two actual translators--
    one for windowed and one for aggregation contexts--and to call the
    translate method to the correct one.


    Example::

       from siuba.sql.translate import (
           SqlTranslator,
           SqlColumn, SqlColumnAgg,
           AggOver, sql_not_impl
           )

       base   = {"__add__": sql_colmeth("add")}
       window = {"mean":    AggOver.func("mean")}
       agg    = {"mean":    sql_not_impl("group by mean")}

       trans = SqlTranslator.from_mappings(
               base, window, agg,
               SqlColumn, SqlColumnAgg
               )

       from siuba import _
       trans.translate(_ + _.mean())
       trans.translate(_ + _.mean(), window = False)
   """

    def __init__(self, window, aggregate):
        self.window = window
        self.aggregate = aggregate

    def translate(self, expr, window = True):
        """Convert an AST of method chains to an AST of function calls."""

        if window:
            return self.window.translate(expr)

        return self.aggregate.translate(expr)


    def shape_call(
            self,
            call, window = True, str_accessors = False,
            verb_name = None, arg_name = None,
            ):
        """Return a siu Call that creates dialect specific SQL when called."""

        from siuba.siu import Call, MetaArg, strip_symbolic, Lazy, str_to_getitem_call
        from siuba.siu.visitors import CodataVisitor

        call = strip_symbolic(call)

        if isinstance(call, Call):
            pass
        elif str_accessors and isinstance(call, str):
            # verbs that can use strings as accessors, like group_by, or
            # arrange, need to convert those strings into a getitem call
            return str_to_getitem_call(call)
        elif isinstance(call, sql.elements.ColumnClause):
            return Lazy(call)
        elif callable(call):
            #TODO: should not happen here
            return Call("__call__", call, MetaArg('_'))

        else:
            # verbs that use literal strings, need to convert them to a call
            # that returns a sqlalchemy "literal" object
            return Lazy(sql.literal(call))

        # raise informative error message if missing translation
        try:
            # TODO: MC-NOTE -- scaffolding in to verify prior behavior works
            shaped_call = self.translate(call, window = window)
            if window:
                trans = self.window
            else:
                trans = self.aggregate

            # TODO: MC-NOTE - once all sql singledispatch funcs are annotated
            # with return types, then switch object back out
            # alternatively, could register a bounding class, and remove
            # the result type check
            v = CodataVisitor(trans.dispatch_cls, object)
            return v.visit(shaped_call)
            
        except FunctionLookupError as err:
            raise SqlFunctionLookupError.from_verb(
                    verb_name or "Unknown",
                    arg_name or "Unknown",
                    err,
                    short = True
                    )


    def from_mappings(WinCls, AggCls):
        from siuba.ops import ALL_OPS

        return SqlTranslator(
                window = create_pandas_translator(ALL_OPS, WinCls, sql.elements.ClauseElement),
                aggregate = create_pandas_translator(ALL_OPS, AggCls, sql.elements.ClauseElement)
                )

