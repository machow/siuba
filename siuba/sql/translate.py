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

from siuba.siu import FunctionLookupBound

# warning for when sql defaults differ from pandas ============================
import warnings


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

from sqlalchemy.sql.elements import ColumnClause

class SqlBase(ColumnClause): pass

class SqlColumn(SqlBase): pass

class SqlColumnAgg(SqlBase): pass


# Custom over clause handling  ================================================

from sqlalchemy.sql.elements import Over


class CustomOverClause(Over):
    """Base class for custom window clauses in SQL translation."""

    def set_over(self, group_by, order_by):
        raise NotImplementedError()

    @classmethod
    def func(cls, name):
        raise NotImplementedError()


class AggOver(CustomOverClause):
    """Over clause for uses of functions min, max, avg, that return one value.

    Note that this class does not set order by, which is how these functions
    generally become their cumulative versions.
    """

    def set_over(self, group_by, order_by = None):
        self.partition_by = group_by
        return self

    @classmethod
    def func(cls, name):
        sa_func = getattr(sql.func, name)
        def f(col, *args, **kwargs) -> AggOver:
            return cls(sa_func(col, *args, **kwargs))

        return f


class RankOver(CustomOverClause): 
    """Over clause for ranking functions.

    Note that in python we might call rank(col), but in SQL the ranking column
    is defined using order by.
    """
    def set_over(self, group_by, order_by = None):
        crnt_partition = getattr(self.partition_by, 'clauses', tuple())
        self.partition_by = sql.elements.ClauseList(*crnt_partition, *group_by.clauses)
        return self

    @classmethod
    def func(cls, name):
        sa_func = getattr(sql.func, name)
        def f(col) -> RankOver:
            return cls(sa_func(), order_by = col)

        return f


class CumlOver(CustomOverClause):
    """Over clause for cumulative versions of functions like sum, min, max.

    Note that this class is also currently used for aggregates that might require
    ordering, like nth, first, etc..

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
        def f(col, *args, **kwargs) -> CumlOver:
            return cls(sa_func(col, *args, **kwargs), rows = rows)

        return f

# convenience aliases for class methods above
win_agg = AggOver.func
win_over = RankOver.func
win_cumul = CumlOver.func


# Simple clause translation generators ========================================

def sql_agg(name):
    sa_func = getattr(sql.func, name)
    return lambda col: sa_func(col)

def sql_scalar(name):
    sa_func = getattr(sql.func, name)
    return lambda col, *args: sa_func(col, *args)

def sql_colmeth(meth, *outerargs):
    def f(col, *args) -> SqlColumn:
        return getattr(col, meth)(*outerargs, *args)
    return f

def set_agg(name):
    # Ordered and theoretical set aggregates
    sa_func = getattr(sql.func, name)
    return lambda col, *args: sa_func(*args).within_group(col)

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

def extend_base(mapping, **kwargs):
    return {**mapping, **kwargs}

from siuba.ops.translate import create_pandas_translator

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
        if window:
            return self.window.translate(expr)

        return self.aggregate.translate(expr)

    def from_mappings(base, window, aggregate, WinCls, AggCls):
        trans_win = {**base, **window}
        trans_agg = {**base, **aggregate}

        return SqlTranslator(
                window = create_pandas_translator(trans_win, WinCls, sql.elements.ClauseElement),
                aggregate = create_pandas_translator(trans_agg, AggCls, sql.elements.ClauseElement)
                )

