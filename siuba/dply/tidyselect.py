import pandas as pd

from siuba.siu import Call,   MetaArg, BinaryOp
from collections import OrderedDict
from itertools import chain
from functools import singledispatch

from typing import List

class Var:
    def __init__(self, name: "str | int | slice | Call", negated = False, alias = None):
        if not isinstance(name, (str, int, slice, Call)):
            raise TypeError(f"Var name cannot be type: {type(name)}.")
        self.name = name
        self.negated = negated
        self.alias = alias

    def __neg__(self):
        return self.to_copy(negated = not self.negated)


    def __invert__(self):
        return self.to_copy(negated = not self.negated)

    def __eq__(self, x):
        name = x.name if isinstance(x, Var) else x
        return self.to_copy(name = name, negated = False, alias = self.name)

    def __call__(self, *args, **kwargs):
        call = Call('__call__',
                    BinaryOp('__getattr__', MetaArg("_"), self.name),
                    *args,
                    **kwargs
                    )

        return self.to_copy(name = call)


    def __repr__(self):
        cls_name = self.__class__.__name__
        sig = f"{repr(self.name)}, negated={self.negated}, alias={self.alias}"
        return f"{cls_name}({sig})"

    def __str__(self):
        op = "-" if self.negated else ""
        pref = self.alias + " = " if self.alias else ""
        return "{pref}{op}{self.name}".format(pref = pref, op = op, self = self)

    def to_copy(self, **kwargs):
        return self.__class__(**{**self.__dict__, **kwargs})


class VarAnd(Var):
    name: "tuple[Var]"

    def __init__(self, name: "tuple[Var]", negated=False, alias=None):
        self.name = name
        self.negated = negated

        bad_var = [x for x in name if not isinstance(x, Var)]

        if any(bad_var):
            raise TypeError(f"VarAnd expects a tuple of Var, but saw entries: {bad_var}")

        if alias is not None:
            raise TypeError("alias must be none for VarAnd (extended slice syntax)")

        self.alias = None

    def __eq__(self, x):
        raise NotImplementedError()

    def __call__(self, *args, **kwargs):
        raise NotImplementedError()

    def flatten(self) -> "tuple[Var]":
        res = []
        for var in self.name:
            neg_var = ~var if self.negated else var
            if isinstance(neg_var, VarAnd):
                res.extend(neg_var.flatten())
            else:
                res.append(neg_var)

        return tuple(res)



class VarList:
    def __getattr__(self, x):
        return Var(x)

    def __getitem__(self, x):
        if not isinstance(x, tuple):
            return Var(x) if not isinstance(x, Var) else x
        else:
            res = [el if isinstance(el, Var) else Var(el) for el in x]
            return VarAnd(tuple(res))


def var_slice(colnames, x):
    """Return indices in colnames correspnding to start and stop of slice."""
    # TODO: produces bahavior similar to df.loc[:, "V1":"V3"], but can reverse
    # TODO: make DRY
    # TODO: reverse not including end points
    if isinstance(x.start, Var):
        start_indx = (colnames == x.start.name).idxmax()
    elif isinstance(x.start, str):
        start_indx = (colnames == x.start).idxmax()
    else:
        start_indx = x.start or 0

    if isinstance(x.stop, Var):
        stop_indx = (colnames == x.stop.name).idxmax() + 1
    elif isinstance(x.stop, str):
        stop_indx = (colnames == x.stop).idxmax() + 1
    else:
        stop_indx = x.stop or len(colnames)

    if start_indx > stop_indx:
        return stop_indx, start_indx
    else:
        return start_indx, stop_indx

def var_put_cols(name, var, cols):
    if isinstance(name, list) and var.alias is not None:
        raise Exception("Cannot assign name to multiple columns")
    
    names = [name] if not isinstance(name, list) else name

    for name in names:
        if var.negated:
            if name in cols: cols.pop(name)
        #elif name in cols: cols.move_to_end(name)
        else: cols[name] = var.alias

def flatten_var(var):
    if isinstance(var, VarAnd):
        return var.flatten()
    return [var]


def var_select(colnames, *args, data=None):
    # TODO: don't erase named column if included again
    colnames = colnames if isinstance(colnames, pd.Series) else pd.Series(colnames)
    cols = OrderedDict()

    #flat_args = var_flatten(args)
    all_vars = chain(*map(flatten_var, args))

    # Add entries in pandas.rename style {"orig_name": "new_name"}
    for ii, arg in enumerate(all_vars):

        # strings are added directly
        if isinstance(arg, str):
            cols[arg] = None

        # integers add colname at corresponding index
        elif isinstance(arg, int):
            cols[colnames.iloc[arg]] = None

        # general var handling
        elif isinstance(arg, Var):
            # remove negated Vars, otherwise include them
            if ii == 0 and arg.negated:
                # if negation used as first arg apply an implicit everything
                cols.update((k, None) for k in colnames)

            # slicing can refer to single, or range of columns
            if isinstance(arg.name, slice):
                start, stop = var_slice(colnames, arg.name)
                for ii in range(start, stop):
                    var_put_cols(colnames[ii], arg, cols)

            # method calls like endswith()
            elif callable(arg.name):
                # TODO: not sure if this is a good idea...
                #       basically proxies to pandas str methods (they must return bool array)
                indx = arg.name(colnames.str)
                var_put_cols(colnames[indx].tolist(), arg, cols)
                #cols.update((x, None) for x in set(colnames[indx]) - set(cols))
            elif isinstance(arg.name, int):
                var_put_cols(colnames.iloc[arg.name], arg, cols)
            else:
                var_put_cols(arg.name, arg, cols)
        elif callable(arg) and data is not None:
            # TODO: call on the data
            col_mask = colwise_eval(data, arg)

            for name in colnames[col_mask]:
                cols[name] = None


        else:
            raise Exception("variable must be either a string or Var instance")

    return cols


def var_create(*args) -> "tuple[Var]":
    vl = VarList()
    all_vars = []
    for arg in args:
        if isinstance(arg, Call):
            res = arg(vl)
            if isinstance(res, VarList):
                raise ValueError("Must select specific column. Did you pass `_` to select?")
            all_vars.append(res)
        elif isinstance(arg, Var):
            all_vars.append(arg)
        elif callable(arg):
            all_vars.append(arg)
        else:
            all_vars.append(Var(arg))
     
    return tuple(all_vars)


@singledispatch
def colwise_eval(data, predicate):
    raise NotImplementedError(
        f"Cannot evaluate tidyselect predicate on data type: {type(data)}"
    )


@colwise_eval.register
def _colwise_eval_pd(data: pd.DataFrame, predicate) -> List[bool]:
    mask = []
    for col_name in data:
        res = predicate(data.loc[:, col_name])
        if not pd.api.types.is_bool(res):
            raise TypeError("TODO")

        mask.append(res)

    return mask

    
    
