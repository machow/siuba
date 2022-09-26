import pandas as pd
import numpy as np

from pandas.core.groupby import DataFrameGroupBy
from siuba.dply.tidyselect import var_create, var_select
from siuba.dply.verbs import gather, separate, extract, expand, complete
from siuba.siu import singledispatch2, Call
from siuba.dply.forcats import fct_inorder

from .utils import vec_as_names, reconstruct_tibble

from typing import Any


def _select_expr_slice(x: "tuple[str]") -> Call:
    from operator import getitem
    from siuba.siu import strip_symbolic, Symbolic

    return strip_symbolic(
        getitem(Symbolic(), x)
    )

def _tidy_select(__data, cols, arg_name):
    if cols is None:
        return {}

    var_list = var_create(cols)
    od = var_select(__data.columns, *var_list)

    # TODO: where in tidyselect package does this check happen?
    missing = set(od) - set(__data.columns)

    if missing:
        raise ValueError(
            f"{arg_name} must select columns present in the data. "
            f"Could not find these columns: {missing}"
        )

    return od

def _maybe_list(seq):
    if seq is None:
        return None

    return list(seq)


def _collapse_index_names(index, sep : "str | None" = None, glue = None):
    if glue is not None:
        if index.nlevels == 1:
            tmp_index = [[x, ""] for x in index]
        else:
            tmp_index = index

        return [glue.format(variable=entry[0], value=entry[1]) for entry in tmp_index]
    elif sep is not None:
        if index.nlevels == 1:
            return list(index)
        return [sep.join(map(str, entry)) for entry in index]
    else:
        raise NotImplementedError()


def _unique_col_name(df):
    base = "__TMP_COL__"
    test = base
    ii = 0

    df_cols = set(df.columns)
    while test in df.columns:
        test = base + str(ii)
        ii += 1

    return test


def _is_select_everything(expr):
    # this is crazy but probably fine for now.
    if (isinstance(expr, Call) and expr.func == "__getitem__"):
        sub_expr = expr.args[1]
        return (
            isinstance(sub_expr, Call)
            and sub_expr.func == "__siu_slice__"
            and len(sub_expr.args) == 1 and
            sub_expr.args[0] == slice(None, None, None)
        )
    return False


def _names_from_spec(spec, multi_index):
    # dict mapping name parts to the final names
    d_spec = dict(zip(spec.iloc[:, 1:].itertuples(index=False, name=None), spec[".name"]))

    return [d_spec[x] for x in multi_index]


@singledispatch2((pd.DataFrame, DataFrameGroupBy))
def pivot_wider(
    __data,
    id_cols=None,
    id_expand=False,
    names_from="name",
    names_prefix="",
    names_sep="_",
    names_glue=None,
    names_sort=None,
    names_vary="fastest",
    names_expand=False,
    names_repair="check_unique",
    values_from="value",
    values_fill=None,
    values_fn=None,
    unused_fn=None
    ):
    """Pivot data from long to wide format.

    This function splits a column, putting the pieces side-by-side based on an index.

    Parameters
    ----------
    __data:
        The input data.
    id_cols:
        A selection of columns that uniquely identify each observation.
    id_expand:
        Whether to ensure each unique combination of id_cols is a row in the data
        before pivoting, using `expand()`. This results in more rows. When True,
        this also sorts the final result by the `id_cols`.
    names_from, values_from:
        A pair fo arguments describing which column (or columns) to get the name of
        the output column (names_from), and which column (or columns) to get the
        cell values from (values_from).
    names_prefix:
        String added to the start of every variable name.
    names_sep:
        If names_from or values_from contains multiple values, this will be used
        to join their values together into a single string to use as a column name.
    names_glue:
        Instead of names_sep and names_prefix, supply a string template that uses
        the names_from columns (and a special .value variable) to create custom
        column names.
    names_sort:
        Should the column names be sorted? The default is False, which results
        in column names ordered by first appearance.
    names_vary:
        Option specifying how columns are ordered when names_from and values_from
        both identify new columns. "fastest" varies names_from fastest, while "slowest"
        varies names_from slowest.
    names_expand:
        Whether to ensure all combinations of names_from columns are in the result
        using the `expand()` function. This results in more columns in the output.
    names_repair:
        Strategy for fixing of invalid column names. "minimal" leaves them as is.
        "check_unique" raises an error if there are duplicate names. "unique"
        de-duplicates names by appending "___{position}" to them.
    values_fill:
        A scalar value used to fill in any missing values. Alternatively, a
        dictionary mapping column names to fill values.
    values_fn:
        An optional function to apply to each cell of the output. This is useful
        when each cell would contain multiple values. E.g. values_fn="max" would
        calculate the max value.
    unused_fn:
        Not implemented.

    Examples
    --------
    >>> from siuba import _

    >>> df = pd.DataFrame(
    ...    {"id": ["a", "b", "a"], "name": ["x", "x", "y"], "value": [1, 2, 3]}
    ... )
    >>> df
      id name  value
    0  a    x      1
    1  b    x      2
    2  a    y      3

    >>> pivot_wider(df, names_from=_.name, values_from=_.value)
      id    x    y
    0  a  1.0  3.0
    1  b  2.0  NaN

    >>> pivot_wider(df, names_from=_.name, values_from=_.value, values_fill=0)
      id  x  y
    0  a  1  3
    1  b  2  0

    >>> many = pd.DataFrame({
    ...    "id": [1, 1, 2, 2],
    ...    "var": ["one", "two", "one", "two"],
    ...    "x": [1, 2, 3, 4],
    ...    "y": [6, 7, 8, 9]
    ... })
    >>> pivot_wider(many, names_from=_.var, values_from=_[_.x, _.y])
       id  x_one  x_two  y_one  y_two
    0   1      1      2      6      7
    1   2      3      4      8      9

    >>> pivot_wider(many, names_from=_.var, values_from=_[_.x, _.y], names_vary="slowest")
       id  x_one  y_one  x_two  y_two
    0   1      1      6      2      7
    1   2      3      8      4      9

    >>> pivot_wider(many, names_from=_.var, values_from=_[_.x, _.y], names_sep=".")
       id  x.one  x.two  y.one  y.two
    0   1      1      2      6      7
    1   2      3      4      8      9

    >>> glue = "{variable}_X_{value}"
    >>> pivot_wider(many, names_from=_.var, values_from=_[_.x, _.y], names_glue=glue)
       id  x_X_one  x_X_two  y_X_one  y_X_two
    0   1        1        2        6        7
    1   2        3        4        8        9

    >>> from siuba.data import warpbreaks
    >>> warpbreaks.head()
       breaks wool tension
    0      26    A       L
    1      30    A       L
    2      54    A       L
    3      25    A       L
    4      70    A       L

    >>> pivot_wider(warpbreaks, names_from=_.wool, values_from=_.breaks, values_fn="mean")
      tension          A          B
    0       H  24.555556  18.777778
    1       L  44.555556  28.222222
    2       M  24.000000  28.777778

    """
    input_ = __data

    if isinstance(__data, DataFrameGroupBy):
        __data = __data.obj

    # create spec ----
    spec = build_wider_spec(
        __data,
        names_from = names_from,
        values_from = values_from,
        names_prefix = names_prefix,
        names_sep = names_sep,
        names_glue = names_glue,
        names_sort = names_sort,
        names_vary = names_vary,
        names_expand = names_expand
    )

    # select id columns ---
    # necessary here, since if the spec is 0 rows you cannot know values_from
    # TODO: clean up symbolic handling of slices
    if id_cols is None:
        name_vars = _tidy_select(__data, names_from, "names_from")
        val_vars = _tidy_select(__data, values_from, "values_from")
        others = {*name_vars, *val_vars}

        id_cols = tuple([col for col in __data.columns if col not in others])
        id_vars = _select_expr_slice(id_cols)
    else:
        id_vars = id_cols

    out = pivot_wider_spec(
        input_,
        spec,
        names_repair = names_repair,
        id_cols = id_vars,
        id_expand = id_expand,
        values_fill = values_fill,
        values_fn = values_fn,
        unused_fn = unused_fn
    )

    return out

@singledispatch2((pd.DataFrame, DataFrameGroupBy))
def pivot_wider_spec(
    __data,
    spec,
    names_repair = "check_unique",
    id_cols = None,
    id_expand = False,
    values_fill = None,
    values_fn = None,
    unused_fn = None
):

    input_ = __data

    # guards ----

    if isinstance(__data, DataFrameGroupBy):
        __data = __data.obj

    if _is_select_everything(id_cols):
        # restores id_cols to the default, which uses all remaining cols
        id_cols = None

    if unused_fn is not None:
        raise NotImplementedError()

    if not isinstance(id_expand, bool):
        raise TypeError("`id_expand` argument must be True or False.")

    # handle tidyselection ----------------------------------------------------

    name_vars = spec.columns[~spec.columns.isin([".name", ".value"])].tolist()
    val_vars = spec.loc[:, ".value"].unique().tolist()

    # select id columns
    if id_cols is None:
        others = {*name_vars, *val_vars}
        id_vars = [col for col in __data.columns if col not in others]
    else:
        id_vars = _tidy_select(__data, id_cols, "id_cols")

    id_var_bad = set(id_vars) & set([*name_vars, *val_vars])
    if id_var_bad:
        raise ValueError(
            "id_cols contains columns that are in "
            f"names_from or values_from: {id_var_bad}."
        )

    # use a categoricals for name columns, to ensure their order in wide format
    # is first-observed order (pandas by default uses alphabetical)
    tmp = __data.copy()
    for name in name_vars:
        tmp[name] = fct_inorder(tmp[name])

    # pivot to wide -----------------------------------------------------------

    if values_fn is None:
        # this block is essentially pd.pivot (which also uses unstack), but also
        # supports filing NAs, and resets indexes
        if not len(id_vars):
            # without id_vars we try to pivot to a frame with 1 row
            n_id_vars = 1
            tmp = tmp.set_index([np.zeros(len(tmp.index)), *name_vars])
        else:
            n_id_vars = len(id_vars)
            tmp = tmp.set_index([*id_vars, *name_vars])

        to_unstack = list(range(n_id_vars, n_id_vars + len(name_vars)))
        wide = (tmp
            .loc[:, list(val_vars)]
            .unstack(to_unstack, fill_value=values_fill)
        )

    else:
        # pivot_table requires a values_fn, so we only use it when one is provided.
        if not len(id_vars):
            # pivot_table without an index var is equivalent to the index being constant.
            # normally the index vars are the index of the pivot_table result, but without
            # an index column explicitly named, the value vars become the rows.
            # so we need to create an explicit index column...
            index_cols = [_unique_col_name(tmp)]
            tmp.loc[:, index_cols[0]] = np.zeros(len(tmp.index))

        else:
            index_cols = list(id_vars)

        # this ensures a single value column won't be used when constructing names
        # since a list creates a new index dimension
        _values = list(val_vars) if len(val_vars) > 1 else list(val_vars)[0]

        wide = pd.pivot_table(
            tmp,
            index=index_cols,
            columns=list(name_vars),
            values=list(val_vars),
            fill_value=values_fill,
            aggfunc=values_fn,
        )

        if wide.index.names != index_cols:
            raise ValueError(
                "pivot_wider produced a result with incorrect index variables. "
                "There is a bug in pandas when attempting to aggregate by a values_fn "
                "that is not an aggregate.\n\n"
                f"Do all the values_fn arguments return single values?: {values_fn}"
            )


    # flatten / reset indexes -------------------------------------------------

    # flatten column index ----
    if isinstance(wide, pd.Series):
        # the .unstack approach returns a Series when there are no id cols.
        # in this case we make it a frame and don't sort the two columns.
        wide = wide.reset_index()

    collapsed_names = _names_from_spec(spec, wide.columns)
    wide.columns = collapsed_names

    # add missing columns and reorder to spec ----
    missing_cols = list(spec[".name"][~spec[".name"].isin(wide.columns)])

    if missing_cols:
        wide[missing_cols] = values_fill

    wide = wide.loc[:, list(spec[".name"])]

    # validate names and move id vars to columns ----
    # note: in pandas 1.5+ we can use the allow_duplicates option to reset, even
    # when index and column names overlap. for now, repair names, rename, then reset.
    unique_names = vec_as_names([*id_vars, *wide.columns], repair="unique")
    repaired_names = vec_as_names([*id_vars, *wide.columns], repair=names_repair)

    uniq_id_vars = unique_names[:len(id_vars)]
    uniq_val_vars = unique_names[len(id_vars):]

    final_id_vars = repaired_names[:len(id_vars)]
    final_val_vars = repaired_names[len(id_vars):]

    wide.columns = uniq_val_vars

    if id_vars:
        wide.rename_axis(uniq_id_vars, inplace=True)
        wide.reset_index(drop=False, inplace=True)
    else:
        wide.reset_index(drop=True, inplace=True)

    wide.columns = repaired_names

    # expand id levels --------------------------------------------------------

    if id_expand:
        if values_fill is not None and not isinstance(values_fill, dict):
            values_fill = {k: values_fill for k in final_val_vars}

        wide = complete(wide, *final_id_vars, fill=values_fill, explicit=False)
        
    # reconstruct with groupings
    return reconstruct_tibble(input_, wide)


@singledispatch2((pd.DataFrame, DataFrameGroupBy))
def build_wider_spec(
    __data,
    names_from = "name",
    values_from = "value",
    names_prefix = "_",
    names_sep = "_",
    names_glue = None,
    names_sort = False,
    names_vary = "fastest",
    names_expand = False
):
    if isinstance(__data, DataFrameGroupBy):
        __data = __data.obj

    # guards ----
    if names_vary not in {"fastest", "slowest"}:
        raise ValueError(
            "names_vary must be one of 'fastest', 'slowest', but received "
            f"argument: {repr(names_vary)}"
        )

    if names_sort:
        raise NotImplementedError()

    if not isinstance(names_expand, bool):
        raise TypeError(
            "names_expand must be set to True or False. "
            f"Received type: {type(names_expand)}."
        )

    # validate tidy selections ------------------------------------------------
    orig_vars = __data.columns

    name_vars = _tidy_select(__data, names_from, "names_from")
    val_vars = _tidy_select(__data, values_from, "values_form")

    if not name_vars:
        raise ValueError("`names_from` must select at least one column.")

    if not val_vars:
        raise ValueError("`values_from` must select at least one column.")

    # get unique variable levels from names_from columns ----------------------

    name_data = __data.loc[:, list(name_vars)]
    if names_expand:
        # cartesian product of unique level names
        # TODO: should nan values be turned into "NA" to match dplyr?
        row_ids = expand(name_data, *name_vars)
    else:
        # distinct rows of variables
        row_ids = name_data.drop_duplicates()

    # cross with value var names ----------------------------------------------

    value_levels = pd.Series(list(val_vars), name = ".value")
    if names_vary == "fastest":
        spec = pd.merge(
            value_levels,
            row_ids,
            how="cross"
        )
    else:
        # the left arg varies slowest, so use names on the left, then relocate.
        spec = (
            pd.merge(
                row_ids,
                value_levels,
                how="cross"
            )
            .loc[:, lambda d: [".value", *d.columns[:-1]]]
        )

    # get columns used to construct .name
    if len(value_levels) > 1:
        df_name_parts = spec
    else:
        df_name_parts = spec.drop(columns=".value")

    # TODO: remove use of multiindex, which is unnecessary
    if len(df_name_parts.columns) > 1:
        spec_as_multi = pd.MultiIndex.from_frame(df_name_parts)
        name_col = _collapse_index_names(spec_as_multi, sep=names_sep, glue=names_glue)
    else:
        name_col = list(df_name_parts.iloc[:, 0])

    spec.insert(0, ".name", name_col)

    return spec
