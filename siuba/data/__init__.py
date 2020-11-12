import pandas as _pd
import sys
from pkg_resources import resource_filename as _rf

__all__ = [
    "mtcars", "cars", "cars_sql",
    "relig_income"
    ]

# Utils =======================================================================

def _load_df(name):
    # There is an artist in the data named "N/A", so tell pandas it's not an NA
    return _pd.read_csv(_rf("siuba.data", name + ".csv"))

def __getattr__(x):
    if x in __all__:
        df = _load_df(x)
        globals()[x] = df
        return x

    raise AttributeError("Only these datasets are defined: %s" % __all__)

if sys.version_info[0] == 3 and sys.version_info[1] < 7:
    # __getattr__ implemented in python 3.7, so for earlier versions need to
    # load all the data up front.
    for _fname in set(__all__) - {"cars", "cars_sql"}:
        globals()[_fname] = _load_df(_fname)

# mtcars ======================================================================

_fname = _rf("siuba.data", "mtcars.csv")

mtcars = _pd.read_csv(_fname)
mtcars.__doc__ = """
mtcars data.

Source: Henderson and Velleman (1981), Building multiple regression models interactively. Biometrics, 37, 391–411.

--- Original DataFrame docs below ---
""" + mtcars.__doc__


# cars ------------------------------------------------------------------------
cars = mtcars[["cyl", "mpg", "hp"]]


# cars_sql --------------------------------------------------------------------
import siuba.sql.utils as _sql_utils
from siuba.sql import LazyTbl as _LazyTbl
cars_sql = _LazyTbl(
        _sql_utils.mock_sqlalchemy_engine("postgresql"),
        "cars",
        ["cyl", "mpg", "hp"]
        )


# Tidyr data ==================================================================

