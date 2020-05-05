import pandas as pd
import pkg_resources

# mtcars ----------------------------------------------------------------------
_fname = pkg_resources.resource_filename("siuba.data", "mtcars.csv")

mtcars = pd.read_csv(_fname)
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

