import pandas as pd
import pkg_resources

_fname = pkg_resources.resource_filename("siuba.data", "mtcars.csv")

mtcars = pd.read_csv(_fname)
mtcars.__doc__ = """
mtcars data.

Source: Henderson and Velleman (1981), Building multiple regression models interactively. Biometrics, 37, 391–411.

--- Original DataFrame docs below ---
""" + mtcars.__doc__
