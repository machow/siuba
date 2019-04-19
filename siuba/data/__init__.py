import pandas as pd
import pkg_resources

_fname = pkg_resources.resource_filename("siuba.data", "mtcars.csv")

mtcars = pd.read_csv(_fname)
