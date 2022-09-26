__all__ = [
        "mtcars",
        "cars",
        "penguins",
        "penguins_raw",
        "cars_sql",
        "band_members",
        "band_instruments",
        "band_instruments2",
        "fish_encounters",
        "us_rent_income",
        "warpbreaks",
        ]

def __dir__():
    return __all__


def _load_data_csv_gz(name):
    import pandas as pd
    import pkg_resources

    fname = pkg_resources.resource_filename("siuba.data", f"{name}.csv.gz")
    return pd.read_csv(fname)


def _load_data_csv(name):
    import pandas as pd
    import pkg_resources

    fname = pkg_resources.resource_filename("siuba.data", f"{name}.csv")
    return pd.read_csv(fname)


def _load_data_cars_sql():
    import siuba.sql.utils as _sql_utils
    from siuba.sql import LazyTbl as _LazyTbl
    cars_sql = _LazyTbl(
            _sql_utils.mock_sqlalchemy_engine("postgresql"),
            "cars",
            ["cyl", "mpg", "hp"]
            )
    
    return cars_sql


def __getattr__(name):
    if name not in __all__:
        raise AttributeError(f"No dataset named: {name}")

    if name == "cars":
        return _load_data_csv_gz("mtcars")[["cyl", "mpg", "hp"]]

    elif name == "cars_sql":
        return _load_data_cars_sql() 

    elif name in {"band_members", "band_instruments", "band_instruments2"}:
        return _load_data_csv(name)

    return _load_data_csv_gz(name)

# cars_sql --------------------------------------------------------------------
