import importlib

def test_data_imports():
    import siuba.data
    from siuba.data import __all__

    # note that we can't do import * inside a function, so programmatically fetch
    # each dataset
    for entry in __all__:
        getattr(siuba.data, entry)
