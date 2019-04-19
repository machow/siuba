import importlib

def get_dialect_funcs(name):
    #dialect = engine.dialect.name
    mod = importlib.import_module('siuba.sql.dialects.{}'.format(name))
    return mod.funcs

