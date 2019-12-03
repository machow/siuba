import importlib

def get_dialect_funcs(name):
    #dialect = engine.dialect.name
    mod = importlib.import_module('siuba.sql.dialects.{}'.format(name))
    return mod.funcs

def get_sql_classes(name):
    mod = importlib.import_module('siuba.sql.dialects.{}'.format(name))
    win_name = name.title() + "Column"
    agg_name = name.title() + "ColumnAgg"
    return {
            'window': getattr(mod, win_name),
            'aggregate': getattr(mod, agg_name)
            }

