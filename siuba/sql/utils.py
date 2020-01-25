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


def mock_sqlalchemy_engine(dialect):
    """
    Create a sqlalchemy.engine.Engine without it connecting to a database.

    Examples
    --------

    ::
        from siuba.sql import LazyTbl
        from siuba import _, mutate, show_query

        engine = mock_sqlalchemy_engine('postgresql')
        tbl = LazyTbl(engine, 'some_table', ['x'])

        query = mutate(tbl, y = _.x + _.x)
        show_query(query)

    """
    from sqlalchemy.engine import Engine
    from sqlalchemy.dialects import registry

    dialect_cls = registry.load('postgresql')   
    return Engine(None, dialect_cls(), '')  

