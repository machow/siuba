from siuba.sql.utils import get_dialect_translator, mock_sqlalchemy_engine
from siuba.sql.verbs import collect
from siuba.sql import LazyTbl
import pytest

@pytest.mark.parametrize('name', [
    'redshift',
    'postgresql',
    'sqlite'
    ])
def test_get_dialect_translator(name):
    get_dialect_translator(name)

def test_mock_sqlalchemy_engine_dialect():
    engine = mock_sqlalchemy_engine("postgresql")
    assert engine.dialect.name == "postgresql"

    engine = mock_sqlalchemy_engine("sqlite")
    assert engine.dialect.name == "sqlite"

def test_mock_sqlalchemy_engine_no_collect():
    engine = mock_sqlalchemy_engine("sqlite")
    tbl = LazyTbl(engine, "some_table", ["x"])
    assert collect(tbl) is None
