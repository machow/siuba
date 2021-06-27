from siuba.sql.utils import get_dialect_translator
import pytest

@pytest.mark.parametrize('name', [
    'redshift',
    'postgresql',
    'sqlite'
    ])
def test_get_dialect_translator(name):
    get_dialect_translator(name)
