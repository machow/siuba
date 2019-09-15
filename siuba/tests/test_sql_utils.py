from siuba.sql.utils import get_dialect_funcs
import pytest

@pytest.mark.parametrize('name', [
    'redshift',
    'postgresql',
    'sqlite'
    ])
def test_get_dialect_funcs(name):
    get_dialect_funcs(name)
