import pytest
from .helpers import (
    assert_equal_query,
    PandasBackend,
    SqlBackend,
    CloudBackend,
    BigqueryBackend,
    data_frame
)

def pytest_addoption(parser):
    parser.addoption(
            "--dbs", action="store", default="sqlite", help="databases tested against (comma separated)"
            )

params_backend = [
    pytest.param(lambda: SqlBackend("postgresql"), id = "postgresql", marks=pytest.mark.postgresql),
    pytest.param(lambda: SqlBackend("mysql"), id = "mysql", marks=pytest.mark.mysql),
    pytest.param(lambda: SqlBackend("sqlite"), id = "sqlite", marks=pytest.mark.sqlite),
    pytest.param(lambda: BigqueryBackend("bigquery"), id = "bigquery", marks=pytest.mark.bigquery),
    pytest.param(lambda: CloudBackend("snowflake"), id = "snowflake", marks=pytest.mark.snowflake),
    pytest.param(lambda: PandasBackend("pandas"), id = "pandas", marks=pytest.mark.pandas)
    ]

@pytest.fixture(params = params_backend, scope = "session")
def backend(request):
    return request.param()

@pytest.fixture
def skip_backend(request, backend):
    if request.node.get_closest_marker('skip_backend'):
        mark_args = request.node.get_closest_marker('skip_backend').args
        if backend.name in mark_args:
            pytest.skip('skipped on backend: {}'.format(backend.name)) 

@pytest.fixture
def xfail_backend(request, backend):
    if request.node.get_closest_marker('xfail_backend'):
        mark_args = request.node.get_closest_marker('xfail_backend').args
        if backend.name in mark_args:
            pytest.xfail()
