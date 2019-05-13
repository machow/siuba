import pytest
from .helpers import assert_equal_query, Backend, data_frame

def pytest_addoption(parser):
    parser.addoption(
            "--dbs", action="store", default="sqlite", help="databases tested against (comma separated)"
            )

params_backend = [
    pytest.param(lambda: Backend("postgresql"), id = "postgresql", marks=pytest.mark.postgresql),
    pytest.param(lambda: Backend("sqlite"), id = "sqlite", marks=pytest.mark.sqlite)
    ]

@pytest.fixture(params = params_backend, scope = "session")
def backend(request):
    return request.param()

@pytest.fixture(autouse=True)
def skip_backend(request, backend):
    if request.node.get_closest_marker('skip_backend'):
        mark_args = request.node.get_closest_marker('skip_backend').args
        if backend.name in mark_args:
            pytest.skip('skipped on backend: {}'.format(backend.name)) 

@pytest.fixture(autouse=True)
def notimpl_backend(request, backend):
    if request.node.get_closest_marker('notimpl_backend'):
        mark_args = request.node.get_closest_marker('notimpl_backend').args
        if backend.name in mark_args:
            pytest.skip('skipped on backend: {}'.format(backend.name)) 

   

