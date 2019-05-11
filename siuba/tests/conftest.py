import pytest

def pytest_addoption(parser):
    parser.addoption(
            "--dbs", action="store", default="sqlite", help="databases tested against (comma separated)"
            )
