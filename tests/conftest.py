import pytest

from tradeflow.spark import create_spark


@pytest.fixture(scope="session")
def spark():
    session = create_spark("tradeflow-tests")
    yield session
    session.stop()
