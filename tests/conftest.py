import pytest
from simpy.core import Environment

from common.collector import DataCollector


@pytest.fixture()
def env() -> Environment:
    return Environment()


@pytest.fixture()
def dc() -> DataCollector:
    return DataCollector()
