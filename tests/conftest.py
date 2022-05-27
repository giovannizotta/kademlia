import pytest
import simpy
from common.collector import DataCollector


@pytest.fixture()
def env() -> simpy.Environment:
    return simpy.Environment()


@pytest.fixture()
def dc() -> DataCollector:
    return DataCollector()

