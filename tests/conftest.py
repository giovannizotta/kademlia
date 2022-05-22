import pytest
import simpy
from common.node import DataCollector

@pytest.fixture()
def env() -> simpy.Environment:
    return simpy.Environment()


@pytest.fixture()
def dc() -> DataCollector:
    return DataCollector()

