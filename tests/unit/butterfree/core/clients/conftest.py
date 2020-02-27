from unittest.mock import Mock

import pytest


@pytest.fixture()
def mocked_spark_read():
    mock = Mock()
    mock.readStream = mock
    mock.read = mock
    mock.format.return_value = mock
    mock.options.return_value = mock
    return mock


@pytest.fixture()
def mocked_spark_write():
    mock = Mock()
    mock.dataframe = mock
    mock.write = mock
    return mock
