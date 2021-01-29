"""Holds functions for managing the running environment."""
import os
from typing import Optional

specification = {
    "ENVIRONMENT": "dev",
    "CASSANDRA_HOST": "test",
    "CASSANDRA_KEYSPACE": "test",
    "CASSANDRA_USERNAME": "test",
    "CASSANDRA_PASSWORD": "test",
    "FEATURE_STORE_S3_BUCKET": "test",
    "FEATURE_STORE_HISTORICAL_DATABASE": "test",
    "KAFKA_CONSUMER_CONNECTION_STRING": "test_host:1234,test_host2:1234",
    "STREAM_CHECKPOINT_PATH": None,
}


class UnspecifiedVariableError(RuntimeError):
    """Environment variables not set error.

    Attributes:
        variable_name: environment variable name.

    """

    def __init__(self, variable_name: str):
        super().__init__(
            f'Variable "{variable_name}" is not listed in the environment'
            " specification\nUpdate the environment module"
            f' to include "{variable_name}"'
        )


def get_variable(variable_name: str, default_value: str = None) -> Optional[str]:
    """Gets an environment variable.

    The variable comes from it's explicitly declared value in the running
    environment or from the default value declared in specification or from the
    default_value.

    Args:
        variable_name: environment variable name.
        default_value: default value to use in case no value is set in the
            environment nor in the environment.yaml specification file.

    Returns:
        The variable's string value

    """
    try:
        spec_default = specification[variable_name]
    except KeyError:
        raise UnspecifiedVariableError(variable_name)
    return os.getenv(variable_name) or spec_default or default_value
