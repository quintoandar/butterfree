"""CassandraClient entity."""

import logging
from ssl import CERT_REQUIRED, PROTOCOL_TLSv1
from typing import Dict, List, Optional, Union

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import (
    EXEC_PROFILE_DEFAULT,
    Cluster,
    ExecutionProfile,
    ResponseFuture,
    Session,
)
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import ConsistencyLevel, dict_factory
from typing_extensions import TypedDict

from butterfree.clients import AbstractClient

logger = logging.getLogger(__name__)

EMPTY_STRING_HOST_ERROR = "The value of Cassandra host is empty. Please fill correctly with your endpoints"  # noqa: E501
GENERIC_INVALID_HOST_ERROR = "The Cassandra host must be a valid string, a string that represents a list or list of strings"  # noqa: E501


class CassandraColumn(TypedDict):
    """Type for cassandra columns.

    It's just a type abstraction, we can use it or a normal dict

    >>> def function(column: CassandraColumn) -> CassandraColumn:
    ...     return column
    >>> # The following two lines will pass in the type checking
    >>> function({'column_name': 'test', 'type': 'integer', 'primary_key': False})
    >>> function(CassandraColumn(column_name='test', type='integer', primary_key=False))

    """

    column_name: str
    type: str
    primary_key: bool


class CassandraClient(AbstractClient):
    """Cassandra Client.

    Attributes:
        user: username to use in connection.
        password: password to use in connection.
        keyspace: key space used in connection.
        host: cassandra endpoint used in connection.
    """

    def __init__(
        self,
        host: List[str],
        keyspace: str,
        user: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        self.host = self._validate_and_format_cassandra_host(host)
        logger.info(f"The host setted is {self.host}")
        self.keyspace = keyspace
        self.user = user
        self.password = password
        self._session: Optional[Session] = None

    def _validate_and_format_cassandra_host(self, host: Union[List, str]):
        """
        Validate and format the provided Cassandra host input.

        This method checks if the input `host` is either a string, a list of strings, or
        a list containing a single string with comma-separated values. It splits the string
        by commas and trims whitespace, returning a list of hosts. If the input is already
        a list of strings, it returns that list. If the input is empty or invalid, a
        ValueError is raised.

        Args:
            host (str | list): The Cassandra host input, which can be a comma-separated
                            string or a list of string endpoints.

        Returns:
            list: A list of formatted Cassandra host strings.

        Raises:
            ValueError: If the input is an empty list/string or if it is not a string
                        (or a representation of a list) or a list of strings.
        """  # noqa: E501
        if isinstance(host, str):
            if host:
                return [item.strip() for item in host.split(",")]
            else:
                raise ValueError(EMPTY_STRING_HOST_ERROR)

        if isinstance(host, list):
            if len(host) == 1 and isinstance(host[0], str):
                return [item.strip() for item in host[0].split(",")]
            elif all(isinstance(item, str) for item in host):
                return host

        raise ValueError(GENERIC_INVALID_HOST_ERROR)

    @property
    def conn(self, *, ssl_path: str = None) -> Session:  # type: ignore
        """Establishes a Cassandra connection."""
        if not self._session:
            auth_provider = (
                PlainTextAuthProvider(username=self.user, password=self.password)
                if self.user is not None
                else None
            )
            ssl_opts = (
                {
                    "ca_certs": ssl_path,
                    "ssl_version": PROTOCOL_TLSv1,
                    "cert_reqs": CERT_REQUIRED,
                }
                if ssl_path is not None
                else None
            )

            execution_profiles = {
                EXEC_PROFILE_DEFAULT: ExecutionProfile(
                    load_balancing_policy=DCAwareRoundRobinPolicy(),
                    consistency_level=ConsistencyLevel.LOCAL_QUORUM,
                    row_factory=dict_factory,
                )
            }
            cluster = Cluster(
                contact_points=self.host,
                auth_provider=auth_provider,
                ssl_options=ssl_opts,
                execution_profiles=execution_profiles,
            )
            self._session = cluster.connect(self.keyspace)
        return self._session

    def sql(self, query: str) -> ResponseFuture:
        """Executes desired query.

        Attributes:
            query: desired query.

        """
        return self.conn.execute(query)

    def get_schema(
        self, table: str, database: Optional[str] = None
    ) -> List[Dict[str, str]]:
        """Returns desired table schema.

        Attributes:
            table: desired table.

        Returns:
            A list dictionaries in the format
            [{"column_name": "example1", type: "cql_type"}, ...]

        """
        query = (
            f"SELECT column_name, type FROM system_schema.columns "  # noqa
            f"WHERE keyspace_name = '{self.keyspace}' "  # noqa
            f"  AND table_name = '{table}';"  # noqa
        )

        response = list(self.sql(query))

        if not response:
            raise RuntimeError(
                f"No columns found for table: {table}" f"in key space: {self.keyspace}"
            )

        return response

    def _get_create_table_query(
        self,
        columns: List[CassandraColumn],
        table: str,
    ) -> str:
        """Creates CQL statement to create a table."""
        parsed_columns = []
        primary_keys = []

        for col in columns:
            col_str = f"{col['column_name']} {col['type']}"
            if col["primary_key"]:
                primary_keys.append(col["column_name"])
            parsed_columns.append(col_str)

        joined_parsed_columns = ", ".join(parsed_columns)

        if len(primary_keys) > 0:
            joined_primary_keys = ", ".join(primary_keys)
            columns_str = (
                f"{joined_parsed_columns}, PRIMARY KEY ({joined_primary_keys})"
            )
        else:
            columns_str = joined_parsed_columns

        query = f"CREATE TABLE {self.keyspace}.{table} " f"({columns_str}); "

        return query

    def create_table(self, columns: List[CassandraColumn], table: str) -> None:
        """Creates a table.

        Attributes:
            columns: a list dictionaries in the format
                [{"column_name": "example1", type: "cql_type", primary_key: True}, ...]
        """
        query = self._get_create_table_query(columns, table)

        self.sql(query)
