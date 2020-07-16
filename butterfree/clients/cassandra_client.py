"""CassandraClient entity."""
from ssl import CERT_REQUIRED, PROTOCOL_TLSv1
from typing import List

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy
from cassandra.query import dict_factory

from butterfree.clients import AbstractClient


class CassandraClient(AbstractClient):
    """Cassandra Client.

    Attributes:
        cassandra_user: username to use in connection.
        cassandra_password: password to use in connection.
        cassandra_key_space: key space used in connection.
        cassandra_host: cassandra endpoint used in connection.
    """

    def __init__(
        self,
        cassandra_host: List[str],
        cassandra_key_space,
        cassandra_user=None,
        cassandra_password=None,
    ):
        self.cassandra_host = cassandra_host
        self.cassandra_key_space = cassandra_key_space
        self.cassandra_user = cassandra_user
        self.cassandra_password = cassandra_password
        self._session = None

    @property
    def conn(self, *, ssl_path: str = None):
        """Establishes a Cassandra connection."""
        auth_provider = (
            PlainTextAuthProvider(
                username=self.cassandra_user, password=self.cassandra_password
            )
            if self.cassandra_user is not None
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

        cluster = Cluster(
            contact_points=self.cassandra_host,
            auth_provider=auth_provider,
            ssl_options=ssl_opts,
            load_balancing_policy=RoundRobinPolicy(),
        )
        self._session = cluster.connect(self.cassandra_key_space)
        self._session.row_factory = dict_factory
        return self._session

    def sql(
        self, query: str,
    ):
        """Executes desired query.

        Attributes:
            query: desired query.

        """
        if not self._session:
            raise RuntimeError("There's no session available for this query.")
        return self._session.execute(query)

    def get_schema(self, table: str) -> list:
        """Returns desired table schema.

        Attributes:
            table: desired table.

        Returns:
            A list dictionaries in the format
            [{"column_name": "example1", type: "cql_type"}, ...]

        """
        query = (
            f"SELECT column_name, type FROM system_schema.columns "  # noqa
            f"WHERE keyspace_name = '{self.cassandra_key_space}' "  # noqa
            f"  AND table_name = '{table}';"  # noqa
        )

        response = list(self.sql(query))

        if not response:
            raise RuntimeError(
                f"No columns found for table: {table}"
                f"in key space: {self.cassandra_key_space}"
            )

        return response

    def _get_create_table_query(self, columns: dict, table: str):
        """Creates CQL statement to create a table."""
        parsed_columns = []
        primary_keys = []

        for col in columns:
            col_str = f"{col['column_name']} {col['type']}"
            if col["primary_key"]:
                primary_keys.append(col["column_name"])
            parsed_columns.append(col_str)

        parsed_columns = ", ".join(parsed_columns)

        if len(primary_keys) > 0:
            primary_keys = ", ".join(primary_keys)
            columns_str = f"{parsed_columns}, PRIMARY KEY ({primary_keys})"
        else:
            columns_str = parsed_columns

        query = f"CREATE TABLE {self.cassandra_key_space}.{table} " f"({columns_str}); "

        return query

    def create_table(self, columns: dict, table: str,) -> None:
        """Creates a table.

        Attributes:
            columns: a list dictionaries in the format
                [{"column_name": "example1", type: "cql_type", primary_key: True}, ...]
        """
        query = self._get_create_table_query(columns, table)

        self.sql(query)
