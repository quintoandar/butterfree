"""CassandraClient entity."""
from ssl import CERT_REQUIRED, PROTOCOL_TLSv1
from typing import Dict, List, Optional

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, ResponseFuture, Session
from cassandra.policies import RoundRobinPolicy
from cassandra.query import dict_factory
from typing_extensions import TypedDict

from butterfree.clients import AbstractClient


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
        self.host = host
        self.keyspace = keyspace
        self.user = user
        self.password = password
        self._session: Optional[Session] = None

    @property
    def conn(self, *, ssl_path: str = None) -> Session:  # type: ignore
        """Establishes a Cassandra connection."""
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

        cluster = Cluster(
            contact_points=self.host,
            auth_provider=auth_provider,
            ssl_options=ssl_opts,
            load_balancing_policy=RoundRobinPolicy(),
        )
        self._session = cluster.connect(self.keyspace)
        self._session.row_factory = dict_factory
        return self._session

    def sql(self, query: str) -> ResponseFuture:
        """Executes desired query.

        Attributes:
            query: desired query.

        """
        if not self._session:
            raise RuntimeError("There's no session available for this query.")
        return self._session.execute(query)

    def get_schema(self, table: str) -> List[Dict[str, str]]:
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
        self, columns: List[CassandraColumn], table: str,
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
