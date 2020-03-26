"""CassandraClient entity."""
from ssl import CERT_REQUIRED, PROTOCOL_TLSv1
from typing import List

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy
from cassandra.query import dict_factory

from butterfree.core.configs import environment


class CassandraClient:
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

        self.cassandra_host = cassandra_host or environment.get_variable(
            "CASSANDRA_HOST"
        )
        self.cassandra_key_space = cassandra_key_space or environment.get_variable(
            "CASSANDRA_KEYSPACE"
        )
        self.cassandra_user = cassandra_user or environment.get_variable(
            "CASSANDRA_USERNAME"
        )
        self.cassandra_password = cassandra_password or environment.get_variable(
            "CASSANDRA_PASSWORD"
        )
        self._session = None

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
        session = cluster.connect(self.cassandra_key_space)
        session.row_factory = dict_factory
        self._session = session

    def get_table_schema(self, table: str) -> list:
        """Returns desired table schema.

        Attributes:
            table: desired table.

        Returns:
            A list dictionaries in the format
            [{"column_name": "example1", type: "cql_type"}, ...]

        """
        if not self._session:
            raise RuntimeError("There's no session available for this query.")

        response = list(
            self._session.execute(
                f"SELECT column_name, type FROM system_schema.columns "  # noqa
                f"WHERE keyspace_name = '{self.cassandra_key_space}' "  # noqa
                f"  AND table_name = '{table}';"  # noqa
            )
        )

        if not response:
            raise RuntimeError(
                f"No columns found for table: {table}"
                f"in key space: {self.cassandra_key_space}"
            )

        return response

    def _get_create_table_query(
        self,
        columns: dict,
        table: str,
        bloom_filter_fp_chance: str,
        caching: str,
        comment: str,
        compaction: dict,
        compression: dict,
        crc_check_chance: float,
        dclocal_read_repair_chance: float,
        default_time_to_live: int,
        gc_grace_seconds: int,
        max_index_interval: int,
        memtable_flush_period_in_ms: int,
        min_index_interval: int,
        read_repair_chance: float,
        speculative_retry: str,
    ):
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

        query = (
            f"CREATE TABLE {self.cassandra_key_space}.{table} "
            f"({columns_str}) "
            f"WITH "
            f"    bloom_filter_fp_chance = {bloom_filter_fp_chance}"
            f"    AND caching = {caching}"
            f"    AND comment = '{comment}'"
            f"    AND compaction = {compaction}"
            f"    AND compression = {compression}"
            f"    AND crc_check_chance = {crc_check_chance}"
            f"    AND dclocal_read_repair_chance = {dclocal_read_repair_chance}"
            f"    AND default_time_to_live = {default_time_to_live}"
            f"    AND gc_grace_seconds = {gc_grace_seconds}"
            f"    AND max_index_interval = {max_index_interval}"
            f"    AND memtable_flush_period_in_ms = {memtable_flush_period_in_ms}"
            f"    AND min_index_interval = {min_index_interval}"
            f"    AND read_repair_chance = {read_repair_chance}"
            f"    AND speculative_retry = '{speculative_retry}';"
        )

        return query

    def create_table(
        self,
        columns: dict,
        table: str,
        bloom_filter_fp_chance: float = None,
        caching: dict = None,
        comment: str = None,
        compaction: dict = None,
        compression: dict = None,
        crc_check_chance: float = None,
        dclocal_read_repair_chance: float = None,
        default_time_to_live: int = None,
        gc_grace_seconds: int = None,
        max_index_interval: int = None,
        memtable_flush_period_in_ms: int = None,
        min_index_interval: int = None,
        read_repair_chance: float = None,
        speculative_retry: str = None,
    ) -> None:
        """Creates a table.

        Attributes:
            columns: a list dictionaries in the format
                [{"column_name": "example1", type: "cql_type", primary_key: True}, ...]
            table_name: the name of the table to be created
            bloom_filter_fp_chance: cassandra table parameter
            caching: cassandra table parameter
            comment: cassandra table parameter
            compaction: cassandra table parameter
            compression: cassandra table parameter
            crc_check_chance: cassandra table parameter
            dclocal_read_repair_chance: cassandra table parameter
            default_time_to_live: cassandra table parameter
            gc_grace_seconds: cassandra table parameter
            max_index_interval: cassandra table parameter
            memtable_flush_period_in_ms: cassandra table parameter
            min_index_interval: cassandra table parameter
            read_repair_chance: cassandra table parameter
            speculative_retry: cassandra table parameter
        """
        if not self._session:
            raise RuntimeError("There's no session available for this query.")

        bloom_filter_fp_chance = bloom_filter_fp_chance or "0.01"
        caching = caching or "{'keys': 'ALL', 'rows_per_partition': 'NONE'}"
        comment = comment or ""
        compaction = compaction or {
            "class": "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy",
            "max_threshold": "32",
            "min_threshold": "4",
        }
        compression = compression or {
            "chunk_length_in_kb": "64",
            "class": "org.apache.cassandra.io.compress.LZ4Compressor",
        }
        crc_check_chance = crc_check_chance or 1.0
        dclocal_read_repair_chance = dclocal_read_repair_chance or 0.1
        default_time_to_live = default_time_to_live or 0
        gc_grace_seconds = gc_grace_seconds or 864000
        max_index_interval = max_index_interval or 2048
        memtable_flush_period_in_ms = memtable_flush_period_in_ms or 0
        min_index_interval = min_index_interval or 128
        read_repair_chance = read_repair_chance or 0.0
        speculative_retry = speculative_retry or "99PERCENTILE"

        query = self._get_create_table_query(
            columns,
            table,
            bloom_filter_fp_chance,
            caching,
            comment,
            compaction,
            compression,
            crc_check_chance,
            dclocal_read_repair_chance,
            default_time_to_live,
            gc_grace_seconds,
            max_index_interval,
            memtable_flush_period_in_ms,
            min_index_interval,
            read_repair_chance,
            speculative_retry,
        )

        self._session.execute(query)

    def execute_query(
        self, query: str,
    ):
        """Executes desired query.

        Attributes:
            query: desired query.

        """
        if not self._session:
            raise RuntimeError("There's no session available for this query.")
        self._session.execute(query)
