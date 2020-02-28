"""CassandraClient entity."""
from cassandra.cluster import Cluster

from butterfree.core.configs import environment


class CassandraClient:
    """Handle Cassandra session connection.

    Get query results with CQL on external systems.

    Attributes:
        username: username to use in connection.
        password: password to use in connection.

    """

    def __init__(self):
        self.username = environment.get_variable("CASSANDRA_USERNAME")
        self.password = environment.get_variable("CASSANDRA_PASSWORD")
        self.host = environment.get_variable("CASSANDRA_HOST")

    @property
    def conn(self):
        """Create a Cassandra connect.

        Returns:
            Cassandra session
        """
        cluster = Cluster([self.host])
        session = cluster.connect()
        return session

    def sql(self, query):
        """Run a query using Cassandra CQL.

        Args:
            query: Cassandra CQL query.

        Returns:
            Result set
        """
        return self.conn.execute(query)
