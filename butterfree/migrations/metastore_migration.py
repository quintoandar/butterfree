"""Metastore Migration entity."""

from butterfree.migrations import Migration


class MetastoreMigration(Migration):
    """Cassandra class for Migrations."""

    def apply_query(self) -> None:
        """Extract data from target origin.

        Args:
            client: client responsible for connecting to Spark session.

        Returns:
            Dataframe with all the data.

        :return: Spark dataframe
        """

    def apply_migration(self) -> None:
        """Extract data from target origin.

        Args:
            client: client responsible for connecting to Spark session.

        Returns:
            Dataframe with all the data.

        :return: Spark dataframe
        """
