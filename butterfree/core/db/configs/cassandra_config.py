"""Holds configurations to read and write with Spark to Cassandra DB."""

from butterfree.core.configs import environment
from butterfree.core.db.configs.abstract_config import AbstractWriteConfig


class CassandraWriteConfig(AbstractWriteConfig):
    """Write configuration with spark for Cassandra databases.

    References can be found
    [here](https://docs.databricks.com/data/data-sources/cassandra.html).
    """

    def __init__(self, mode: str = None, format_: str = None, keyspace: str = None):
        """Initialize CassandraWriteConfig with specific configuration.

        :param mode: write mode for spark.
        :param format_: write format for spark.
        :param keyspace:  Cassandra DB keyspace to write data.
        """
        self.mode = mode
        self.format_ = format_
        self.keyspace = keyspace
        self.username = environment.get_variable("CASSANDRA_USERNAME")
        self.password = environment.get_variable("CASSANDRA_PASSWORD")
        self.host = environment.get_variable("CASSANDRA_HOST")

    @property
    def format_(self) -> str:
        """Attribute "format" getter.

        :return format_: write format for spark with cassandra.
        """
        return self.__format

    @format_.setter
    def format_(self, value: str):
        """Attribute "format" setter.

        :param value: used to set attribute "format_".
        """
        self.__format = value or "org.apache.spark.sql.cassandra"

    @property
    def mode(self) -> str:
        """Attribute "mode" getter.

        :return mode: write mode for spark with cassandra.
        """
        return self.__mode

    @mode.setter
    def mode(self, value):
        """Attribute "mode" setter.

        :param value: used to set attribute "mode".
        """
        self.__mode = value or "append"

    @property
    def keyspace(self) -> str:
        """Attribute "keyspace" getter.

        :return: Cassandra DB keyspace to write data.
        """
        return self.__keyspace

    @keyspace.setter
    def keyspace(self, value: str):
        """Attribute "keyspace" setter.

        Defaults to environment variable/spec "CASSANDRA_KEYSPACE".

        :param value: used to set attribute "keyspace".
        """
        value = value or environment.get_variable("CASSANDRA_KEYSPACE")
        if not value:
            raise ValueError("Config 'keyspace' cannot be empty.")
        self.__keyspace = value

    @property
    def username(self) -> str:
        """Attribute "username" getter.

        :return: username to connect to Cassandra DB.
        """
        return self.__username

    @username.setter
    def username(self, value: str):
        """Attribute "username" setter.

        Defaults to environment variable/spec "CASSANDRA_USERNAME".

        :param value: used to set attribute "username".
        """
        if value is None:
            raise ValueError("Config 'username' cannot be empty.")
        self.__username = value

    @property
    def password(self) -> str:
        """Attribute "password" getter.

        :return: password to connect to Cassandra DB.
        """
        return self.__password

    @password.setter
    def password(self, value: str):
        """Attribute "password" setter.

        Defaults to environment variable/spec "CASSANDRA_PASSWORD".

        :param value: used to set attribute "password".
        """
        if value is None:
            raise ValueError("Config 'password' cannot be empty.")
        self.__password = value

    @property
    def host(self) -> str:
        """Attribute "host" getter.

        :return: Cassandra DB's host endpoint.
        """
        return self.__host

    @host.setter
    def host(self, value: str):
        """Attribute "host" setter.

        Defaults to environment variable/spec "CASSANDRA_HOST".

        :param value: used to set attribute "host".
        """
        if value is None:
            raise ValueError("Config 'host' cannot be empty.")
        self.__host = value

    def get_options(self, table: str) -> dict:
        """Get write options.

        Options will be a dictionary with the write configuration for spark to
        cassandra.

        :param table: table name to write data into Cassandra DB.
        :return: password to connect to Cassandra DB.
        """
        return {
            "table": table,
            "keyspace": self.keyspace,
            "spark.cassandra.auth.username": self.username,
            "spark.cassandra.auth.password": self.password,
            "spark.cassandra.connection.host": self.host,
        }
