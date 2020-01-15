from butterfree.core.configs import environment
from butterfree.core.db.configs.abstract_config import AbstractWriteConfig


class CassandraWriteConfig(AbstractWriteConfig):
    def __init__(self, mode=None, format_=None, keyspace=None):
        self.mode = mode
        self.format_ = format_
        self.keyspace = keyspace
        self.username = environment.get_variable("CASSANDRA_USERNAME")
        self.password = environment.get_variable("CASSANDRA_PASSWORD")
        self.host = environment.get_variable("CASSANDRA_HOST")

    @property
    def format_(self):
        return self.__format

    @format_.setter
    def format_(self, value):
        self.__format = value or "org.apache.spark.sql.cassandra"

    @property
    def mode(self):
        return self.__mode

    @mode.setter
    def mode(self, value):
        self.__mode = value or "append"

    @property
    def keyspace(self):
        return self.__keyspace

    @keyspace.setter
    def keyspace(self, value):
        value = value or environment.get_variable("CASSANDRA_KEYSPACE")
        if value is None:
            raise ValueError("Config 'keyspace' cannot be empty.")
        self.__keyspace = value

    @property
    def username(self):
        return self.__username

    @username.setter
    def username(self, value):
        if value is None:
            raise ValueError("Config 'username' cannot be empty.")
        self.__username = value

    @property
    def password(self):
        return self.__password

    @password.setter
    def password(self, value):
        if value is None:
            raise ValueError("Config 'password' cannot be empty.")
        self.__password = value

    @property
    def host(self):
        return self.__host

    @host.setter
    def host(self, value):
        if value is None:
            raise ValueError("Config 'host' cannot be empty.")
        self.__host = value

    @property
    def options(self):
        return {
            "keyspace": self.keyspace,
            "spark.cassandra.auth.username": self.username,
            "spark.cassandra.auth.password": self.password,
            "spark.cassandra.connection.host": self.host,
        }
