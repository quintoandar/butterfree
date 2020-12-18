import json
import logging
from typing import List

from pyspark.sql import Row

from butterfree.clients import CassandraClient
from butterfree.load.writers.foreach.foreach_writer import ForEachWriter


class ForEachCassandraWriter(ForEachWriter):
    def __init__(
        self, host: List[str], username: str, password: str, keyspace: str, table: str,
    ) -> None:
        self.__config = dict(
            host=host, username=username, password=password, keyspace=keyspace,
        )
        self.__client = self.__get_client(self.__config)
        self.__query = f"INSERT INTO {keyspace}.{table} JSON "

    @staticmethod
    def __get_client(config: dict) -> CassandraClient:
        return CassandraClient(
            cassandra_host=config.get("host"),
            cassandra_keyspace=config.get("keyspace"),
            cassandra_user=config.get("username"),
            cassandra_password=config.get("password"),
        )

    def open(self, partition_id: str, epoch_id: str) -> None:
        # Open connection. This method is optional in Python.
        logging.info(f"Partition: {partition_id} | Epoch: {epoch_id}")
        if not self.__client.conn:
            logging.info("Opening a new connection to cassandra...")
            self.__client = self.__get_client(self.__config)

    def process(self, row: Row) -> None:
        query = self.__query + json.dumps(row.asDict())
        logging.info(query)
        self.__client.sql(query)

    def close(self, error: Exception) -> None:
        # Close the connection. This method in optional in Python.
        logging.error(str(error))
        self.__client.conn.shutdown()
        raise error
