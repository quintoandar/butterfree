import logging
import os

from parameters_validation import validate_parameters
from pyspark.sql import DataFrame

from quintoandar_butterfree.core.output import Output


class Cassandra(Output):
    @validate_parameters
    def __init__(self, column_family: str):
        self._keyspace_name = "feature_store"
        self._cassandra_connection_host = os.getenv("CASSANDRA_CONNECTION_HOST")
        self._column_family = column_family

    @validate_parameters
    def write(self, dataframe: DataFrame):
        load_options = self._load_cassandra_config()

        logging.info(
            f"Writing to the Cassandra at {self._keyspace_name}.{self._column_family}"
        )

        dataframe.write.mode("append").format("org.apache.spark.sql.cassandra").options(
            **load_options
        ).save()

        logging.info("Finished writing to the Online Feature Store")

    def _load_cassandra_config(self):
        return {
            "table": self._column_family,
            "keyspace": self._keyspace_name,
            "spark.cassandra.auth.username": os.getenv("CASSANDRA_USERNAME"),
            "spark.cassandra.auth.password": os.getenv("CASSANDRA_PASSWORD"),
            "spark.cassandra.connection.host": self._cassandra_connection_host,
        }
