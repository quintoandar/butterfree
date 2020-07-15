"""SparkClient entity."""

from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType

from butterfree.clients import AbstractClient


class SparkClient(AbstractClient):
    """Handle Spark session connection.

    Get query results with SQL, reads and writes data on external systems.

    """

    def __init__(self):
        self._session = None

    @property
    def conn(self) -> SparkSession:
        """Gets or creates an SparkSession.

        Returns:
            Spark session

        """
        if not self._session:
            self._session = SparkSession.builder.getOrCreate()
        return self._session

    def read(
        self,
        format: str,
        options: dict,
        schema: StructType = None,
        stream: bool = False,
    ) -> DataFrame:
        """Use the SparkSession.read interface to load data into a dataframe.

        Check docs for more information:
            https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#generic-loadsave-functions

        Args:
            format: string with the format to be used by the DataframeReader.
            options: options to setup the DataframeReader.
            stream:  flag to indicate if data must be read in stream mode.
            schema: an optional pyspark.sql.types.StructType for the input schema.

        Returns:
            Dataframe

        """
        if not isinstance(format, str):
            raise ValueError("format needs to be a string with the desired read format")
        if not isinstance(options, dict):
            raise ValueError("options needs to be a dict with the setup configurations")

        df_reader = self.conn.readStream if stream else self.conn.read
        df_reader = df_reader.schema(schema) if schema else df_reader
        return df_reader.format(format).options(**options).load()

    def read_table(self, table: str, database: str = None) -> DataFrame:
        """Use the SparkSession.read interface to read a metastore table.

        Args:
            database: name of the metastore database/schema
            table: name of the table in metastore

        Returns:
            Dataframe

        """
        if not isinstance(table, str):
            raise ValueError(
                "table needs to be a string with the name of the registered table"
            )
        return self.conn.read.table(f"{database}.{table}" if database else table)

    def sql(self, query: str) -> DataFrame:
        """Run a query using Spark SQL.

        Args:
            query: Spark SQL query.

        Returns:
            Dataframe

        """
        return self.conn.sql(query)

    @staticmethod
    def write_dataframe(dataframe: DataFrame, format_: str, mode: str, **options):
        """Receive a spark DataFrame and write it.

        Args:
            dataframe: dataframe containing data from a feature set.
            format_: format used to save the dataframe.
            mode: writing modem can be "error", "append", "overwrite" or
                "ignore". For more information:
                [here](https://spark.apache.org/docs/2.3.0/sql-programming-guide.html#save-modes).
            **options: all other options that can be used in a DataFrameWriter.

        """
        if not isinstance(format_, str):
            raise ValueError("format needs to be a string")
        if not isinstance(mode, str):
            raise ValueError("mode needs to be a string")
        dataframe.write.save(format=format_, mode=mode, **options)

    def write_stream(
        self,
        dataframe: DataFrame,
        processing_time: str,
        output_mode: str,
        checkpoint_path: str,
        format_: str,
        mode: str,
        **options,
    ):
        """Starts streaming data writing job.

        Args:
            dataframe: Spark dataframe containing data from a feature set.
            processing_time: a processing time interval as a string.
                E.g. '5 seconds', '1 minute'. Set a trigger that runs the
                mini-batch periodically based on the processing time. If the
                effect of processing data as soon as the data arrives, without
                having to wait for the time frame, is desired, the value
                '0 seconds' can be set.
            output_mode: specifies how data of a streaming DataFrame/Dataset is
                written to a streaming sink destination.
            checkpoint_path: path on S3 to save checkpoints for the stream job.
                These checkpoint can be used on the the job re-start to return
                from where it stops.
            format_: format used to save the dataframe.
            mode: writing modem can be "error", "append", "overwrite" or
                "ignore". For more information:
                [here](https://spark.apache.org/docs/2.3.0/sql-programming-guide.html#save-modes).
            **options: all other options that can be used in a DataFrameWriter.

        More information about processing_time, output_mode and checkpoint_path
        can be found in Spark documentation:
        [here](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

        Returns:
            Streaming handler.

        """
        if not dataframe.isStreaming:
            raise ValueError("A stream df is needed to start a streaming job.")

        return (
            dataframe.writeStream.trigger(processingTime=processing_time)
            .outputMode(output_mode)
            .option("checkpointLocation", checkpoint_path)
            .foreachBatch(
                lambda batch_df, _: self.write_dataframe(
                    batch_df, format_, mode, **options
                )
            )
            .start()
        )

    @staticmethod
    def write_table(
        dataframe: DataFrame,
        database: str,
        table_name: str,
        path: str,
        format_: str = None,
        mode: str = None,
        partition_by: List[str] = None,
        **options,
    ):
        """Receive a spark DataFrame and write it as a table in metastore.

        Args:
            dataframe: spark dataframe containing data from a feature set.
            database: specified database name.
            table_name: specified table name.
            path: string with the local to save the table.
            format_: string with the format used to save.
            mode: writing mode, it can be: "error", "append", "overwrite" or
                "ignore". More information:
                [here](https://spark.apache.org/docs/2.3.0/sql-programming-guide.html#save-modes).
            partition_by: names of partitioning columns.
            options: all other options that can be used in a DataFrameWriter.

        """
        if not isinstance(database, str):
            raise ValueError("database needs to be a string")
        if not isinstance(table_name, str):
            raise ValueError("table_name needs to be a string")
        if not isinstance(path, str):
            raise ValueError("path needs to be a string of the local to save")

        name = "{}.{}".format(database, table_name)

        dataframe.write.saveAsTable(
            mode=mode,
            format=format_,
            partitionBy=partition_by,
            name=name,
            path=path,
            **options,
        )

    def create_temporary_view(
        self, dataframe: DataFrame, name: str
    ) -> Optional[StreamingQuery]:
        """Create a temporary view from a given dataframe.

        Args:
            dataframe: dataframe to be be queried by the view.
            name: name of the temporary view.

        """
        return (
            dataframe.createOrReplaceTempView(name)
            if not dataframe.isStreaming
            else dataframe.writeStream.format("memory").queryName(name).start()
        )
