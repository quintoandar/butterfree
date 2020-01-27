"""Holds the Online Feature Store writer class."""

from typing import Any, List

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, row_number

from butterfree.core.constant.columns import TIMESTAMP_COLUMN
from butterfree.core.db.configs import CassandraConfig
from butterfree.core.writer.writer import Writer


class OnlineFeatureStoreWriter(Writer):
    """Enable writing feature sets into the Online Feature Store.

    Attributes:
        spark_client: client for spark connections with external services.
        db_config:
    """

    def __init__(self, spark_client, db_config=None):
        super().__init__(spark_client)
        self.db_config = db_config or CassandraConfig()

    @staticmethod
    def filter_latest(dataframe: DataFrame, id_columns: List[Any]):
        """Filters latest data from the dataframe.

        Args:
            dataframe: spark dataframe containing data from a feature set.
            id_columns: unique identifier column set for this feature set.

        Returns:
            dataframe: contains only latest data for each unique id in the feature set.
        """
        if TIMESTAMP_COLUMN not in dataframe.columns:
            raise KeyError("DataFrame must have a 'ts' column to order by.")
        if id_columns is None or not id_columns:
            raise ValueError("Users must provide the unique identifiers.")
        missing = [c for c in id_columns if c not in dataframe.columns]
        if missing:
            raise KeyError(f"{missing} not found in the DataFrame.")

        window = Window.partitionBy(*id_columns).orderBy(col(TIMESTAMP_COLUMN).desc())
        return (
            dataframe.select(col("*"), row_number().over(window).alias("rn"),)
            .filter(col("rn") == 1)
            .drop("rn")
        )

    def write(self, feature_set, dataframe):
        """Loads the latest data from a feature set into the Online Feature Store.

        Args:
            feature_set: object processed with feature_set informations.
            dataframe: spark dataframe containing data from a feature set.
        """
        dataframe = self.filter_latest(
            dataframe=dataframe, id_columns=feature_set["key_columns"]
        )
        self.spark_client.write_dataframe(
            dataframe=dataframe,
            mode=self.db_config.mode,
            format=self.db_config.format_,
            options=self.db_config.get_options(table=feature_set["name"]),
        )

    def validate(self, feature_set, dataframe):
        """Validate to load the feature set into Writer.

        Args:
            feature_set: object processed with feature_set informations.
            dataframe: spark dataframe containing data from a feature set.

        Returns:
            False: fail validation.
            True: success validation.
        """
        if not isinstance(feature_set["format"], str):
            raise ValueError("format needs to be a string with the desired read format")

        if not isinstance(feature_set["name"], str):
            raise ValueError(
                "table_name needs to be a string with the local of the registered table"
            )

        dataframe = self.filter_latest(
            dataframe=dataframe, id_columns=feature_set["key_columns"]
        )
        dataframe = dataframe.count()

        feature_store = self.spark_client.read(
            format=feature_set["format"],
            options=self.db_config.get_options(table=feature_set["name"]),
        ).count()

        return True if feature_store == dataframe else False
