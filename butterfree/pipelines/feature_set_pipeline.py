"""FeatureSetPipeline entity."""
from typing import List

from butterfree.clients import SparkClient
from butterfree.dataframe_service import repartition_sort_df
from butterfree.extract import Source
from butterfree.load import Sink
from butterfree.transform import FeatureSet


class FeatureSetPipeline:
    """Defines a ETL pipeline for the construction of a feature set.

    Attributes:
        source: source of the data, the entry point of the pipeline.
        feature_set: feature set composed by features and context metadata.
        sink: sink used to write the output dataframe in the desired locations.
        spark_client: client used to access Spark connection.

    Example:
        This an example regarding the feature set pipeline definition. All
        sources, feature set (and its features) and writers are defined.

        >>> import os

        >>> from butterfree.pipelines import FeatureSetPipeline
        >>> from butterfree.constants.columns import TIMESTAMP_COLUMN
        >>> from butterfree.configs.db import S3Config
        >>> from butterfree.extract import Source
        >>> from butterfree.extract.readers import TableReader
        >>> from butterfree.transform import FeatureSet
        >>> from butterfree.transform.features import (
        ...     Feature,
        ...     KeyFeature,
        ...     TimestampFeature,
        ...)
        >>> from butterfree.transform.transformations import (
        ...     SparkFunctionTransform,
        ...     CustomTransform,
        ... )
        >>> from butterfree.load import Sink
        >>> from butterfree.load.writers import HistoricalFeatureStoreWriter
        >>> import pyspark.sql.functions as F

        >>> def divide(df, fs, column1, column2):
        ...     name = fs.get_output_columns()[0]
        ...     df = df.withColumn(name, F.col(column1) / F.col(column2))
        ...     return df

        >>> pipeline = FeatureSetPipeline(
        ...    source=Source(
        ...        readers=[
        ...            TableReader(
        ...                id="table_reader_id",
        ...                database="table_reader_db",
        ...                table="table_reader_table",
        ...            ),
        ...        ],
        ...        query=f"select * from table_reader_id ",
        ...    ),
        ...    feature_set=FeatureSet(
        ...        name="feature_set",
        ...        entity="entity",
        ...        description="description",
        ...        features=[
        ...            Feature(
        ...                name="feature1",
        ...                description="test",
        ...            transformation=SparkFunctionTransform(
        ...                 functions=[F.avg, F.stddev_pop]
        ...             ).with_window(
        ...                 partition_by="id",
        ...                 order_by=TIMESTAMP_COLUMN,
        ...                 mode="fixed_windows",
        ...                 window_definition=["2 minutes", "15 minutes"],
        ...             ),
        ...            ),
        ...            Feature(
        ...                name="divided_feature",
        ...                description="unit test",
        ...                transformation=CustomTransform(
        ...                    transformer=divide,
        ...                    column1="feature1",
        ...                    column2="feature2",
        ...                ),
        ...            ),
        ...        ],
        ...        keys=[
        ...            KeyFeature(
        ...                name="id",
        ...                description="The user's Main ID or device ID"
        ...            )
        ...        ],
        ...        timestamp=TimestampFeature(),
        ...    ),
        ...    sink=Sink(
        ...         writers=[
        ...            HistoricalFeatureStoreWriter(
        ...                db_config=S3Config(
        ...                    database="db",
        ...                    format_="parquet",
        ...                    path=os.path.join(
        ...                        os.path.dirname(os.path.abspath(__file__))
        ...                    ),
        ...                ),
        ...            )
        ...        ],
        ...    ),
        ...)

        >>> pipeline.run()

        This last method (run) will execute the pipeline flow, it'll read from
        the defined sources, compute all the transformations and save the data
        to the specified locations.

    """

    def __init__(
        self,
        source: Source,
        feature_set: FeatureSet,
        sink: Sink,
        spark_client: SparkClient = None,
    ):
        self.source = source
        self.feature_set = feature_set
        self.sink = sink
        self.spark_client = spark_client

    @property
    def source(self) -> Source:
        """Source of the data, the entry point of the pipeline."""
        return self._source

    @source.setter
    def source(self, source: Source):
        if not isinstance(source, Source):
            raise ValueError("source must be a Source instance")
        self._source = source

    @property
    def feature_set(self) -> FeatureSet:
        """Feature set composed by features and context metadata."""
        return self._feature_set

    @feature_set.setter
    def feature_set(self, feature_set: FeatureSet):
        if not isinstance(feature_set, FeatureSet):
            raise ValueError("feature_set must be a FeatureSet instance")
        self._feature_set = feature_set

    @property
    def sink(self):
        """Sink used to write the output dataframe in the desired locations."""
        return self._sink

    @sink.setter
    def sink(self, sink: Sink):
        if not isinstance(sink, Sink):
            raise ValueError("sink must be a Sink instance")
        self._sink = sink

    @property
    def spark_client(self):
        """Client used to access Spark connection."""
        return self._spark_client

    @spark_client.setter
    def spark_client(self, spark_client: SparkClient):
        self._spark_client = spark_client or SparkClient()
        if not isinstance(self._spark_client, SparkClient):
            raise ValueError("spark_client must be a SparkClient instance")

    def run(
        self,
        end_date: str = None,
        partition_by: List[str] = None,
        order_by: List[str] = None,
        num_processors: int = None,
    ):
        """Runs the defined feature set pipeline.

        The pipeline consists in the following steps:
        - Constructs the input dataframe from the data source.
        - Construct the feature set dataframe using the defined Features.
        - Load the data to the configured sink locations.

        It's important to notice, however, that both parameters partition_by
        and num_processors are WIP, we intend to enhance their functionality
        soon. Use only if strictly necessary.

        """
        dataframe = self.source.construct(client=self.spark_client)

        if partition_by:
            order_by = order_by or partition_by
            dataframe = repartition_sort_df(
                dataframe, partition_by, order_by, num_processors
            )

        dataframe = self.feature_set.construct(
            dataframe=dataframe,
            client=self.spark_client,
            end_date=end_date,
            num_processors=num_processors,
        )

        self.sink.flush(
            dataframe=dataframe,
            feature_set=self.feature_set,
            spark_client=self.spark_client,
        )

        if not dataframe.isStreaming:
            self.sink.validate(
                dataframe=dataframe,
                feature_set=self.feature_set,
                spark_client=self.spark_client,
            )
