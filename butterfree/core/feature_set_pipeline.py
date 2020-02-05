"""FeatureSetPipeline entity."""
from butterfree.core.client import SparkClient
from butterfree.core.reader import Source
from butterfree.core.transform import FeatureSet
from butterfree.core.writer import Sink


class FeatureSetPipeline:
    """Defines a ETL pipeline for the construction of a feature set.

    Attributes:
        source: source of the data, the entry point of the pipeline.
        feature_set: feature set composed by features and context metadata.
        sink: sink used to write the output dataframe in the desired locations.
        spark_client: client used to access Spark connection.
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

    def run(self):
        """Runs the defined feature set pipeline.

        The pipeline consists in the following steps:
        - Constructs the input dataframe from the data source.
        - Construct the feature set dataframe using the defined Features.
        - Load the data to the configured sink locations.

        """
        dataframe = self.source.construct(client=self.spark_client)
        dataframe = self.feature_set.construct(dataframe=dataframe)
        self.sink.flush(
            dataframe=dataframe,
            feature_set=self.feature_set,
            spark_client=self.spark_client,
        )
        self.sink.validate(
            dataframe=dataframe,
            feature_set=self.feature_set,
            spark_client=self.spark_client,
        )
