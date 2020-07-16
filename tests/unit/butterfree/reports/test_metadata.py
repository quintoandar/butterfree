from pyspark.sql import functions

from butterfree.constants import DataType
from butterfree.extract import Source
from butterfree.extract.readers import FileReader, TableReader
from butterfree.load import Sink
from butterfree.load.writers import (
    HistoricalFeatureStoreWriter,
    OnlineFeatureStoreWriter,
)
from butterfree.pipelines import FeatureSetPipeline
from butterfree.reports import Metadata
from butterfree.transform import FeatureSet
from butterfree.transform.features import Feature, KeyFeature, TimestampFeature
from butterfree.transform.transformations import SparkFunctionTransform
from butterfree.transform.utils import Function


class TestMetadata:
    def test_json(self):
        pipeline = FeatureSetPipeline(
            source=Source(
                readers=[
                    TableReader(id="source_a", database="db", table="table",),
                    FileReader(id="source_b", path="path", format="parquet",),
                ],
                query="select a.*, b.specific_feature "
                "from source_a left join source_b on a.id=b.id",
            ),
            feature_set=FeatureSet(
                name="feature_set",
                entity="entity",
                description="description",
                keys=[
                    KeyFeature(
                        name="user_id",
                        description="The user's Main ID or device ID",
                        dtype=DataType.INTEGER,
                    )
                ],
                timestamp=TimestampFeature(from_column="ts"),
                features=[
                    Feature(
                        name="page_viewed__rent_per_month",
                        description="Average of something.",
                        transformation=SparkFunctionTransform(
                            functions=[
                                Function(functions.avg, DataType.FLOAT),
                                Function(functions.stddev_pop, DataType.DOUBLE),
                            ],
                        ),
                    ),
                ],
            ),
            sink=Sink(
                writers=[
                    HistoricalFeatureStoreWriter(db_config=None),
                    OnlineFeatureStoreWriter(db_config=None),
                ],
            ),
        )

        target_json = [
            {
                "feature_set": "feature_set",
                "description": "description",
                "source": [
                    {"reader": "Table Reader", "location": "db.table"},
                    {"reader": "File Reader", "location": "path"},
                ],
                "sink": [
                    {"writer": "Historical Feature Store Writer"},
                    {"writer": "Online Feature Store Writer"},
                ],
                "features": [
                    {
                        "column_name": "user_id",
                        "data_type": "IntegerType",
                        "description": "The user's Main ID or device ID",
                    },
                    {
                        "column_name": "timestamp",
                        "data_type": "TimestampType",
                        "description": "Time tag for the state of all features.",
                    },
                    {
                        "column_name": "page_viewed__rent_per_month__avg",
                        "data_type": "FloatType",
                        "description": "Average of something.",
                    },
                    {
                        "column_name": "page_viewed__rent_per_month__stddev_pop",
                        "data_type": "DoubleType",
                        "description": "Average of something.",
                    },
                ],
            }
        ]

        info = Metadata(pipeline)
        json = info.to_json()
        assert json == target_json

    def test_markdown(self):
        pipeline = FeatureSetPipeline(
            source=Source(
                readers=[
                    TableReader(id="source_a", database="db", table="table",),
                    FileReader(id="source_b", path="path", format="parquet",),
                ],
                query="select a.*, b.specific_feature "
                "from source_a left join source_b on a.id=b.id",
            ),
            feature_set=FeatureSet(
                name="feature_set",
                entity="entity",
                description="description",
                keys=[
                    KeyFeature(
                        name="user_id",
                        description="The user's Main ID or device ID",
                        dtype=DataType.INTEGER,
                    )
                ],
                timestamp=TimestampFeature(from_column="ts"),
                features=[
                    Feature(
                        name="page_viewed__rent_per_month",
                        description="Average of something.",
                        transformation=SparkFunctionTransform(
                            functions=[
                                Function(functions.avg, DataType.FLOAT),
                                Function(functions.stddev_pop, DataType.DOUBLE),
                            ],
                        ),
                    ),
                ],
            ),
            sink=Sink(
                writers=[
                    HistoricalFeatureStoreWriter(db_config=None),
                    OnlineFeatureStoreWriter(db_config=None),
                ],
            ),
        )

        target_md = (
            "\n# Feature_set\n\n## Description\n\n\ndescription  \n\n"
            "## Feature Set Pipeline\n\n"
            "### Source\n\n|Reader|Location|\n| :---: | :---: |\n|"
            "Table Reader|db.table|\n|File Reader|path|\n\n"
            "### Sink\n\n|Writer|\n| :---: |\n|Historical Feature Store Writer"
            "|\n|Online Feature Store Writer|\n\n"
            "### Features\n\n|Column name|Data type|Description|\n| :---: | :---: "
            "| :---: |\n|user_id|IntegerType|"
            "The user's Main ID or device ID|\n|timestamp"
            "|TimestampType|Time tag for the state of all features.|\n|"
            "page_viewed__rent_per_month__avg|FloatType|"
            "Average of something.|\n|"
            "page_viewed__rent_per_month__stddev_pop|DoubleType|"
            "Average of something.|\n"
        )

        info = Metadata(pipeline)
        markdown = info.to_markdown()
        assert markdown == target_md
