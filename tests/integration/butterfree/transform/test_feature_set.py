from pyspark.sql import functions as F

from butterfree.clients import SparkClient
from butterfree.constants import DataType
from butterfree.constants.columns import TIMESTAMP_COLUMN
from butterfree.testing.dataframe import assert_dataframe_equality
from butterfree.transform import FeatureSet
from butterfree.transform.features import Feature, KeyFeature, TimestampFeature
from butterfree.transform.transformations import CustomTransform, SparkFunctionTransform
from butterfree.transform.utils import Function


def divide(df, fs, column1, column2):
    name = fs.get_output_columns()[0]
    df = df.withColumn(name, F.col(column1) / F.col(column2))
    return df


class TestFeatureSet:
    def test_construct(
        self, feature_set_dataframe, fixed_windows_output_feature_set_dataframe
    ):
        # given

        spark_client = SparkClient()

        # arrange

        feature_set = FeatureSet(
            name="feature_set",
            entity="entity",
            description="description",
            features=[
                Feature(
                    name="feature1",
                    description="test",
                    transformation=SparkFunctionTransform(
                        functions=[
                            Function(F.avg, DataType.FLOAT),
                            Function(F.stddev_pop, DataType.FLOAT),
                        ]
                    ).with_window(
                        partition_by="id",
                        order_by=TIMESTAMP_COLUMN,
                        mode="fixed_windows",
                        window_definition=["2 minutes", "15 minutes"],
                    ),
                ),
                Feature(
                    name="divided_feature",
                    description="unit test",
                    dtype=DataType.FLOAT,
                    transformation=CustomTransform(
                        transformer=divide, column1="feature1", column2="feature2",
                    ),
                ),
            ],
            keys=[
                KeyFeature(
                    name="id",
                    description="The user's Main ID or device ID",
                    dtype=DataType.INTEGER,
                )
            ],
            timestamp=TimestampFeature(),
        )

        output_df = (
            feature_set.construct(feature_set_dataframe, client=spark_client)
            .orderBy(feature_set.timestamp_column)
            .select(feature_set.columns)
        )

        target_df = fixed_windows_output_feature_set_dataframe.orderBy(
            feature_set.timestamp_column
        ).select(feature_set.columns)

        # assert
        assert_dataframe_equality(output_df, target_df)
