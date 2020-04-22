import pytest
from pyspark.sql import functions

from butterfree.core.constants.data_type import DataType
from butterfree.core.transform.features import Feature
from butterfree.core.transform.transformations import AggregatedTransform


class TestAggregatedTransform:
    def test_feature_transform(self, feature_set_dataframe, target_df_agg):
        test_feature = Feature(
            name="feature1",
            description="unit test",
            dtype=DataType.BIGINT,
            transformation=AggregatedTransform(functions=["avg", "stddev_pop"]),
        )

        # aggregated feature transform won't run transformations
        # and depends on the feature set
        with pytest.raises(NotImplementedError):
            _ = test_feature.transform(feature_set_dataframe)

    def test_output_columns(self):
        test_feature = Feature(
            name="feature1",
            description="unit test",
            dtype=DataType.BIGINT,
            transformation=AggregatedTransform(functions=["avg", "stddev_pop"]),
        )

        df_columns = test_feature.get_output_columns()

        assert all(
            [
                a == b
                for a, b in zip(df_columns, ["feature1__avg", "feature1__stddev_pop"],)
            ]
        )

    def test_unsupported_aggregation(self, feature_set_dataframe):
        with pytest.raises(KeyError):
            Feature(
                name="feature1",
                description="unit test",
                dtype=DataType.BIGINT,
                transformation=AggregatedTransform(functions=["median"]),
            )

    def test_blank_aggregation(self, feature_set_dataframe):
        with pytest.raises(ValueError, match="Aggregations must not be empty."):
            Feature(
                name="feature1",
                description="unit test",
                dtype=DataType.BIGINT,
                transformation=AggregatedTransform(functions=[]),
            )

    def test_aggregations_with_filter_expression(self, spark_context):
        # arrange
        test_feature = Feature(
            name="feature_with_filter",
            description="unit test",
            dtype=DataType.BIGINT,
            transformation=AggregatedTransform(
                functions=["avg", "min", "max"], filter_expression="type = 'a'"
            ),
            from_column="feature",
        )
        target_aggregations = [
            agg(functions.when(functions.expr("type = 'a'"), functions.col("feature")))
            for agg in [functions.avg, functions.min, functions.max]
        ]

        # act
        output_aggregations = test_feature.transformation.aggregations

        # assert

        # cast to string to compare the columns definitions because direct column
        # comparison was not working
        assert str(target_aggregations) == str(output_aggregations)
