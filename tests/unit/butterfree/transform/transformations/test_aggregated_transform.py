from functools import partial

import pytest
from pyspark.sql import functions

from butterfree.constants import DataType
from butterfree.transform.features import Feature
from butterfree.transform.transformations import AggregatedTransform
from butterfree.transform.utils import Function, Window


class TestAggregatedTransform:
    def test_feature_transform(self, feature_set_dataframe, target_df_agg):
        test_feature = Feature(
            name="feature1",
            description="unit test",
            transformation=AggregatedTransform(
                functions=[
                    Function(functions.avg, DataType.DOUBLE),
                    Function(functions.stddev_pop, DataType.DOUBLE),
                ]
            ),
        )

        # aggregated feature transform won't run transformations
        # and depends on the feature set
        with pytest.raises(NotImplementedError):
            _ = test_feature.transform(feature_set_dataframe)

    def test_output_columns(self):
        test_feature = Feature(
            name="feature1",
            description="unit test",
            transformation=AggregatedTransform(
                functions=[
                    Function(functions.avg, DataType.DOUBLE),
                    Function(functions.stddev_pop, DataType.DOUBLE),
                ]
            ),
        )

        df_columns = test_feature.get_output_columns()

        assert all(
            [
                a == b
                for a, b in zip(
                    df_columns,
                    ["feature1__avg", "feature1__stddev_pop"],
                )
            ]
        )

    def test_unsupported_aggregation(self, feature_set_dataframe):
        with pytest.raises(TypeError):
            Feature(
                name="feature1",
                description="unit test",
                transformation=AggregatedTransform(
                    functions=[Function("median", DataType.DOUBLE)]
                ),
            )

    def test_blank_aggregation(self, feature_set_dataframe):
        with pytest.raises(ValueError):
            Feature(
                name="feature1",
                description="unit test",
                transformation=AggregatedTransform(
                    functions=[Function(func=None, data_type="")]
                ),
            )

    def test_aggregations_with_filter_expression(self, spark_context):
        # arrange
        test_feature = Feature(
            name="feature_with_filter",
            description="unit test",
            transformation=AggregatedTransform(
                functions=[
                    Function(functions.avg, DataType.DOUBLE),
                    Function(functions.min, DataType.DOUBLE),
                    Function(functions.max, DataType.DOUBLE),
                ],
                filter_expression="type = 'a'",
            ),
            from_column="feature",
        )
        target_aggregations = [
            agg(functions.when(functions.expr("type = 'a'"), functions.col("feature")))
            for agg in [functions.avg, functions.min, functions.max]
        ]

        # act
        output_aggregations = [
            agg.function for agg in test_feature.transformation.aggregations
        ]

        # assert

        # cast to string to compare the columns definitions because direct column
        # comparison was not working
        assert str(target_aggregations) == str(output_aggregations)

    def test_anonymous_function(self):
        with pytest.raises(
            AttributeError,
            match="Anonymous functions are not supported on AggregatedTransform.",
        ):
            Feature(
                name="feature1",
                description="unit test",
                transformation=AggregatedTransform(
                    functions=[
                        Function(
                            func=partial(functions.count), data_type=DataType.INTEGER
                        )
                    ]
                ),
            ).get_output_columns()

    def test_get_names_and_types(self):
        # arrange
        feature = Feature(
            name="feature",
            description="unit test",
            transformation=AggregatedTransform(
                functions=[
                    Function(functions.avg, DataType.DOUBLE),
                    Function(functions.stddev_pop, DataType.DOUBLE),
                ]
            ),
            from_column="dummy_column",
        )
        # act
        names_and_types = feature.transformation.get_names_and_types()

        # assert
        assert names_and_types == [
            ("feature__avg", "DOUBLE"),
            ("feature__stddev_pop", "DOUBLE"),
        ]

    def test_get_names_and_types_with_pivot(self):
        # arrange
        feature = Feature(
            name="feature",
            description="unit test",
            transformation=AggregatedTransform(
                functions=[
                    Function(functions.avg, DataType.DOUBLE),
                ]
            ),
            from_column="dummy_column",
        )
        # act
        names_and_types = feature.transformation.get_names_and_types(
            pivot_values=["characteristic_1", "characteristic_2"]
        )

        # assert
        assert names_and_types == [
            ("characteristic_1_feature__avg", "DOUBLE"),
            ("characteristic_2_feature__avg", "DOUBLE"),
        ]

    def test_get_names_and_types_with_window(self):
        # arrange
        feature = Feature(
            name="feature",
            description="unit test",
            transformation=AggregatedTransform(
                functions=[
                    Function(functions.avg, DataType.DOUBLE),
                ]
            ),
            from_column="dummy_column",
        )
        windows = [
            Window(
                partition_by="id",
                order_by="timestamp",
                mode="fixed_windows",
                window_definition="2 hours",
            ),
            Window(
                partition_by="id",
                order_by="timestamp",
                mode="fixed_windows",
                window_definition="3 hours",
            ),
        ]

        # act
        names_and_types = feature.transformation.get_names_and_types(windows=windows)

        # assert
        assert names_and_types == [
            ("feature__avg_over_2_hours_fixed_windows", "DOUBLE"),
            ("feature__avg_over_3_hours_fixed_windows", "DOUBLE"),
        ]

    def test_get_names_and_types_with_window_and_pivot(self):
        # arrange
        feature = Feature(
            name="feature",
            description="unit test",
            transformation=AggregatedTransform(
                functions=[
                    Function(functions.avg, DataType.DOUBLE),
                ]
            ),
            from_column="dummy_column",
        )
        windows = [
            Window(
                partition_by="id",
                order_by="timestamp",
                mode="fixed_windows",
                window_definition="2 hours",
            ),
        ]

        # act
        names_and_types = feature.transformation.get_names_and_types(
            pivot_values=["characteristic_1", "characteristic_2"], windows=windows
        )

        # assert
        assert names_and_types == [
            ("characteristic_1_feature__avg_over_2_hours_fixed_windows", "DOUBLE"),
            ("characteristic_2_feature__avg_over_2_hours_fixed_windows", "DOUBLE"),
        ]
