from unittest.mock import Mock

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, FloatType, LongType, TimestampType

from butterfree.clients import SparkClient
from butterfree.constants import DataType
from butterfree.testing.dataframe import assert_dataframe_equality
from butterfree.transform import FeatureSet
from butterfree.transform.features import Feature
from butterfree.transform.transformations import (
    AggregatedTransform,
    SparkFunctionTransform,
    SQLExpressionTransform,
)
from butterfree.transform.utils import Function
from tests.unit.butterfree.transform.conftest import (
    feature_add,
    feature_divide,
    key_id,
    timestamp_c,
)


class TestFeatureSet:
    @pytest.mark.parametrize(
        "name, entity, description, keys, timestamp, features",
        [
            # invalid name
            (
                None,
                "entity",
                "description",
                [key_id],
                timestamp_c,
                [feature_add, feature_divide],
            ),
            # invalid entity
            (
                "name",
                None,
                "description",
                [key_id],
                timestamp_c,
                [feature_add, feature_divide],
            ),
            # invalid description
            (
                "name",
                "entity",
                None,
                [key_id],
                timestamp_c,
                [feature_add, feature_divide],
            ),
            # invalid keys
            (
                "name",
                "entity",
                "description",
                [None],
                timestamp_c,
                [feature_add, feature_divide],
            ),
            (
                "name",
                "entity",
                "description",
                [key_id],
                None,
                [feature_add, feature_divide],
            ),
            (
                "name",
                "entity",
                "description",
                [key_id],
                timestamp_c,
                [None],
            ),
        ],
    )
    def test_cannot_instantiate(
        self, name, entity, description, keys, timestamp, features
    ):
        # act and assert
        with pytest.raises(ValueError):
            FeatureSet(name, entity, description, keys, timestamp, features)

    def test_getters(self, feature_add, feature_divide, key_id, timestamp_c):
        # arrange
        name = "name"
        entity = "entity"
        description = "description"

        # act
        feature_set = FeatureSet(
            name,
            entity,
            description,
            [key_id],
            timestamp_c,
            [feature_add, feature_divide],
        )

        # assert
        assert name == feature_set.name
        assert entity == feature_set.entity
        assert description == feature_set.description
        assert [key_id] == feature_set.keys
        assert timestamp_c == feature_set.timestamp
        assert [feature_add, feature_divide] == feature_set.features
        assert "timestamp" == feature_set.timestamp_column
        assert ["id"] == feature_set.keys_columns

    def test_duplicate_keys(self, feature_add, feature_divide, key_id, timestamp_c):
        # arrange
        name = "name"
        entity = "entity"
        description = "description"

        # act and assert
        with pytest.raises(KeyError):
            _ = FeatureSet(
                name,
                entity,
                description,
                [key_id, key_id],
                timestamp_c,
                [feature_add, feature_divide],
            )

    def test_duplicate_features(self, feature_add, key_id, timestamp_c):
        # arrange
        name = "name"
        entity = "entity"
        description = "description"

        # act and assert
        with pytest.raises(KeyError):
            _ = FeatureSet(
                name,
                entity,
                description,
                [key_id],
                timestamp_c,
                [feature_add, feature_add],
            )

    def test_multiple_timestamps(self, feature_add, key_id, timestamp_c):
        # arrange
        name = "name"
        entity = "entity"
        description = "description"
        timestamp_c.get_output_columns = Mock(return_value=["timestamp1", "timestamp2"])

        # act and assert
        with pytest.raises(ValueError):
            _ = FeatureSet(
                name, entity, description, [key_id], timestamp_c, [feature_add]
            )

    def test_columns(self, key_id, timestamp_c, feature_add, feature_divide):
        # arrange
        name = "name"
        entity = "entity"
        description = "description"

        # act
        fs = FeatureSet(
            name,
            entity,
            description,
            [key_id],
            timestamp_c,
            [feature_add, feature_divide],
        )
        out_columns = fs.columns

        # assert
        assert (
            out_columns
            == key_id.get_output_columns()
            + timestamp_c.get_output_columns()
            + feature_add.get_output_columns()
            + feature_divide.get_output_columns()
        )

    def test_construct(
        self,
        dataframe,
        feature_set_dataframe,
        key_id,
        timestamp_c,
        feature_add,
        feature_divide,
    ):
        spark_client = Mock()

        # arrange
        feature_set = FeatureSet(
            name="name",
            entity="entity",
            description="description",
            keys=[key_id],
            timestamp=timestamp_c,
            features=[feature_add, feature_divide],
        )

        # act
        result_df = feature_set.construct(dataframe, spark_client)
        result_columns = result_df.columns

        # assert
        assert (
            result_columns
            == key_id.get_output_columns()
            + timestamp_c.get_output_columns()
            + feature_add.get_output_columns()
            + feature_divide.get_output_columns()
        )
        assert_dataframe_equality(result_df, feature_set_dataframe)
        assert result_df.is_cached

    def test_construct_transformations(
        self,
        dataframe,
        feature_set_dataframe,
        key_id,
        timestamp_c,
        feature_add,
        feature_divide,
    ):
        spark_client = Mock()

        # arrange
        feature_set = FeatureSet(
            name="name",
            entity="entity",
            description="description",
            keys=[key_id],
            timestamp=timestamp_c,
            features=[feature_add, feature_divide],
        )

        # act
        result_df = feature_set.construct(dataframe, spark_client)

        # assert
        assert_dataframe_equality(result_df, feature_set_dataframe)

    def test__get_features_columns(self):
        # arrange
        feature_1 = Feature("feature1", "description", DataType.FLOAT)
        feature_1.get_output_columns = Mock(return_value=["col_a", "col_b"])

        feature_2 = Feature("feature2", "description", DataType.FLOAT)
        feature_2.get_output_columns = Mock(return_value=["col_c"])

        feature_3 = Feature("feature3", "description", DataType.FLOAT)
        feature_3.get_output_columns = Mock(return_value=["col_d"])

        target_features_columns = ["col_a", "col_b", "col_c", "col_d"]

        # act
        result_features_columns = FeatureSet._get_features_columns(
            feature_1, feature_2, feature_3
        )

        # assert
        assert target_features_columns == result_features_columns

    def test_filtering(
        self,
        filtering_dataframe,
        key_id,
        timestamp_c,
        feature1,
        feature2,
        feature3,
        output_filtering_dataframe,
    ):
        spark_client = Mock()

        # arrange
        feature_set = FeatureSet(
            name="name",
            entity="entity",
            description="description",
            keys=[key_id],
            timestamp=timestamp_c,
            features=[feature1, feature2, feature3],
        )

        # act
        result_df = (
            feature_set.construct(filtering_dataframe, spark_client)
            .orderBy("timestamp")
            .collect()
        )

        # assert
        assert (
            result_df
            == output_filtering_dataframe.orderBy("timestamp")
            .select(feature_set.columns)
            .collect()
        )

    def test_feature_set_with_invalid_feature(self, key_id, timestamp_c, dataframe):
        spark_client = SparkClient()
        with pytest.raises(ValueError):
            FeatureSet(
                name="name",
                entity="entity",
                description="description",
                features=[
                    Feature(
                        name="feature1",
                        description="test",
                        transformation=AggregatedTransform(
                            functions=[Function(F.avg, DataType.FLOAT)]
                        ),
                    ),
                ],
                keys=[key_id],
                timestamp=timestamp_c,
            ).construct(dataframe, spark_client)

    def test_get_schema(self, feature_set):
        expected_schema = [
            {
                "column_name": "id",
                "type": LongType(),
                "primary_key": True,
            },
            {
                "column_name": "timestamp",
                "type": TimestampType(),
                "primary_key": False,
            },
            {
                "column_name": "feature1__avg_over_2_minutes_fixed_windows",
                "type": FloatType(),
                "primary_key": False,
            },
            {
                "column_name": "feature1__avg_over_15_minutes_fixed_windows",
                "type": FloatType(),
                "primary_key": False,
            },
            {
                "column_name": "feature1__stddev_pop_over_2_minutes_fixed_windows",
                "type": DoubleType(),
                "primary_key": False,
            },
            {
                "column_name": "feature1__stddev_pop_over_15_minutes_fixed_windows",
                "type": DoubleType(),
                "primary_key": False,
            },
        ]

        schema = feature_set.get_schema()

        assert schema == expected_schema

    def test_feature_without_datatype(self, key_id, timestamp_c, dataframe):
        spark_client = SparkClient()
        with pytest.raises(ValueError):
            FeatureSet(
                name="name",
                entity="entity",
                description="description",
                features=[
                    Feature(
                        name="feature1",
                        description="test",
                        transformation=SQLExpressionTransform(
                            expression="feature1 + a"
                        ),
                    ),
                ],
                keys=[key_id],
                timestamp=timestamp_c,
            ).construct(dataframe, spark_client)

    def test_define_start_date(self, feature_set):
        start_date = feature_set.define_start_date("2020-08-04")

        assert isinstance(start_date, str)
        assert start_date == "2020-08-04"

    def test_build_features_metadata_simple_feature(self, key_id, timestamp_c):
        feature = Feature(
            name="simple",
            description="dummy",
            dtype=DataType.DOUBLE,
        )

        # arrange
        feature_set = FeatureSet(
            name="name",
            entity="entity",
            description="description",
            keys=[key_id],
            timestamp=timestamp_c,
            features=[feature],
        )

        # act
        metadata = feature_set._build_features_metadata()

        # assert
        assert len(metadata) == 1
        assert metadata[0].name == "simple"
        assert metadata[0].data_type == "DOUBLE"
        assert metadata[0].primary_key is False
        assert metadata[0].description == "dummy"

    def test_build_features_metadata_spark_function_feature_no_window(
        self, key_id, timestamp_c
    ):
        # arrange
        feature_spark_function = Feature(
            name="feature_spark",
            description="spark function feature",
            transformation=SparkFunctionTransform(
                functions=[Function(F.avg, DataType.DOUBLE)]
            ),
        )

        feature_set = FeatureSet(
            name="name",
            entity="entity",
            description="description",
            keys=[key_id],
            timestamp=timestamp_c,
            features=[feature_spark_function],
        )

        # act
        metadata = feature_set._build_features_metadata()

        # assert
        assert len(metadata) == 1
        assert metadata[0].name == "feature_spark__avg"
        assert metadata[0].data_type == "DOUBLE"
        assert metadata[0].primary_key is False
        assert metadata[0].description == "spark function feature"

    def test_build_features_metadata_no_spark_function_feature_no_window(
        self, key_id, timestamp_c
    ):
        # arrange
        feature_sql_expression = Feature(
            name="feature_sql_expr",
            description="sql expression feature",
            transformation=SQLExpressionTransform(expression="avg(feature_a)"),
            dtype=DataType.DOUBLE,
        )

        feature_set = FeatureSet(
            name="name",
            entity="entity",
            description="description",
            keys=[key_id],
            timestamp=timestamp_c,
            features=[feature_sql_expression],
        )

        # act
        metadata = feature_set._build_features_metadata()

        # assert
        assert len(metadata) == 1
        assert metadata[0].name == "feature_sql_expr"
        assert metadata[0].data_type == "DOUBLE"
        assert metadata[0].primary_key is False
        assert metadata[0].description == "sql expression feature"

    def test_build_features_metadata_spark_function_feature_with_window(
        self, key_id, timestamp_c
    ):
        """
        Using a dictionary with names as keys allows direct access to each metadata entry # noqa: E501
        without making assumptions about their position in the list, which could change
        between runs because of spark parallel processing.
        """
        # arrange
        feature_spark_window = Feature(
            name="feature_spark_window",
            description="spark function window feature",
            transformation=SparkFunctionTransform(
                functions=[
                    Function(func=F.avg, data_type=DataType.DOUBLE),
                    Function(func=F.stddev_pop, data_type=DataType.DOUBLE),
                ]
            ).with_window(
                partition_by="id",
                window_definition=["5 events", "15 events"],
                mode="row_windows",
            ),
        )

        feature_set = FeatureSet(
            name="name",
            entity="entity",
            description="description",
            keys=[key_id],
            timestamp=timestamp_c,
            features=[feature_spark_window],
        )

        # act
        metadata = feature_set._build_features_metadata()

        # assert
        assert len(metadata) == 4  # 2 functions * 2 window definitions

        # Convert to a dictionary for easier lookup by name
        metadata_dict = {m.name: m for m in metadata}

        # Get each entry directly from the dictionary
        avg_5_events = metadata_dict.get(
            "feature_spark_window__avg_over_5_events_row_windows"
        )
        stddev_5_events = metadata_dict.get(
            "feature_spark_window__stddev_pop_over_5_events_row_windows"
        )
        avg_15_events = metadata_dict.get(
            "feature_spark_window__avg_over_15_events_row_windows"
        )
        stddev_15_events = metadata_dict.get(
            "feature_spark_window__stddev_pop_over_15_events_row_windows"
        )

        # Verify all entries exist
        assert avg_5_events is not None, "Missing avg_5_events metadata"
        assert stddev_5_events is not None, "Missing stddev_5_events metadata"
        assert avg_15_events is not None, "Missing avg_15_events metadata"
        assert stddev_15_events is not None, "Missing stddev_15_events metadata"

        # Assertions for F.avg over "5 events"
        assert (
            avg_5_events.name == "feature_spark_window__avg_over_5_events_row_windows"
        )
        assert avg_5_events.data_type == "DOUBLE"
        assert avg_5_events.primary_key is False
        assert avg_5_events.description == "spark function window feature"

        # Assertions for F.stddev_pop over "5 events"
        assert (
            stddev_5_events.name
            == "feature_spark_window__stddev_pop_over_5_events_row_windows"
        )
        assert stddev_5_events.data_type == "DOUBLE"
        assert stddev_5_events.primary_key is False
        assert stddev_5_events.description == "spark function window feature"

        # Assertions for F.avg over "15 events"
        assert (
            avg_15_events.name == "feature_spark_window__avg_over_15_events_row_windows"
        )
        assert avg_15_events.data_type == "DOUBLE"
        assert avg_15_events.primary_key is False
        assert avg_15_events.description == "spark function window feature"

        # Assertions for F.stddev_pop over "15 events"
        assert (
            stddev_15_events.name
            == "feature_spark_window__stddev_pop_over_15_events_row_windows"
        )
        assert stddev_15_events.data_type == "DOUBLE"
        assert stddev_15_events.primary_key is False
        assert stddev_15_events.description == "spark function window feature"
