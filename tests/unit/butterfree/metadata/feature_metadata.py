import pytest
from pyspark.sql import functions as F

from butterfree.constants import DataType
from butterfree.metadata.feature_metadata import FeatureMetadata
from butterfree.transform.features import Feature
from butterfree.transform.transformations import (
    CustomTransform,
    SparkFunctionTransform,
    SQLExpressionTransform,
    StackTransform,
)
from butterfree.transform.transformations.h3_transform import H3HashTransform
from butterfree.transform.utils import Function


def divide(df, parent_feature, column1, column2):
    name = parent_feature.get_output_columns()[0]
    df = df.withColumn(name, F.col(column1) / F.col(column2))
    return df


transformations_params = [
    (
        SQLExpressionTransform(expression="feature1/feature2"),
        DataType.FLOAT,
        [{"name": "feature", "data_type": "FLOAT", "primary_key": False}],
    ),
    (
        CustomTransform(transformer=divide, column1="feature1", column2="feature2"),
        DataType.DOUBLE,
        [{"name": "feature", "data_type": "DOUBLE", "primary_key": False}],
    ),
    (
        H3HashTransform(h3_resolutions=[6, 7], lat_column="lat", lng_column="lng"),
        DataType.STRING,
        [
            {
                "name": "lat_lng__h3_hash__6",
                "data_type": "STRING",
                "primary_key": False,
            },
            {
                "name": "lat_lng__h3_hash__7",
                "data_type": "STRING",
                "primary_key": False,
            },
        ],
    ),
    (
        SparkFunctionTransform(functions=[Function(F.cos, DataType.DOUBLE)]),
        None,  # dtype is None for SparkFunctionTransform
        [{"name": "feature__cos", "data_type": "DOUBLE", "primary_key": False}],
    ),
    (
        StackTransform("id_a", "id_b"),
        DataType.INTEGER,
        [{"name": "feature", "data_type": "INTEGER", "primary_key": False}],
    ),
]


class TestFeatureMetadata:
    @pytest.mark.parametrize(
        "transformation, dtype, expected_metadata", transformations_params
    )
    def test_build_metadata_with_transformation(
        self, transformation, dtype, expected_metadata
    ):
        # arrange
        feature = Feature(
            name="feature",
            description="unit test",
            dtype=dtype,
            transformation=transformation,
        )
        # act
        metadata = feature.build_metadata()

        # assert
        assert isinstance(metadata, list)
        assert len(metadata) == len(expected_metadata)
        for i, meta in enumerate(metadata):
            assert isinstance(meta, FeatureMetadata)
            assert meta.name == expected_metadata[i]["name"]
            assert meta.data_type == expected_metadata[i]["data_type"]
            assert meta.primary_key == expected_metadata[i]["primary_key"]
            assert meta.description == "unit test"

    def test_build_metadata_without_transformation(self):
        # arrange
        feature = Feature(
            name="feature",
            description="unit test",
            dtype=DataType.BIGINT,
        )
        # act
        metadata = feature.build_metadata()

        # assert
        assert isinstance(metadata, list)
        assert len(metadata) == 1
        assert isinstance(metadata[0], FeatureMetadata)
        assert metadata[0].name == "feature"
        assert metadata[0].data_type == "BIGINT"
        assert metadata[0].primary_key is False
        assert metadata[0].description == "unit test"
