import pytest

from butterfree.core.transform import Feature, FeatureSet, TransformComponent


class TransformId(TransformComponent):
    def transform(self, dataframe):
        return dataframe.withColumn("id", dataframe.id.cast("int"))

    @property
    def output_columns(self):
        return ["id"]


class TransformTs(TransformComponent):
    def transform(self, dataframe):
        return dataframe.withColumn("ts", dataframe.ts.cast("string"))

    @property
    def output_columns(self):
        return ["ts"]


class TransformAdd100(TransformComponent):
    def transform(self, dataframe):
        return dataframe.withColumn("add", dataframe.value + 100)

    @property
    def output_columns(self):
        return ["add"]


class TransformSub100(TransformComponent):
    def transform(self, dataframe):
        return dataframe.withColumn("sub", dataframe.value - 100)

    @property
    def output_columns(self):
        return ["sub"]


FEATURE_ID = Feature(
    "id", "description", from_column="id", transformation=TransformId()
)


FEATURE_TS = Feature(
    "ts", "description", from_column="ts", transformation=TransformTs()
)


FEATURE_ADD100 = Feature(
    "add", "description", from_column="value", transformation=TransformAdd100()
)


FEATURE_SUB100 = Feature(
    "sub", "description", from_column="value", transformation=TransformSub100()
)


class TestFeatureSet:
    @pytest.mark.parametrize(
        "name, entity, description, key_columns, timestamp_column, feature_columns",
        [
            # invalid name
            (None, "entity", "description", ["id"], "ts", ["id", "ts"]),
            # invalid entity
            ("name", None, "description", ["id"], "ts", ["id", "ts"]),
            # invalid description
            ("name", "entity", None, ["id"], "ts", ["id", "ts"]),
            # key_column doesn't exists
            ("name", "entity", "description", ["invalid"], "ts", ["id", "ts"]),
            # empty key_columns
            ("name", "entity", "description", [], "ts", ["id", "ts"]),
            # invalid key_columns
            ("name", "entity", "description", None, ["ts"], ["id", "ts"]),
            # timestamp_column doesn't exist
            ("name", "entity", "description", ["id"], "invalid", ["id", "ts"]),
            # invalid timestamp_column
            ("name", "entity", "description", ["id"], None, ["id", "ts"]),
            # duplicated columns in features
            ("name", "entity", "description", ["id"], "ts", ["id", "id", "ts"]),
        ],
    )
    def test_cannot_instantiate(
        self,
        name,
        entity,
        description,
        key_columns,
        timestamp_column,
        feature_columns,
        mocked_feature,
    ):
        # arrange
        mocked_feature.get_output_columns.return_value = feature_columns

        # act and assert
        with pytest.raises(ValueError):
            FeatureSet(
                name,
                entity,
                description,
                [mocked_feature],
                key_columns,
                timestamp_column,
            )

    def test_getters(self, mocked_feature):
        # arrange
        mocked_feature.get_output_columns.return_value = ["id", "ts"]

        name = "name"
        entity = "entity"
        description = "description"
        features = [mocked_feature]
        key_columns = ["id"]
        timestamp_column = "ts"

        # act
        feature_set = FeatureSet(
            name, entity, description, features, key_columns, timestamp_column
        )

        # assert
        assert name == feature_set.name
        assert entity == feature_set.entity
        assert description == feature_set.description
        assert features == feature_set.features
        assert key_columns == feature_set.key_columns
        assert timestamp_column == feature_set.timestamp_column

    @pytest.mark.parametrize(
        "features", ["not a list", ["not", "a", "list", "of", "Features"]]
    )
    def test_features_invalid_params(self, features):
        # arrange
        name = "name"
        entity = "entity"
        description = "description"
        key_columns = ["id"]
        timestamp_column = "ts"

        # act and assert
        with pytest.raises(ValueError):
            FeatureSet(
                name, entity, description, features, key_columns, timestamp_column,
            )

    @pytest.mark.parametrize(
        "input_data, features, target_data",
        [
            (
                [{"id": "1", "ts": 0, "value": 100}],  # input data
                [FEATURE_ID, FEATURE_TS, FEATURE_ADD100],  # features
                [{"id": 1, "ts": "0", "add": 200}],  # target_Data
            ),
            (
                [{"id": "1", "ts": 0, "value": 100}],  # input data
                [FEATURE_ID, FEATURE_TS, FEATURE_SUB100],  # features
                [{"id": 1, "ts": "0", "sub": 0}],  # target_Data
            ),
            (
                [{"id": "1", "ts": 0, "value": 100}],  # input data
                [FEATURE_ID, FEATURE_TS, FEATURE_ADD100, FEATURE_SUB100],  # features
                [{"id": 1, "ts": "0", "add": 200, "sub": 0}],  # target_Data
            ),
            (
                [{"id": "1", "ts": 0, "value": 100, "unused_column": 0}],  # input data
                [FEATURE_ID, FEATURE_TS, FEATURE_ADD100, FEATURE_SUB100],  # features
                [{"id": 1, "ts": "0", "add": 200, "sub": 0}],  # target_Data
            ),
        ],
    )
    def test_construct(self, input_data, features, target_data, base_spark):
        # arrange
        sc, spark = base_spark

        input_df = spark.read.json(sc.parallelize(input_data, 1))
        target_df = spark.read.json(sc.parallelize(target_data, 1))

        feature_set = FeatureSet(
            "name",
            "entity",
            "description",
            features,
            key_columns=["id"],
            timestamp_column="ts",
        )

        # act
        result_df = feature_set.construct(input_df)
        col_names = result_df.schema.names

        # assert
        print("input_df:", input_df.collect())
        print("target_df:", target_df.select(col_names).collect())
        print("result_df:", result_df.select(col_names).collect())
        assert (
            target_df.select(col_names).collect()
            == result_df.select(col_names).collect()
        )

    def test_construct_invalid_params(self, mocked_feature):
        # arrange
        mocked_feature.get_output_columns.return_value = ["id", "ts"]

        name = "name"
        entity = "entity"
        description = "description"
        key_columns = ["id"]
        timestamp_column = "ts"

        feature_set = FeatureSet(
            name, entity, description, [mocked_feature], key_columns, timestamp_column,
        )

        # act and assert
        with pytest.raises(ValueError):
            feature_set.construct("not a df")
