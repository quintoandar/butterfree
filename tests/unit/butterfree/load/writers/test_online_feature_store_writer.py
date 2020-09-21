import pytest

from butterfree.load.writers import OnlineFeatureStoreWriter


class TestOnlineFeatureStoreWriter:
    def test_filter_latest(
        self, feature_set_dataframe, latest_feature_set_dataframe, cassandra_config
    ):
        # with
        writer = OnlineFeatureStoreWriter(cassandra_config)

        # when
        result_df = writer.filter_latest(feature_set_dataframe, id_columns=["id"])
        sort_columns = result_df.schema.fieldNames()

        # then
        assert sorted(
            latest_feature_set_dataframe.select(*sort_columns).collect()
        ) == sorted(result_df.select(*sort_columns).collect())

    def test_filter_latest_without_ts(
        self, feature_set_dataframe_without_ts, cassandra_config
    ):
        # with
        writer = OnlineFeatureStoreWriter(cassandra_config)

        # then
        with pytest.raises(KeyError):
            _ = writer.filter_latest(
                feature_set_dataframe_without_ts, id_columns=["id"]
            )

    def test_filter_latest_without_id_columns(
        self, feature_set_dataframe, cassandra_config
    ):
        # with
        writer = OnlineFeatureStoreWriter(cassandra_config)

        # then
        with pytest.raises(ValueError, match="must provide the unique identifiers"):
            _ = writer.filter_latest(feature_set_dataframe, id_columns=[])

        # then
        with pytest.raises(KeyError, match="not found"):
            _ = writer.filter_latest(
                feature_set_dataframe.drop("id"), id_columns=["id"]
            )

    def test_load(
        self,
        feature_set_dataframe,
        latest_feature_set_dataframe,
        cassandra_config,
        mocker,
        feature_set,
    ):
        # with
        spark_client = mocker.stub("spark_client")
        writer = OnlineFeatureStoreWriter(cassandra_config)

        # when
        result_df, db_config, options, database, table_name, partition_by = writer.load(
            feature_set, feature_set_dataframe, spark_client
        )

        assert sorted(latest_feature_set_dataframe.collect()) == sorted(
            result_df.collect()
        )
        assert writer.db_config == db_config
        # assert if all additional options got from db_config
        # are in the called args in write_dataframe
        assert all(
            item in options.items()
            for item in writer.db_config.get_options(table=feature_set.name).items()
        )

    def test_get_db_schema(self, cassandra_config, test_feature_set, expected_schema):
        writer = OnlineFeatureStoreWriter(cassandra_config)
        schema = writer.get_db_schema(test_feature_set)

        assert schema == expected_schema
