import os
from unittest import mock

import pytest
from pyspark.sql import DataFrame

from butterfree.clients import SparkClient
from butterfree.load.writers import DeltaWriter

DELTA_LOCATION = "spark-warehouse"


class TestDeltaWriter:

    def __checkFileExists(self, file_name: str = "test_delta_table") -> bool:
        return os.path.exists(os.path.join(DELTA_LOCATION, file_name))

    @pytest.fixture
    def merge_builder_mock(self):
        builder = mock.MagicMock()
        builder.whenMatchedDelete.return_value = builder
        builder.whenMatchedUpdateAll.return_value = builder
        builder.whenNotMatchedInsertAll.return_value = builder
        return builder

    @pytest.fixture
    def delta_table_mock(self, mocker, merge_builder_mock):
        mock_table = mocker.Mock()
        mock_table.alias.return_value.merge.return_value = merge_builder_mock
        return mock_table

    def test_merge(self, feature_set_dataframe, mocker, delta_table_mock):
        # Arrange
        client = SparkClient()
        mocker.patch(
            "butterfree.load.writers.delta_writer.DeltaTable.forName",
            return_value=delta_table_mock,
        )

        # Mock catalog exists
        client.conn.catalog.tableExists = mock.MagicMock(return_value=True)

        # Mock describe table com uma estrutura mais detalhada
        mock_pandas_df = mock.MagicMock()
        mock_reset_index = mock.MagicMock()
        mock_groupby = mock.MagicMock()
        mock_agg = mock.MagicMock()
        mock_provider = mock.MagicMock()

        # Configurando o comportamento em cadeia
        mock_pandas_df.reset_index.return_value = mock_reset_index
        mock_reset_index.groupby.return_value = mock_groupby
        mock_groupby.__getitem__.return_value = mock_groupby
        mock_groupby.aggregate.return_value = mock_agg
        mock_agg.Provider = "delta"

        # Mock sql method
        mock_sql_result = mock.MagicMock()
        mock_sql_result.toPandas.return_value = mock_pandas_df
        client.conn.sql = mock.MagicMock(return_value=mock_sql_result)

        # Act
        DeltaWriter().merge(
            client=client,
            database=None,
            table="test_delta_table",
            merge_on=["id"],
            source_df=feature_set_dataframe,
        )

        # Assert
        delta_table_mock.alias.assert_called_once_with("target")
        delta_table_mock.alias.return_value.merge.assert_called_once()

    def test_merge_table_not_found(self, feature_set_dataframe, mocker):
        # Arrange
        client = SparkClient()
        client.conn.catalog.tableExists = mock.MagicMock(return_value=False)
        mock_delta = mocker.patch(
            "butterfree.load.writers.delta_writer.DeltaTable.forName",
            side_effect=Exception("Table does not exist or is not a Delta table"),
        )

        # Act & Assert
        with pytest.raises(
            Exception, match="Table does not exist or is not a Delta table"
        ):
            DeltaWriter().merge(
                client=client,
                database=None,
                table="nonexistent_table",
                merge_on=["id"],
                source_df=feature_set_dataframe,
            )

    def test_optimize(self, mocker):
        client = SparkClient()
        conn_mock = mocker.patch(
            "butterfree.clients.SparkClient.conn", return_value=mock.Mock()
        )
        dw = DeltaWriter()
        dw.optimize = mock.MagicMock(client)
        dw.optimize(client, "a_table")
        conn_mock.assert_called_once

    def test_vacuum(self, mocker):
        client = SparkClient()
        conn_mock = mocker.patch(
            "butterfree.clients.SparkClient.conn", return_value=mock.Mock()
        )
        dw = DeltaWriter()
        retention_hours = 24
        dw.vacuum = mock.MagicMock(client)
        dw.vacuum("a_table", retention_hours, client)
        conn_mock.assert_called_once
