import os
from unittest import mock

import pytest

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

    def test_merge(self, feature_set_dataframe, merge_builder_mock):

        client = SparkClient()
        delta_writer = DeltaWriter()
        delta_writer.merge = mock.MagicMock()

        DeltaWriter().merge(
            client=client,
            database=None,
            table="test_delta_table",
            merge_on=["id"],
            source_df=feature_set_dataframe,
        )

        assert merge_builder_mock.execute.assert_called_once

        # Step 2
        source = client.conn.createDataFrame(
            [(1, "test3"), (2, "test4"), (3, "test5")], ["id", "feature"]
        )

        DeltaWriter().merge(
            client=client,
            database=None,
            table="test_delta_table",
            merge_on=["id"],
            source_df=source,
            when_not_matched_insert_condition=None,
            when_matched_update_condition="id > 2",
        )

        assert merge_builder_mock.execute.assert_called_once

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
