from butterfree.clients import SparkClient
from butterfree.extract import Source


class TestSource:
    def test_construct(self, mocker, target_df):
        # given
        spark_client = SparkClient()

        reader_id = "a_source"
        reader = mocker.stub(reader_id)
        reader.build = mocker.stub("build")
        reader.build.side_effect = target_df.createOrReplaceTempView(reader_id)

        # when
        source_selector = Source(
            readers=[reader],
            query=f"select * from {reader_id}",  # noqa
        )

        result_df = source_selector.construct(spark_client)

        assert result_df.collect() == target_df.collect()

    def test_is_cached(self, mocker, target_df):
        # given
        spark_client = SparkClient()

        reader_id = "a_source"
        reader = mocker.stub(reader_id)
        reader.build = mocker.stub("build")
        reader.build.side_effect = target_df.createOrReplaceTempView(reader_id)

        # when
        source_selector = Source(
            readers=[reader],
            query=f"select * from {reader_id}",  # noqa
        )

        result_df = source_selector.construct(spark_client)

        assert result_df.is_cached

    def test_jinja_templating(self, mocker, target_df):
        # given
        spark_client = SparkClient()
        mocker.patch.object(spark_client, 'sql', return_value=target_df)

        reader_id = "a_source"
        reader = mocker.stub(reader_id)
        reader.build = mocker.stub("build")
        reader.build.side_effect = target_df.createOrReplaceTempView(reader_id)

        start_date = "2024-01-01"
        end_date = "2024-01-31"
        template_query = """
            SELECT *
            FROM table
            WHERE date_column BETWEEN '{{ start_date }}' AND '{{ end_date }}'
        """

        # when
        source_selector = Source(
            readers=[reader],
            query=template_query,
        )

        result_df = source_selector.construct(
            spark_client,
            start_date=start_date,
            end_date=end_date,
        )

        # then
        expected_query = f"""
            SELECT *
            FROM table
            WHERE date_column BETWEEN '{start_date}' AND '{end_date}'
        """
        spark_client.sql.assert_called_once_with(expected_query)
        assert result_df.collect() == target_df.collect()
