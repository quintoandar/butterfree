from butterfree.load.processing import json_transform


class TestJsonTransform:
    def test_json_transformation(
        self, input_df, json_df,
    ):
        result_df = json_transform(dataframe=input_df)

        # assert
        assert sorted(result_df.collect()) == sorted(json_df.collect())
