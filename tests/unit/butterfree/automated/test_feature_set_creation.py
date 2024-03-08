import unittest
from unittest.mock import MagicMock

from butterfree.automated.feature_set_creation import FeatureSetCreation


class TestFeatureSetCreation(unittest.TestCase):
    def setUp(self):
        self.feature_set_creation = FeatureSetCreation()

    def test_get_features_with_regex(self):
        sql_query = "SELECT column1, column2 FROM table1"
        expected_features = ["column1", "column2"]

        features = self.feature_set_creation._get_features_with_regex(sql_query)

        self.assertEqual(features, expected_features)

    def test_get_data_type(self):
        field_name = "column1"
        df_mock = MagicMock()
        df_mock.schema.jsonValue.return_value = {
            "fields": [{"name": "column1", "type": "string"}]
        }

        data_type = self.feature_set_creation._get_data_type(field_name, df_mock)

        self.assertEqual(data_type, ".STRING")
