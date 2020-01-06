from typing import List

from quintoandar_butterfree.core.feature import Feature
from quintoandar_butterfree.core.output import Output
from quintoandar_butterfree.core.source import Source


class FeatureSet:
    def __init__(
        self, *, source: Source, features: List[Feature], outputs: List[Output]
    ):
        self._source = source
        self._features = features
        self._outputs = outputs

    def run(self):
        dataframe = self._source.fetch()
        feature_columns = []
        for feature in self._features:
            dataframe, computed_features = feature.compute(dataframe)
            feature_columns.extend(computed_features)
        dataframe = dataframe.select(*feature_columns)
        for output in self._outputs:
            output.write(dataframe)
