from typing import List

from butterfree.core.transform import FeatureSet
from butterfree.core.writer.writer import Writer


class Sink:
    def __init__(
        self,
        writers: List[Writer],
    ):
        self.writers = writers

    def validate(self, feature_set: FeatureSet):
        for writer in self.writers:
            writer.validate(feature_set=feature_set)

    def flush(self, feature_set: FeatureSet):
        if self.validate(feature_set):
            for writer in self.writers:
                writer.write(feature_set=feature_set)
        else:
            raise ValueError("Dataframe is invalid.")
