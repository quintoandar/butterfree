import json

from butterfree import (
    AggregatedTransform,
    FeatureSet,
    FeatureSetPipeline,
    FileReader,
    KafkaReader,
    SparkFunctionTransform,
    TableReader,
)


class Metadata:
    def __init__(self, feature_set: FeatureSetPipeline, save: bool = False):
        self.feature_set = feature_set
        self.save = save
        self._name = None
        self._desc_feature_set = None
        self._source = []
        self._sink = []
        self._features = []

    def _construct(self):
        self._name = self.feature_set.feature_set.name
        self._desc_feature_set = self.feature_set.feature_set.description

        source = []
        for reader in self.feature_set.source.readers:
            if isinstance(reader, TableReader):
                source.append((reader.__name__, f"{reader.database}.{reader.table}"))
            if isinstance(reader, FileReader):
                source.append((reader.__name__, reader.path))
            if isinstance(reader, KafkaReader):
                source.append((reader.__name__, reader.topic))

        self._source = source

        self._sink = [writer.__name__ for writer in self.feature_set.sink.writers]

        desc_feature = [
            feature.description for feature in self.feature_set.feature_set.keys
        ]
        desc_feature.append(self.feature_set.feature_set.timestamp.description)

        for feature in self.feature_set.feature_set.features:
            if isinstance(feature.transformation, SparkFunctionTransform):
                for _ in feature.transformation._windows:
                    desc_feature.append(feature.description)
            elif isinstance(feature.transformation, AggregatedTransform):
                pivot_values = self.feature_set.feature_set._pivot_values or [None]
                windows = self.feature_set.feature_set._windows or [None]
                for _ in range(len(pivot_values) * len(windows)):
                    desc_feature.append(feature.description)
            else:
                desc_feature.append(feature.description)

        schema = self.feature_set.feature_set.get_schema()

        self._features = [(column, desc) for column, desc in zip(schema, desc_feature)]

        return self

    def to_json(self):

        params = self._construct()

        lines = [
            {
                "feature_set": params._name,
                "description": params._desc_feature_set,
                "source": [{"reader": r, "location": l} for r, l in params._source],
                "sink": [{"writer": w} for w in params._sink],
                "features": [
                    {
                        "column_name": c["column_name"],
                        "data_type": str(c["type"]),
                        "description": desc,
                    }
                    for c, desc in params._features
                ],
            }
        ]

        if self.save:
            with open(f"{params._name}.json", "w") as outfile:
                json.dump(lines, outfile)
        else:
            return lines

    def to_markdown(self):
        params = self._construct()

        lines = f"""##Feature Set
- name: {params._name}
- description: {params._desc_feature_set}
- source: 
```json
{[{"reader": r, "location": l} for r, l in params._source]}
```
- sink:
```json
{[{"writer": w} for w in params._sink]}
```
- features:
```json
{[{"column_name": c["column_name"], "data_type": str(c["type"]), "description": desc} for
                             c, desc in params._features]}
```"""

        if self.save:
            with open(f"{params._name}.md", "w") as outfile:
                outfile.write(str(lines))
        else:
            return lines
