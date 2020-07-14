"""Write feature set metadata."""

import json

from mdutils import MdUtils

from butterfree import FeatureSetPipeline, FileReader, KafkaReader, TableReader
from butterfree.core.transform.aggregated_feature_set import AggregatedFeatureSet


class Metadata:
    """Generate metadata for feature set pipeline.

    Attributes:
        feature_set: object processed with feature set pipeline.
        save: bool value with default is False.
            When this value is True, it will generate a file.

    Example:

        >>> feature_set_pipeline = FeatureSetPipeline()
        >>> metadata = Metadata(feature_set_pipeline)
        >>> metadata.to_json()

    [
    {
        "feature_set": "feature_set",
        "description": "description",
        "source": [
            {
                "reader": "Table Reader",
                "location": "db.table"
            },
            {
                "reader": "File Reader",
                "location": "path"
            }
        ],
        "sink": [
            {
                "writer": "Historical Feature Store Writer"
            },
            {
                "writer": "Online Feature Store Writer"
            }
        ],
        "features": [
            {
                "column": {
                    "name": "user_id",
                    "data_type": "IntegerType"
                },
                "description": "The user's Main ID or device ID"
            },
            {
                "column": {
                    "name": "timestamp",
                    "data_type": "TimestampType"
                },
                "description": "Time tag for the state of all features."
            },
            {
                "column": {
                    "name":
                        "listing_page_viewed__rent_per_month__avg_over_7_days_fixed_windows",
                    "data_type": "FloatType"
                },
                "description": "Average of something."
            },
            {
                "column": {
                    "name":
                        "listing_page_viewed__rent_per_month__avg_over_2_weeks_fixed_windows",
                    "data_type": "FloatType"
                },
                "description": "Average of something."
            }
        ]
    }
    ]
    """

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
            windows = feature.transformation._windows or (
                self.feature_set.feature_set._windows
                if isinstance(self.feature_set.feature_set, AggregatedFeatureSet)
                else [None]
            )
            pivot_values = (
                self.feature_set.feature_set._pivot_values
                if isinstance(self.feature_set.feature_set, AggregatedFeatureSet)
                else [None]
            )
            desc_feature += [
                feature.description
                for _ in feature.transformation.functions
                for _ in range(len(pivot_values) * len(windows))
            ] or [feature.description]

        schema = self.feature_set.feature_set.get_schema()

        self._features = [(column, desc) for column, desc in zip(schema, desc_feature)]

        return self

    def to_json(self):
        """Generate json file."""
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
        """Generate markdown file."""
        params = self._construct()

        markdown = MdUtils(file_name=params._name)
        markdown.new_header(level=1, title=str(params._name).capitalize())
        markdown.new_header(level=2, title="Description")
        markdown.new_paragraph(params._desc_feature_set)
        markdown.new_line()
        markdown.new_header(level=2, title="Feature Set Pipeline")
        markdown.new_header(level=3, title="Source")

        source = ["Reader", "Location"]
        for r, l in params._source:
            source.extend([r, l])

        count_rows = len(source) // 2

        markdown.new_table(columns=2, rows=count_rows, text=source, text_align="center")
        markdown.new_header(level=3, title="Sink")

        sink = ["Writer"]
        for w in params._sink:
            sink.extend([w])

        count_rows = len(sink)

        markdown.new_table(columns=1, rows=count_rows, text=sink, text_align="center")
        markdown.new_header(level=3, title="Features")

        features = ["Column name", "Data type", "Description"]
        for c, desc in params._features:
            features.extend([c["column_name"], str(c["type"]), desc])

        count_rows = len(features) // 3

        markdown.new_table(
            columns=3, rows=count_rows, text=features, text_align="center"
        )

        if self.save:
            markdown.create_md_file()
        else:
            return markdown.file_data_text
