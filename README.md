## Butterfree
Made with :heart: by the **MLOps** team from [QuintoAndar](https://github.com/quintoandar/)

![butterfree's evolution](https://i.imgur.com/wJzsBaK.gif)

This library supports Python version 3.6+ and meant to provide tools for building ETL pipelines for Feature Stores using [Apache Spark](https://spark.apache.org/).

The library is centered on the following concetps:
- **ETL**: central framework to create data pipelines. Spark-based **Extract**, **Transform** and **Load** modules ready to use.
- **Declarative Feature Engineering**: care about **what** you want to compute and **not how** to code it.
- **Feature Store Modeling**: the library easily provides everything you need to process and load data to your Feature Store.

To understand the main concepts of Feature Store modeling and library main features you can check [Butterfree's Wiki](https://github.com/quintoandar/butterfree/wiki).

## Example

```python
from pyspark.sql import functions as F

from butterfree.core.constants.data_type import DataType
from butterfree.core.pipelines.feature_set_pipeline import FeatureSetPipeline
from butterfree.core.extract import Source
from butterfree.core.extract.readers import TableReader, FileReader
from butterfree.core.transform.aggregated_feature_set import AggregatedFeatureSet 
from butterfree.core.transform.features import Feature, KeyFeature, \
    TimestampFeature
from butterfree.core.transform.transformations import AggregatedTransform
from butterfree.core.transform.utils.function import Function
from butterfree.core.load import Sink
from butterfree.core.load.writers import (
    HistoricalFeatureStoreWriter,
    OnlineFeatureStoreWriter,
)


class PageViewsFeatureSetPipeline(FeatureSetPipeline):
    def __init__(self):
        super().__init__(
            source=Source(
                readers=[
                    TableReader(
                        id="page_viewed_events", database="datalake",
                        table="events",
                    ).with_(
                        filter,
                        condition="event_type='page_viewed' and year >= 2020",
                    ),
                    FileReader(
                        id="region", path="s3://bucket/path/region.json",
                        format="json",
                    ),
                ],
                query=(
                    """
                    select
                        page_viewed_events.id_event,
                        page_viewed_events.ts_event,
                        page_viewed_events.id_page,
                        region.name as region_name
                    from
                        page_viewed_events
                        join region
                            on page_viewed_events.id_region = region.id
                    """
                ),
            ),
            feature_set=AggregatedFeatureSet(
                name="page_views",
                entity="page_ranking",
                description=(
                    """
                    Feature Set holding information about page view counts.

                    Aggregates data from page_viewed event type for the year of 2020.
                    The time windows are defined for intervals of 1 day, 2 days, 14 days
                    and 30 days.
                    """
                ),
                keys=[
                    KeyFeature(
                        name="id",
                        description="The unique identificator for the web page.",
                        from_column="id_page",
                        dtype=DataType.BIGINT,
                    )
                ],
                timestamp=TimestampFeature(from_column="ts_event"),
                features=[
                    Feature(
                        name="id_event",
                        description="Count of events defined over the column id_event.",
                        transformation=AggregatedTransform(
                            functions=[Function(F.count, DataType.INTEGER)],
                        ),
                    ),
                ],
            ).with_windows(
                definitions=["1 day", "7 days", "14 days", "30 days"]),
            sink=Sink(
                writers=[HistoricalFeatureStoreWriter(),
                         OnlineFeatureStoreWriter()]
            ),
        )

```


Running the defining pipeline is easy as:
```
pipeline = PageViewsFeatureSetPipeline()
pipeline.run()
```

In summary, this class will define a batch pipeline for building a feature set from counts of page view events. With Butterfree declarative feature engineering, a set of aggregate-over-time features, like the declared 1 day, 7 days, 14 days and 30 days windows, are easily defined. Features are aggregated by a key column with a time reference.

Data will be sent to the Historical Feature Store, whose default storage is the Datalake Spark Metastore, mapping files in S3, partitioned by year, month and day. From there, users can query the feature set, to find values for each feature in any point of time - perfect for building datasets.

Also, the latest data (latest version of each feature for each id) is written to the Online Feature Store, by default on a Cassandra DB, for low-latency feature retrieval at prediction time.

For more in depth examples check the [Butterfree's notebook examples](https://github.com/quintoandar/butterfree/tree/master/examples)  

## Requirements and Installation
Butterfree depends on **Python 3.6+** and it is **Spark 3.0 ready** :heavy-check-mark:

[Python Package Index](https://quintoandar.github.io/python-package-server/) hosts reference to a pip-installable module of this library, using it is as straightforward as including it on your project's requirements.

```bash
pip install quintoandar-butterfree --extra-index-url https://quintoandar.github.io/python-package-server/
```

Or after listing `quintoandar-butterfree` in your `requirements.txt` file:

```bash
pip install -r requirements.txt --extra-index-url https://quintoandar.github.io/python-package-server/
```

You may also have access to our preview build (unstable) by installing from `staging` branch:

```bash
pip install git+https://github.com/quintoandar/butterfree.git@staging
```

## Documentation
The official documentation is hosted on [Read the Docs](https://quintoandar-butterfree.readthedocs-hosted.com/en/stable/)

## License
TBD

## Contributing
All contributions are welcome! Feel free to open Pull Requests. Check the development and contributing **guidelines** described [here](CONTRIBUTING.md).
