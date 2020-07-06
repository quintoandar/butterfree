## Butterfree Python Library

![butterfree's evolution](https://i.imgur.com/wJzsBaK.gif)

_This library is part of the QuintoAndar-specific libraries._

This library supports Python version 3.6+ and meant to provide tools for
building ETL pipelines for feature stores using [Apache Spark](https://spark.apache.org/).
Every pipeline uses the idea of feature sets. Each feature set is, in
summary, a compound of features for an entity object (like House, for
example) that can be created easily from a small set of sources.

More about this idea later :smile:

You can follow how Butterfree evolves [here](https://drive.google.com/open?id=11T6_VRbjouT_QbWBXDomYVR9R2ffnlQZ).
This will be a living class diagram of this tool!

## Creating your first feature set

```python
from butterfree.core.pipelines.feature_set_pipeline import FeatureSetPipeline
from butterfree.core.extract import Source
from butterfree.core.extract.readers import TableReader
from butterfree.core.transform import FeatureSet
from butterfree.core.transform.features import Feature, KeyFeature, TimestampFeature
from butterfree.core.transform.transformations import AggregatedTransform
from butterfree.core.load import Sink
from butterfree.core.load.writers import (
    HistoricalFeatureStoreWriter,
    OnlineFeatureStoreWriter,
)
​
​
class HouseListingPageViewedEvents(FeatureSetPipeline):
    def __init__(self):
        super(HouseListingPageViewedEvents, self).__init__(
            source=Source(
                readers=[
                    TableReader(
                        id="amplitude_events",
                        database="datalake_amplitude_clean",
                        table="events_repartitioned",
                    ).with_(
                        filter,
                        condition="event_type='listing_page_viewed' and id_app=170698 and ts_event > '2019-01-01'",
                    )
                ],
                query=(
                    """
                    select
                        ts_event,
                        get_json_object(event_properties, '$.house_id') as event_house_id,
                        id_app
                    from amplitude_events
                    """
                ),
            ),
            feature_set=FeatureSet(
                name="house_listing_page_viewed",
                entity="house",
                description=(
                    """
                    Holds all house listing page viewed events within amplitude's scope.
​
                    It has information since 2019 and only for PWA app (170698).
                    """
                ),
                keys=[
                    KeyFeature(
                        name="id",
                        description="The user's Main ID or device ID",
                        from_column="event_house_id",
                    )
                ],
                timestamp=TimestampFeature(from_column="ts_event"),
                features=[
                    Feature(
                        name="id",
                        description="Aggregated Feature.",
                        transformation=AggregatedTransform(
                            aggregations=["count"],
                            partition="id",
                            windows=["1 week", "2 weeks", "4 weeks"],
                            mode=["rolling_windows"],
                        ),
                    )
                ],
            ),
            sink=Sink(
                writers=[HistoricalFeatureStoreWriter(), OnlineFeatureStoreWriter()]
            ),
        )

pipeline = HouseListingPageViewedEvents()
pipeline.run()
```

In summary, this class will setup a batch pipeline for building the count
of listing page views, over 1, 2 and 4 weeks past, per house and day. Data
will be sent to the Historical Feature Store (which default is a Hadoop
table, mapping files in S3, partitioned by year, month and day of the
feature values). From there, users can query the feature set, to find
values for each feature in any point of time.

Also, latest data (latest version of each feature for each house) is
written to a Cassandra DB, our default Online Feature Store, for fast
lookup at prediction time. 

## Installing

[QuintoAndar's Python Package Index](https://quintoandar.github.io/python-package-server/)
hosts reference to a pip-installable module of this library, using it is
as straightforward as including it on your project's requirements.

```bash
pip install quintoandar-butterfree --extra-index-url https://quintoandar.github.io/python-package-server/
```

Or after listing `quintoandar-butterfree` in your
`requirements.txt` file:

```bash
pip install -r requirements.txt --extra-index-url https://quintoandar.github.io/python-package-server/
```

### DEV Build

You may also have access to our preview build (unstable) by
installing `quintoandar-butterfree-dev`. 

```bash
pip install quintoandar-butterfree-dev --extra-index-url https://quintoandar.github.io/python-package-server/
```


## Development Environment

At the bare minimum you'll need the following for your development
environment:

1. [Python 3.6.8](http://www.python.org/)


It is strongly recommended to also install and use [pyenv](https://github.com/pyenv/pyenv):

 - [pyenv-installer](https://github.com/pyenv/pyenv-installer)

This tool eases the burden of dealing with virtualenvs and having to activate and
deactivate'em by hand. Once you run `pyenv local my-project-venv` the directory you're
in will be bound to the `my-project-venv` virtual environment and then you will have
never to bother again activating the correct venv.

## Getting started

Run `make help` for more information on ready to use scripts.

#### 1. Clone the project:

```bash
    git clone git@github.com:quintoandar/butterfree.git
    cd butterfree
```

#### 2. Setup the python environment for the project:

```bash
make environment
```

If you need to configure your development environment in your IDE, notice
pyenv will store your python under
`~/.pyenv/versions/3.6.8 butterfree/bin/python`.

##### Errors

If you receive one error of missing OpenSSL to run the `pyenv install`, you can try to fix running:

```bash
sudo apt install -y libssl1.0-dev
```

#### 3. Install dependencies

```bash
make requirements
```

##### Errors

If you receive one error like this one:
```bash
 "import setuptools, tokenize;__file__='/tmp/pip-build-98gth33d/googleapis-common-protos/setup.py';
 .... 
 failed with error code 1 in /tmp/pip-build-98gth33d/googleapis-common-protos/
```
 
You can try to fix it running:

```bash
python -m pip install --upgrade pip setuptools wheel
```

## Development

Library's content live under the [`quintoandar`](https://github.com/quintoandar/butterfree/tree/master/quintoandar)
module, where you'll find [it's public API exposed](https://github.com/quintoandar/butterfree/tree/master/quintoandar/__init__.py).


### Tests

TL;DR: Just run `make tests` to check if your code is fine.

This project is thoroughly tested as of the time of this writing. Unit tests
rely under the [test module](https://github.com/quintoandar/butterfree/tree/master/tests/unit)
and integration tests, under the [integration_test module](https://github.com/quintoandar/butterfree/tree/master/tests/integration).

[pytest](https://docs.pytest.org/en/latest/)
is used to write all of this project's tests.

You can run unit tests by issuing make at the project's root:
```bash
make unit-tests
```

You can run
integration tests in the same fashion:
```bash
make integration-test
```

Style check is available through make too:
```bash
make style-check
make quality-check
```

You can run unit tests, integration tests and style check in a single batch:
```bash
make tests
```

To test all notebook examples under `examples/` folder, you can run:
```bash
make test-examples
```

### Code Style, PEP8 & Formatting

TL;DR: Just run `make apply-style` before you commit.
Check if everything is fine with `make checks`.

This project follows:
- [PEP8](https://www.python.org/dev/peps/pep-0008/) for code style.
- [PEP257](https://www.python.org/dev/peps/pep-0257/) with Google's docsting
style ([example](https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html#example-google)).
using some nice tooling to unifies style across the project's codebase and
improve quality.

No need to worry about manually reviewing your style and imports,
`black` and `isort` will automatically fix most of style inconsistency:

```bash
make apply-style
```

Additionally [Flake 8](http://flake8.pycqa.org/en/latest/) is used to
check for other things such as unnecessary imports and code-complexity.

You can check Flake 8 and Black by running the following within the project root:

```bash
make checks
```

### API Documentation

If you created a new class, changed a method or
just added new information in the documents and 
these changes have already been to the master branch,
you will need to update the API documentation
and it's super simple, in the **documentation** branch 
just bring the modifications from the master branch and run `make update-docs`.

```bash
make update-docs
```

No need to worry about modifying the `html files`, 
everything is generated from [Sphinx](https://www.sphinx-doc.org/en/master/index.html) and hosted by [ReadtheDocs](https://readthedocs.org/).
But your documentation changes will only be applied after a merge to documentation branch.


## Git Flow

Releasing is pretty easy with our CI/CD. But make sure to follow our git flow
[guidelines](GITFLOW.md).

## Contributing

Any contributions are welcome! Feel free to open Pull Requests and
posting them to [our
**#data-products-reviews** slack channel](https://quintoandar.slack.com/messages/data-products-reviews/).

Please **follow the guidelines** described [here](CONTRIBUTING.md).
